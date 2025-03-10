import os
import json
import uuid
import time
import logging
import asyncio
import functools
from typing import Dict, List, Optional, Tuple, Union, Any
from datetime import datetime
import shutil
import io
import uvicorn
from fastapi import FastAPI, BackgroundTasks, UploadFile, File, Form, HTTPException, Depends, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from loguru import logger
from prometheus_client import Counter, Histogram, Gauge, generate_latest
import traceback
from fastapi.exception_handlers import http_exception_handler

from starlette.requests import Request

import magic_pdf.model as model_config
from magic_pdf.config.enums import SupportedPdfParseMethod
from magic_pdf.data.data_reader_writer import DataWriter, FileBasedDataWriter
from magic_pdf.data.data_reader_writer.s3 import S3DataReader, S3DataWriter
from magic_pdf.data.dataset import PymuDocDataset
from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze
from magic_pdf.operators.models import InferenceResult
from magic_pdf.operators.pipes import PipeResult

# 导入工具类和配置加载器
from utils.mysql_utils import MySQLUtils
from utils.minio_utils import MinioUtils
from utils.config_loader import ConfigLoader
from utils.redis_utils import RedisUtils

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pdf_api.log")
    ]
)
logger = logging.getLogger("pdf_api")

# 加载配置
config_loader = ConfigLoader()
app_config = config_loader.get_app_config()

# 根据配置设置日志级别
log_level = app_config.get("log_level", "INFO")
logging.getLogger().setLevel(log_level)
logger.info(f"Log level set to {log_level}")

# 初始化数据库和存储工具
mysql_utils = MySQLUtils(config_loader.get_mysql_config())
minio_utils = MinioUtils(config_loader.get_minio_config())
redis_utils = RedisUtils(config_loader.get_redis_config())

# 应用配置
max_workers = app_config.get("max_workers", 4)
clean_interval_hours = app_config.get("clean_interval_hours", 24)
job_expiry_hours = app_config.get("job_expiry_hours", 48)
model_config.__use_inside_model__ = True

app = FastAPI()

# 添加Prometheus指标
REQUEST_COUNT = Counter('pdf_api_requests_total', 'Total number of requests', ['method', 'endpoint', 'status'])
REQUEST_LATENCY = Histogram('pdf_api_request_latency_seconds', 'Request latency in seconds', ['method', 'endpoint'])
JOBS_COUNT = Gauge('pdf_api_jobs_count', 'Number of jobs by status', ['status'])
PROCESSING_JOBS = Gauge('pdf_api_processing_jobs', 'Number of jobs currently processing')
PROCESSING_TIME = Histogram('pdf_api_processing_time_seconds', 'Time taken to process PDFs', ['parse_method'])

# 定义任务状态常量
class JobStatus:
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

# 创建初始数据库表
try:
    mysql_utils.create_tables()
except Exception as e:
    logger.error(f"Failed to create database tables: {e}")

# 定义获取任务信息的工具函数
def get_job_info(job_id: str) -> Optional[Dict[str, Any]]:
    """
    根据任务ID获取任务信息
    
    Args:
        job_id: 任务ID
        
    Returns:
        任务信息字典，如果任务不存在则返回None
    """
    # 直接从MySQL获取任务信息
    try:
        job_data = mysql_utils.get_job(job_id)
        return job_data
    except Exception as e:
        logger.error(f"Error getting job info: {e}")
        return None

# 数据写入器
class MemoryDataWriter(DataWriter):
    def __init__(self):
        self.buffer = StringIO()

    def write(self, path: str, data: bytes) -> None:
        if isinstance(data, str):
            self.buffer.write(data)
        else:
            self.buffer.write(data.decode("utf-8"))

    def write_string(self, path: str, data: str) -> None:
        self.buffer.write(data)

    def get_value(self) -> str:
        return self.buffer.getvalue()

    def close(self):
        self.buffer.close()


def init_writers(
    pdf_file: UploadFile = None,
    job_id: str = None,
) -> Tuple[
    S3DataWriter,
    S3DataWriter,
    bytes,
]:
    """初始化数据写入器，默认使用MinIO存储
    
    Args:
        pdf_file: 上传的PDF文件对象
        job_id: 任务ID，用于构建存储路径
        
    Returns:
        元组，包含输出写入器、图像写入器和PDF字节数据
    """
    # 使用任务ID作为存储路径基础
    storage_base_path = f"jobs/{job_id}" if job_id else "temp"
    image_storage_path = f"{storage_base_path}/images"
    
    # 默认使用MinIO存储
    bucket_name = os.environ.get("MINIO_BUCKET", "pdf-processor")
    
    # 获取MinIO配置
    minio_config = config_loader.get_minio_config()
    ak = minio_config.get("access_key", "minioadmin")
    sk = minio_config.get("secret_key", "minioadmin")
    endpoint_url = minio_config.get("endpoint", "localhost:9000")
    
    # 确保endpoint_url包含协议前缀
    if endpoint_url and not endpoint_url.startswith(('http://', 'https://')):
        secure = minio_config.get("secure", False)
        protocol = "https://" if secure else "http://"
        endpoint_url = f"{protocol}{endpoint_url}"
    
    # 初始化S3数据写入器
    output_writer = S3DataWriter(
        storage_base_path, 
        bucket=bucket_name,
        ak=ak,
        sk=sk,
        endpoint_url=endpoint_url
    )
    
    image_writer = S3DataWriter(
        image_storage_path, 
        bucket=bucket_name,
        ak=ak,
        sk=sk,
        endpoint_url=endpoint_url
    )
    
    # 处理PDF文件
    if pdf_file:
        pdf_bytes = pdf_file.file.read()
    else:
        raise HTTPException(
            status_code=400, detail="PDF file must be provided"
        )
    
    return output_writer, image_writer, pdf_bytes


def process_pdf(
    pdf_bytes: bytes,
    parse_method: str,
    image_writer: Union[S3DataWriter, FileBasedDataWriter],
) -> Tuple[InferenceResult, PipeResult]:
    """
    Process PDF file content

    Args:
        pdf_bytes: Binary content of PDF file
        parse_method: Parse method ('ocr', 'txt', 'auto')
        image_writer: Image writer

    Returns:
        Tuple[InferenceResult, PipeResult]: Returns inference result and pipeline result
    """
    ds = PymuDocDataset(pdf_bytes)
    infer_result: InferenceResult = None
    pipe_result: PipeResult = None

    if parse_method == "ocr":
        infer_result = ds.apply(doc_analyze, ocr=True)
        pipe_result = infer_result.pipe_ocr_mode(image_writer)
    elif parse_method == "txt":
        infer_result = ds.apply(doc_analyze, ocr=False)
        pipe_result = infer_result.pipe_txt_mode(image_writer)
    else:  # auto
        if ds.classify() == SupportedPdfParseMethod.OCR:
            infer_result = ds.apply(doc_analyze, ocr=True)
            pipe_result = infer_result.pipe_ocr_mode(image_writer)
        else:
            infer_result = ds.apply(doc_analyze, ocr=False)
            pipe_result = infer_result.pipe_txt_mode(image_writer)

    return infer_result, pipe_result


def encode_image(image_path: str) -> str:
    """Encode image using base64"""
    with open(image_path, "rb") as f:
        return b64encode(f.read()).decode()


def process_pdf_background(
    job_id: str,
    pdf_bytes: bytes,
    pdf_name: str,
    parse_method: str = "auto",
    is_json_md_dump: bool = True,
    return_layout: bool = True,
    return_info: bool = True,
    return_content_list: bool = True,
    return_images: bool = True,
):
    """
    后台任务处理PDF文件并将结果保存到MinIO
    
    Args:
        job_id: 唯一任务标识符
        pdf_bytes: PDF文件内容
        pdf_name: PDF文件名称
        parse_method: 解析方法（默认为"auto"）
        is_json_md_dump: 是否保存JSON和Markdown到MinIO（默认为True，通常不需修改）
        return_layout: 是否在结果中包含文档布局（默认为True，通常不需修改）
        return_info: 是否在结果中包含文档元数据（默认为True，通常不需修改）
        return_content_list: 是否在结果中包含内容列表（默认为True，通常不需修改）
        return_images: 是否在结果中包含提取的图像（默认为True，通常不需修改）
    """
    # 短暂延迟以确保API响应已经返回给客户端
    # 这样可以避免任何潜在的阻塞
    time.sleep(0.2)
    
    try:
        logger.info(f"Background processing started for job {job_id}")
        
        # 先检查任务是否存在
        job_info = mysql_utils.get_job(job_id)
        if not job_info:
            logger.error(f"Cannot process job {job_id}: Job not found in database")
            return
        
        # 更新任务状态为处理中，初始进度为5%
        update_job_progress(job_id, 5.0, JobStatus.PROCESSING)
        
        # 初始化结果字典 - 添加更多关键字段
        result_dict = {
            "job_id": job_id,
            "pdf_name": pdf_name,
            "parse_method": parse_method,
            "processing_time": {
                "start": datetime.now().isoformat(),
                "end": None
            },
            "parameters": {
                "is_json_md_dump": is_json_md_dump,
                "return_layout": return_layout,
                "return_info": return_info,
                "return_content_list": return_content_list,
                "return_images": return_images
            },
            "result": {}
        }
        
        # 初始化MinIO数据写入器
        bucket_name = os.environ.get("MINIO_BUCKET", "pdf-processor")
        storage_base_path = f"jobs/{job_id}"
        image_storage_path = f"{storage_base_path}/images"
        
        # 获取MinIO配置
        minio_config = config_loader.get_minio_config()
        ak = minio_config.get("access_key", "minioadmin")
        sk = minio_config.get("secret_key", "minioadmin")
        endpoint_url = minio_config.get("endpoint", "localhost:9000")
        
        # 确保endpoint_url包含协议前缀
        if endpoint_url and not endpoint_url.startswith(('http://', 'https://')):
            secure = minio_config.get("secure", False)
            protocol = "https://" if secure else "http://"
            endpoint_url = f"{protocol}{endpoint_url}"
        
        # 创建写入器
        output_writer = S3DataWriter(
            storage_base_path, 
            bucket=bucket_name,
            ak=ak,
            sk=sk,
            endpoint_url=endpoint_url
        )
        
        image_writer = S3DataWriter(
            image_storage_path, 
            bucket=bucket_name,
            ak=ak,
            sk=sk,
            endpoint_url=endpoint_url
        )
        
        # 保存原始PDF到MinIO
        output_writer.write(f"{job_id}.pdf", pdf_bytes)
        
        # 更新进度到15% - 准备分析阶段
        update_job_progress(job_id, 15.0)
        
        # 处理PDF
        logger.info(f"Processing PDF for job {job_id} with method {parse_method}")
        infer_result, pipe_result = process_pdf(pdf_bytes, parse_method, image_writer)
        
        # 更新进度到50% - PDF处理完成
        update_job_progress(job_id, 50.0)
        
        # 获取模型推理结果（这是一个关键步骤）
        try:
            model_inference_result = infer_result.get_infer_res()
            logger.info(f"Job {job_id}: 成功获取模型推理结果")
            result_dict["result"]["model_inference"] = model_inference_result
            
            # 保存模型推理结果为独立的JSON文件
            model_json_str = json.dumps(model_inference_result, ensure_ascii=False, indent=2, default=str)
            output_writer.write_string(f"{job_id}_model.json", model_json_str)
            logger.info(f"Job {job_id}: 模型推理结果已保存为{job_id}_model.json")
        except Exception as e:
            logger.warning(f"Job {job_id}: 获取模型推理结果失败: {e}")
        
        # 创建临时目录用于保存可视化文件
        temp_dir = f"/tmp/pdf_job_{job_id}"
        os.makedirs(temp_dir, exist_ok=True)
        
        # 绘制模型结果并保存
        try:
            model_pdf_path = os.path.join(temp_dir, f"{job_id}_model.pdf")
            infer_result.draw_model(model_pdf_path)
            
            # 读取生成的文件并上传到MinIO
            if os.path.exists(model_pdf_path):
                with open(model_pdf_path, 'rb') as f:
                    model_viz_bytes = f.read()
                    output_writer.write(f"{job_id}_model.pdf", model_viz_bytes)
                    logger.info(f"Job {job_id}: 成功保存模型可视化结果")
        except Exception as e:
            logger.warning(f"Job {job_id}: 绘制模型结果失败: {e}")
            
        # 绘制布局结果并保存
        try:
            layout_pdf_path = os.path.join(temp_dir, f"{job_id}_layout.pdf")
            pipe_result.draw_layout(layout_pdf_path)
            
            # 读取生成的文件并上传到MinIO
            if os.path.exists(layout_pdf_path):
                with open(layout_pdf_path, 'rb') as f:
                    layout_viz_bytes = f.read()
                    output_writer.write(f"{job_id}_layout.pdf", layout_viz_bytes)
                    logger.info(f"Job {job_id}: 成功保存布局可视化结果")
        except Exception as e:
            logger.warning(f"Job {job_id}: 绘制布局结果失败: {e}")
            
        # 绘制span结果并保存
        try:
            span_pdf_path = os.path.join(temp_dir, f"{job_id}_spans.pdf")
            pipe_result.draw_span(span_pdf_path)
            
            # 读取生成的文件并上传到MinIO
            if os.path.exists(span_pdf_path):
                with open(span_pdf_path, 'rb') as f:
                    span_viz_bytes = f.read()
                    output_writer.write(f"{job_id}_spans.pdf", span_viz_bytes)
                    logger.info(f"Job {job_id}: 成功保存span可视化结果")
        except Exception as e:
            logger.warning(f"Job {job_id}: 绘制span结果失败: {e}")
        
        # 清理临时文件
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                logger.info(f"Job {job_id}: 已清理临时文件目录 {temp_dir}")
        except Exception as e:
            logger.warning(f"Job {job_id}: 清理临时文件失败: {e}")
            
        # 获取中间JSON
        try:
            middle_json_content = pipe_result.get_middle_json()
            if middle_json_content:
                # 使用json.dumps确保正确序列化
                middle_json_str = json.dumps(middle_json_content, ensure_ascii=False, indent=2, default=str)
                output_writer.write_string(f"{job_id}_middle.json", middle_json_str)
                result_dict["result"]["middle_json"] = middle_json_content
                logger.info(f"Job {job_id}: 成功保存中间JSON结果")
        except Exception as e:
            logger.warning(f"Job {job_id}: 获取中间JSON失败: {e}")
            
        # 获取内容列表
        image_dir = "images"
        if return_content_list:
            try:
                content_list = pipe_result.get_content_list(image_dir)
                output_writer.write_string(f"{job_id}_content_list.json", json.dumps(content_list, ensure_ascii=False, indent=2, default=str))
                result_dict["result"]["content_list"] = content_list
                logger.info(f"Job {job_id}: 成功保存内容列表")
            except Exception as e:
                logger.warning(f"Job {job_id}: 获取内容列表失败: {e}")
                
        # 获取markdown
        try:
            md_content = pipe_result.get_markdown(image_dir)
            output_writer.write_string(f"{job_id}.md", md_content)
            result_dict["result"]["markdown"] = md_content
            logger.info(f"Job {job_id}: 成功保存Markdown")
        except Exception as e:
            logger.warning(f"Job {job_id}: 获取Markdown失败: {e}")
        
        # 更新进度到70% - 结果提取完成
        update_job_progress(job_id, 70.0)
        
        # 处理图像
        if return_images and hasattr(infer_result, 'images') and infer_result.images:
            # 图像数据直接保存到MinIO并记录
            images_data = []
            for i, img_data in enumerate(infer_result.images):
                # 保存图像到MinIO
                img_name = f"{i}.jpg"
                image_writer.write(img_name, img_data)
                
                # 添加到响应数据中 - 使用字节数据创建内存文件进行base64编码
                buffer = io.BytesIO(img_data)
                base64_data = b64encode(buffer.getvalue()).decode()
                images_data.append({"id": i, "data": base64_data})
            
            # 添加图像信息到结果字典
            result_dict["result"]["images"] = images_data
        
        # 更新进度到90% - 图像保存完成
        update_job_progress(job_id, 90.0)
        
        # 记录处理结束时间
        result_dict["processing_time"]["end"] = datetime.now().isoformat()
        result_dict["processing_duration_seconds"] = (
            datetime.fromisoformat(result_dict["processing_time"]["end"]) - 
            datetime.fromisoformat(result_dict["processing_time"]["start"])
        ).total_seconds()
        
        # 将主要结果字段保存到MySQL数据库
        mysql_utils.save_pdf_job_result(job_id, result_dict["result"])
        
        # 更新进度到100% - 任务完成
        update_job_progress(job_id, 100.0, JobStatus.COMPLETED)
        
        logger.info(f"PDF processing for job {job_id} completed successfully")
    except Exception as e:
        logger.error(f"Error processing PDF for job {job_id}: {e}")
        stacktrace = traceback.format_exc()
        logger.error(stacktrace)
        
        # 使用mysql_utils的封装方法保存错误状态
        error_message = str(e)
        mysql_utils.save_pdf_job_error(job_id, error_message)
        
        logger.info(f"Job {job_id} marked as failed due to error: {error_message}")

# 辅助函数：更新任务进度，带有重试机制
def update_job_progress(job_id: str, progress: float, status: Optional[str] = None):
    """
    更新任务进度，带有重试机制
    
    Args:
        job_id: 任务ID
        progress: 进度百分比
        status: 可选的状态更新
    """
    # 最大尝试次数
    max_retries = 3
    retry_delay = 0.5  # 基础延迟时间（秒）
    
    for attempt in range(max_retries):
        try:
            # 更新Redis中的进度
            redis_utils.update_job_progress(job_id, progress)
            
            # 如果需要更新状态，则更新MySQL中的状态
            if status:
                update_data = {"status": status}
                mysql_utils.update_job(job_id, update_data)
                logger.info(f"Updated job {job_id} status to {status}")
                
            return  # 成功更新，直接返回
                
        except Exception as e:
            logger.error(f"Error updating job progress (attempt {attempt+1}/{max_retries}): {e}")
            
            # 如果不是最后一次尝试，则等待后重试
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
    
    # 如果所有尝试都失败
    logger.error(f"Failed to update job {job_id} progress after {max_retries} attempts")



def get_pdf_from_minio(bucket_name: str, object_name: str) -> Optional[bytes]:
    """
    从MinIO中获取PDF文件内容
    
    Args:
        bucket_name: MinIO存储桶名称
        object_name: MinIO对象名称
        
    Returns:
        PDF文件的二进制内容，如果发生错误则返回None
    """
    try:
        # 获取系统初始化的MinIO配置（来自config.yaml）
        base_config = config_loader.get_minio_config()

        if bucket_name == base_config.get("bucket_name"):
            # 使用默认桶时复用全局minio_utils实例
            return minio_utils.get_file_content(object_name)

        # 否则创建临时连接，使用相同配置但替换bucket_name
        temp_config = {
            "endpoint": base_config["endpoint"],
            "access_key": base_config["access_key"],
            "secret_key": base_config["secret_key"],
            "secure": base_config.get("secure", False),
            "bucket_name": bucket_name  # 动态替换桶名
        }

        temp_minio = MinioUtils(temp_config)
        return temp_minio.get_file_content(object_name)

    except Exception as e:
        logger.error(f"Error getting PDF from MinIO: {e}")
        return None


# 请求中间件 - 记录请求信息
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start_time = time.time()
    
    # 对于异常情况的默认响应
    status_code = 500
    
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    except Exception as e:
        logger.error(f"Request error: {str(e)}")
        # 让FastAPI处理异常
        raise
    finally:
        # 记录请求指标
        endpoint = request.url.path
        REQUEST_COUNT.labels(request.method, endpoint, status_code).inc()
        REQUEST_LATENCY.labels(request.method, endpoint).observe(time.time() - start_time)


# 自定义异常处理
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    # 记录错误
    logger.error(f"Global exception: {str(exc)}")
    logger.error(traceback.format_exc())
    
    # 对于HTTP异常，使用标准处理
    if isinstance(exc, HTTPException):
        return await http_exception_handler(request, exc)
        
    # 对于其他异常，返回500错误
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "type": str(type(exc).__name__)},
    )

# 清理过期任务
def cleanup_old_jobs(max_age_hours: int = 24):
    """
    清理旧的任务数据
    
    Args:
        max_age_hours: 最大保留时间（小时）
        
    Returns:
        清理的任务数量
    """
    try:
        current_time = time.time()
        cutoff_time = current_time - (max_age_hours * 3600)
        
        # 从MySQL获取已完成或失败且超过保留时间的任务
        cutoff_date = datetime.fromtimestamp(cutoff_time).strftime('%Y-%m-%d %H:%M:%S')
        query = """
        SELECT id, status, created_at, pdf_name FROM pdf_jobs 
        WHERE status IN (%s, %s) AND created_at < %s
        """
        params = (JobStatus.COMPLETED, JobStatus.FAILED, cutoff_date)
        expired_jobs = mysql_utils.execute_query(query, params)
        
        logger.info(f"Found {len(expired_jobs)} expired jobs to clean up (older than {max_age_hours} hours)")
        
        count = 0
        for job in expired_jobs:
            job_id = job.get("id")
            pdf_name = job.get("pdf_name", "unknown")
            
            try:
                # 先从MinIO删除文件
                try:
                    minio_utils.delete_directory(f"jobs/{job_id}")
                    logger.info(f"Cleaned up files for expired job {job_id} ({pdf_name}) from MinIO")
                except Exception as e:
                    logger.warning(f"Failed to delete MinIO files for job {job_id} ({pdf_name}): {e}")
                    # 继续处理，即使MinIO删除失败也尝试删除MySQL记录
                
                # 然后从MySQL删除任务记录
                if mysql_utils.delete_job(job_id):
                    logger.info(f"Cleaned up expired job {job_id} ({pdf_name}) from database")
                    count += 1
                else:
                    logger.warning(f"Failed to delete job {job_id} ({pdf_name}) from database")
            except Exception as e:
                logger.error(f"Error cleaning up job {job_id} ({pdf_name}): {e}")
                # 继续处理下一个任务
            
        logger.info(f"Successfully cleaned up {count} expired jobs")
        return count
        
    except Exception as e:
        logger.error(f"Error cleaning up old jobs: {e}")
        logger.exception("Cleanup error details:")
        return 0


def get_job_result_from_minio(job_id: str) -> Optional[Dict[str, Any]]:
    """
    从MinIO获取PDF任务的处理结果
    
    Args:
        job_id: 任务ID
        
    Returns:
        处理结果数据字典，如果未找到则返回None
    """
    try:
        # 获取MinIO配置
        bucket_name = os.environ.get("MINIO_BUCKET", "pdf-processor")
        storage_base_path = f"jobs/{job_id}"
        
        minio_config = config_loader.get_minio_config()
        ak = minio_config.get("access_key", "minioadmin")
        sk = minio_config.get("secret_key", "minioadmin")
        endpoint_url = minio_config.get("endpoint", "localhost:9000")
        
        # 确保endpoint_url包含协议前缀
        if endpoint_url and not endpoint_url.startswith(('http://', 'https://')):
            secure = minio_config.get("secure", False)
            protocol = "https://" if secure else "http://"
            endpoint_url = f"{protocol}{endpoint_url}"
        
        # 创建S3DataReader
        reader = S3DataReader(
            storage_base_path, 
            bucket=bucket_name,
            ak=ak,
            sk=sk,
            endpoint_url=endpoint_url
        )
        
        # 从字节数据解码JSON字符串的辅助函数
        def decode_json_bytes(json_bytes):
            if not json_bytes:
                return None
                
            try:
                json_str = json_bytes.decode('utf-8')
                return json.loads(json_str)
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                logger.error(f"Error decoding JSON: {str(e)}")
                return None
        
        # 先尝试读取完整结果文件
        try:
            complete_result_bytes = reader.read(f"{job_id}_complete_result.json")
            if complete_result_bytes:
                logger.info(f"Found complete result for job {job_id}")
                return decode_json_bytes(complete_result_bytes)
        except Exception as e:
            logger.warning(f"Could not read complete result for job {job_id}: {str(e)}")
        
        # 如果没有完整结果，则尝试读取内容列表
        content_list_data = None
        middle_json_data = None
        
        try:
            content_list_bytes = reader.read(f"{job_id}_content_list.json")
            if content_list_bytes:
                content_list_data = decode_json_bytes(content_list_bytes)
                if content_list_data:
                    logger.info(f"Successfully loaded content list for job {job_id}")
        except Exception as e:
            logger.warning(f"Could not read content list for job {job_id}: {str(e)}")
            
        try:
            middle_json_bytes = reader.read(f"{job_id}_middle.json")
            if middle_json_bytes:
                middle_json_data = decode_json_bytes(middle_json_bytes)
                if middle_json_data:
                    logger.info(f"Successfully loaded middle JSON for job {job_id}")
        except Exception as e:
            logger.warning(f"Could not read middle JSON for job {job_id}: {str(e)}")
            
        # 获取基本任务信息
        job_info = mysql_utils.get_job(job_id)
        if not job_info:
            logger.warning(f"Job {job_id} not found in database")
            return None
        
        # 构建结果字典
        result = {
            "job_id": job_id,
            "pdf_name": job_info.get("pdf_name", ""),
            "status": job_info.get("status", ""),
            "created_at": job_info.get("created_at", ""),
            "completed_at": job_info.get("completed_at", ""),
            "result": {}
        }
        
        # 添加内容列表
        if content_list_data:
            result["result"]["content_list"] = content_list_data
            
        # 添加中间JSON
        if middle_json_data:
            result["result"]["middle_json"] = middle_json_data
            
        if not content_list_data and not middle_json_data:
            logger.error(f"No result content could be loaded for job {job_id}")
            
        return result
            
    except Exception as e:
        logger.exception(f"Error getting job result from MinIO: {str(e)}")
        return None

@app.post(
    "/pdf_parse_from_minio",
    tags=["projects"],
    summary="Parse PDF files stored in MinIO",
)
async def pdf_parse_from_minio(
        background_tasks: BackgroundTasks,
        bucket_name: str = Form(...),
        object_name: str = Form(...)
):
    """
    处理MinIO中的PDF文件并返回任务ID，用于后续检查状态和获取结果。

    Args:
        background_tasks: 后台任务对象
        bucket_name: MinIO桶名称
        object_name: MinIO中的PDF对象名称

    Returns:
        任务ID和状态信息
    """
    try:
        # 获取文件内容
        pdf_bytes = get_pdf_from_minio(bucket_name, object_name)
        if not pdf_bytes:
            raise HTTPException(status_code=404, detail="PDF file not found in MinIO")

        # 生成任务ID
        job_id = str(uuid.uuid4())

        # 获取文件名
        pdf_name = os.path.basename(object_name)

        # 保存任务信息到MySQL
        saved = mysql_utils.save_job(
            job_id,
            pdf_name,
            JobStatus.PENDING
        )

        if not saved:
            raise HTTPException(status_code=500, detail="Failed to create job in database")

        # 小延迟确保数据库事务完成
        await asyncio.sleep(0.1)

        # 在后台启动任务处理 - 确保这不会阻塞API响应
        background_tasks.add_task(
            process_pdf_background,
            job_id,
            pdf_bytes,
            pdf_name,
            "auto"
        )

        logger.info(f"非阻塞任务已创建: {job_id} - 立即返回响应")

        # 立即返回任务ID
        return {
            "job_id": job_id,
            "message": "任务已创建并在后台处理中",
            "status": JobStatus.PENDING
        }

    except Exception as e:
        logger.exception(f"Error in pdf_parse_from_minio: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post(
    "/pdf_parse",
    tags=["projects"],
    summary="Parse PDF files uploaded directly",
)
async def pdf_parse(
        background_tasks: BackgroundTasks,
        pdf_file: UploadFile = File(...),
):
    """
    处理上传的PDF文件并返回任务ID，用于后续检查状态和获取结果。

    Args:
        background_tasks: 后台任务对象
        pdf_file: 要解析的PDF文件

    Returns:
        任务ID和状态信息
    """
    try:
        # 生成唯一的任务ID
        job_id = str(uuid.uuid4())

        # 获取PDF文件
        output_writer, image_writer, pdf_bytes = init_writers(
            pdf_file=pdf_file,
            job_id=job_id,
        )

        # 获取PDF文件名
        pdf_name = pdf_file.filename
        logger.info(f"Starting job {job_id} for PDF {pdf_name}")

        # 保存任务信息到MySQL
        mysql_utils.save_job(
            job_id,
            pdf_name,
            JobStatus.PENDING
        )

        # 在后台启动任务处理
        background_tasks.add_task(
            process_pdf_background,
            job_id,
            pdf_bytes,
            pdf_name,
            "auto"
        )

        # 返回任务ID
        return {
            "job_id": job_id,
            "message": "任务已创建并正在处理",
            "status": "pending"
        }

    except Exception as e:
        logger.exception(f"Error in pdf_parse: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/get_job_status/{job_id}",
    tags=["projects"],
    summary="Check the status of a PDF processing job",
)
async def get_job_status(job_id: str):
    """
    获取PDF处理任务的状态

    Args:
        job_id: 任务ID

    Returns:
        任务状态信息，包括进度百分比
    """
    try:
        # 使用线程池执行数据库查询，避免阻塞
        loop = asyncio.get_event_loop()
        job_data = await loop.run_in_executor(
            None,  # 使用默认的线程池
            functools.partial(mysql_utils.get_job, job_id)
        )

        if not job_data:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        # 从Redis获取任务进度
        redis_job_data = await loop.run_in_executor(
            None,
            functools.partial(redis_utils.get_job_status, job_id)
        )

        # 获取Redis中的进度信息，如果不存在则默认为0
        progress = 0.0
        if redis_job_data and "progress" in redis_job_data:
            progress = redis_job_data.get("progress", 0.0)

        # 构建响应
        response = {
            "job_id": job_id,
            "status": job_data.get("status", "unknown"),
            "progress": progress,
            "pdf_name": job_data.get("pdf_name", "unknown"),
            "created_at": job_data.get("created_at", datetime.now().timestamp())
        }

        # 如果任务失败，包含错误信息
        if job_data.get("status") == JobStatus.FAILED and job_data.get("error_message"):
            response["error"] = job_data.get("error_message")

        # 如果任务完成，包含完成时间
        if job_data.get("status") in [JobStatus.COMPLETED, JobStatus.FAILED] and job_data.get("completed_at"):
            response["completed_at"] = job_data.get("completed_at")

        return response

    except asyncio.TimeoutError as e:
        error_msg = f"Timeout retrieving job status for {job_id}: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=504, detail="Request timed out")
    except HTTPException:
        # 直接重新抛出HTTP异常
        raise
    except Exception as e:
        # 捕获并记录详细的错误信息
        error_msg = f"Error retrieving job status for {job_id}: {str(e)}"
        stack_trace = traceback.format_exc()
        logger.error(f"{error_msg}\n{stack_trace}")
        raise HTTPException(status_code=500, detail=error_msg)


@app.delete(
    "/pdf_job/{job_id}",
    tags=["projects"],
    summary="Delete a PDF processing job and its results",
)
async def delete_job(job_id: str, delete_files: bool = True):
    """
    删除PDF处理任务及其结果

    Args:
        job_id: 任务ID
        delete_files: 是否同时删除存储的文件（默认为True，建议保持默认值以确保数据一致性）

    Returns:
        删除结果
    """
    try:
        # 获取任务信息
        job_data = mysql_utils.get_job(job_id)

        if not job_data:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        # 删除Redis中的任务进度信息
        try:
            redis_utils.delete_job(job_id)
            logger.info(f"Deleted Redis data for job {job_id}")
        except Exception as redis_err:
            logger.warning(f"Failed to delete Redis data for job {job_id}: {redis_err}")
            # 不阻止继续处理

        # 无论delete_files参数值如何，都删除MinIO中的文件以保持数据一致性
        minio_delete_success = False
        try:
            # 构建存储路径
            storage_path = f"jobs/{job_id}"
            logger.info(f"Attempting to delete MinIO files at path: {storage_path}")

            # 执行删除操作
            minio_delete_success = minio_utils.delete_directory(storage_path)

            if minio_delete_success:
                logger.info(f"Successfully deleted MinIO files for job {job_id}")
            else:
                logger.warning(f"MinIO deletion reported failure for job {job_id}")
        except Exception as e:
            logger.error(f"Exception while deleting MinIO files for job {job_id}: {e}")
            # 记录但不阻止继续处理，避免文件删除失败导致数据库记录无法删除

        # 删除任务数据
        result = mysql_utils.delete_job(job_id)

        if not result:
            logger.warning(f"Failed to delete job {job_id} from database")
            raise HTTPException(
                status_code=500,
                detail="Failed to delete job from database"
            )

        # 返回状态，包含MinIO删除结果
        response = {
            "success": True,
            "message": f"Job {job_id} deleted from database",
            "minio_files_deleted": minio_delete_success
        }

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error deleting job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 批量查询任务状态
@app.post("/pdf_jobs/status", tags=["projects"], summary="Get status of multiple PDF processing jobs")
async def get_multiple_job_status(job_ids: List[str]):
    """
    批量查询多个任务的状态

    Args:
        job_ids: 任务ID列表

    Returns:
        Dict: 任务状态字典
    """
    result = {}

    for job_id in job_ids:
        job_info = get_job_info(job_id)
        if job_info:
            result[job_id] = {
                "status": job_info.get("status", "unknown"),
                "pdf_name": job_info.get("pdf_name", "unknown"),
                "created_at": job_info.get("created_at", time.time())
            }

            if job_info.get("error"):
                result[job_id]["error"] = job_info["error"]
        else:
            result[job_id] = {"status": "not_found"}

    return result


@app.get(
    "/pdf_files/{job_id}",
    tags=["projects"],
    summary="获取任务的所有相关文件列表",
)
async def get_job_files(job_id: str):
    """
    获取指定任务的所有相关文件列表

    Args:
        job_id: 任务ID

    Returns:
        文件列表，按类型分组
    """
    try:
        # 检查任务是否存在
        job_data = mysql_utils.get_job(job_id)
        if not job_data:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        # 从MinIO获取文件列表
        files = minio_utils.get_job_files(job_id)

        return {
            "job_id": job_id,
            "files": files
        }

    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Error getting job files for {job_id}: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


@app.get(
    "/pdf_file/{job_id}/{file_type}",
    tags=["projects"],
    summary="获取任务的指定类型文件",
    response_class=Response,
)
async def get_job_file(job_id: str, file_type: str):
    """
    获取指定任务的特定类型文件

    Args:
        job_id: 任务ID
        file_type: 文件类型，可以是以下值之一：
                  - pdf: 原始PDF
                  - layout: 布局可视化PDF
                  - spans: 文本块可视化PDF
                  - model: 模型结果可视化PDF
                  - model_json: 模型推理结果JSON
                  - middle_json: 中间处理结果JSON
                  - content_list: 内容列表JSON
                  - markdown: Markdown文本

    Returns:
        请求的文件内容
    """
    try:
        # 检查任务是否存在
        job_data = mysql_utils.get_job(job_id)
        if not job_data:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        # 确定文件路径
        file_path = None
        content_type = None

        if file_type == "pdf":
            file_path = f"jobs/{job_id}/{job_id}.pdf"
            content_type = "application/pdf"
        elif file_type == "layout":
            file_path = f"jobs/{job_id}/{job_id}_layout.pdf"
            content_type = "application/pdf"
        elif file_type == "spans":
            file_path = f"jobs/{job_id}/{job_id}_spans.pdf"
            content_type = "application/pdf"
        elif file_type == "model":
            file_path = f"jobs/{job_id}/{job_id}_model.pdf"
            content_type = "application/pdf"
        elif file_type == "model_json":
            file_path = f"jobs/{job_id}/{job_id}_model.json"
            content_type = "application/json"
        elif file_type == "middle_json":
            file_path = f"jobs/{job_id}/{job_id}_middle.json"
            content_type = "application/json"
        elif file_type == "content_list":
            file_path = f"jobs/{job_id}/{job_id}_content_list.json"
            content_type = "application/json"
        elif file_type == "markdown":
            file_path = f"jobs/{job_id}/{job_id}.md"
            content_type = "text/markdown"
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {file_type}")

        # 获取文件内容
        file_content = minio_utils.get_file_content(file_path)
        if not file_content:
            raise HTTPException(status_code=404, detail=f"File not found: {file_path}")

        # 返回文件内容
        return Response(content=file_content, media_type=content_type)

    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Error getting file {file_type} for job {job_id}: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


@app.get(
    "/pdf_image/{job_id}/{image_path:path}",
    tags=["projects"],
    summary="获取任务的特定图片文件",
    response_class=Response,
)
async def get_job_image(job_id: str, image_path: str):
    """
    获取指定任务的特定图片文件

    Args:
        job_id: 任务ID
        image_path: 图片路径，从/pdf_files/{job_id}接口获取的image路径

    Returns:
        图片文件内容
    """
    try:
        # 检查任务是否存在
        job_data = mysql_utils.get_job(job_id)
        if not job_data:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        # 获取文件内容
        image_content = minio_utils.get_file_content(image_path)
        if not image_content:
            raise HTTPException(status_code=404, detail=f"Image not found: {image_path}")

        # 确定内容类型
        content_type = "image/jpeg"  # 默认为JPEG
        if image_path.lower().endswith(".png"):
            content_type = "image/png"
        elif image_path.lower().endswith(".gif"):
            content_type = "image/gif"

        # 返回图片内容
        return Response(content=image_content, media_type=content_type)

    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Error getting image {image_path} for job {job_id}: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)

# 重试功能
@app.post("/pdf_job/{job_id}/retry", tags=["projects"], summary="Retry a failed PDF processing job")
async def retry_job(
        job_id: str,
        background_tasks: BackgroundTasks,
        parse_method: str = None
):
    """
    重试失败的PDF处理任务

    Args:
        job_id: 任务ID
        background_tasks: 后台任务对象
        parse_method: 可选的新解析方法，如果提供则使用新方法

    Returns:
        Dict: 任务ID和状态
    """
    try:
        # 检查任务是否存在
        job_info = get_job_info(job_id)
        if not job_info:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        # 获取任务参数
        job_params = mysql_utils.get_job(job_id)
        if not job_params:
            raise HTTPException(status_code=404, detail=f"Job {job_id} parameters not found")

        # 获取原始PDF文件
        bucket_name = os.environ.get("MINIO_BUCKET", "pdf-processor")
        pdf_bytes = minio_utils.get_file_content(f"jobs/{job_id}/{job_id}.pdf")
        if not pdf_bytes:
            raise HTTPException(
                status_code=404,
                detail=f"Original PDF for job {job_id} not found in MinIO"
            )

        # 确定使用的解析方法
        if parse_method is None:
            # 使用默认解析方法或使用job_params中的parse_method（如果存在）
            parse_method = job_params.get("parse_method", "auto")

        # 更新MySQL状态
        mysql_utils.save_job(
            job_id,
            job_params.get("pdf_name", f"{job_id}.pdf"),
            JobStatus.PENDING
        )

        # 启动后台任务重新处理PDF
        # 所有布尔参数使用默认值True，不再从pdf_job_params表获取
        background_tasks.add_task(
            process_pdf_background,
            job_id=job_id,
            pdf_bytes=pdf_bytes,
            pdf_name=job_params.get("pdf_name", f"{job_id}.pdf"),
            parse_method=parse_method
            # 不需要指定其他参数，将使用函数默认值(都为True)
        )

        return {
            "job_id": job_id,
            "status": JobStatus.PENDING,
            "message": "PDF processing restarted"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error retrying job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))



# 添加定期清理功能的端点
@app.post("/admin/cleanup", tags=["admin"], summary="Clean up expired job data")
async def cleanup_jobs(max_age_hours: int = Query(24, gt=0, le=720)):
    """
    手动触发清理过期任务数据

    Args:
        max_age_hours: 最大保留时间（小时），默认24小时
    """
    removed_count = cleanup_old_jobs(max_age_hours)
    return {"status": "success", "removed_count": removed_count}


# 健康检查端点
@app.get("/health", tags=["system"], summary="API health check")
async def health_check():
    """健康检查端点"""
    health_data = {
        "status": "ok",
        "version": "1.0",
        "timestamp": time.time(),
        "components": {
            "mysql": mysql_utils.db_pool is not None,
            "minio": minio_utils.minio_client is not None,
            "redis": redis_utils.redis_client is not None  # 添加Redis组件检查
        },
        "details": {}
    }

    # 检查组件连接
    try:
        if mysql_utils.db_pool:
            conn = mysql_utils.db_pool.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            # 确保读取结果
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            # 如果成功连接并执行查询，标记为健康
            health_data["components"]["mysql"] = True
    except Exception as e:
        logger.error(f"MySQL健康检查失败: {e}")
        health_data["components"]["mysql"] = False
        health_data["status"] = "degraded"
        health_data["details"]["mysql_error"] = str(e)

    try:
        if minio_utils.minio_client:
            minio_utils.minio_client.list_buckets()
            # 如果成功列出桶，标记为健康
            health_data["components"]["minio"] = True
    except Exception as e:
        logger.error(f"MinIO健康检查失败: {e}")
        health_data["components"]["minio"] = False
        health_data["status"] = "degraded"
        health_data["details"]["minio_error"] = str(e)

    # 添加Redis健康检查
    try:
        if redis_utils.redis_client:
            redis_utils.redis_client.ping()
            # 如果成功ping通，标记为健康
            health_data["components"]["redis"] = True
    except Exception as e:
        logger.error(f"Redis健康检查失败: {e}")
        health_data["components"]["redis"] = False
        health_data["status"] = "degraded"
        health_data["details"]["redis_error"] = str(e)

    if any(not status for status in health_data["components"].values()):
        health_data["status"] = "critical"

    return health_data


# 指标端点
@app.get("/metrics", tags=["system"], summary="Prometheus metrics")
async def metrics():
    """获取Prometheus格式的监控指标"""
    try:
        # 从MySQL获取任务状态统计
        status_counts = {}
        processing_count = 0

        # 直接查询每种状态的数量
        query = "SELECT status, COUNT(*) as count FROM pdf_jobs GROUP BY status"
        results = mysql_utils.execute_query(query)

        # 初始化所有状态计数为0
        for status in [JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.COMPLETED, JobStatus.FAILED]:
            status_counts[status] = 0

        # 更新从数据库获取的计数
        for row in results:
            status = row.get("status")
            count = row.get("count", 0)
            status_counts[status] = count

            # 计算处理中的任务数
            if status == JobStatus.PROCESSING:
                processing_count = count

        # 更新指标
        for status, count in status_counts.items():
            JOBS_COUNT.labels(status).set(count)

        PROCESSING_JOBS.set(processing_count)

    except Exception as e:
        logger.error(f"Error updating metrics from database: {e}")
        # 出错时不更新指标，保持上一次的值

    return PlainTextResponse(generate_latest())


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8888)
