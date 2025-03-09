import os
import uuid
import json
import shutil
import tempfile
import gc
import fitz
import torch
import base64
import filetype
from glob import glob
from io import StringIO
from pathlib import Path
from typing import Tuple, Union, Dict, Any, List

import litserve as ls
from fastapi import HTTPException, UploadFile
from fastapi.responses import JSONResponse
from loguru import logger

import magic_pdf.model as model_config
from magic_pdf.config.enums import SupportedPdfParseMethod
from magic_pdf.data.data_reader_writer import DataWriter, FileBasedDataWriter
from magic_pdf.data.data_reader_writer.s3 import S3DataReader, S3DataWriter
from magic_pdf.data.dataset import PymuDocDataset
from magic_pdf.libs.config_reader import get_bucket_name, get_s3_config
from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze, ModelSingleton
from magic_pdf.operators.models import InferenceResult
from magic_pdf.operators.pipes import PipeResult

model_config.__use_inside_model__ = True


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


class MinerUAPI(ls.LitAPI):
    def __init__(self, output_dir='output'):
        self.output_dir = Path(output_dir)
        os.makedirs(output_dir, exist_ok=True)

    def setup(self, device):
        """初始化模型和运行环境，每个GPU实例会执行一次"""
        if device.startswith('cuda'):
            os.environ['CUDA_VISIBLE_DEVICES'] = device.split(':')[-1]
            if torch.cuda.device_count() > 1:
                raise RuntimeError("Remove any CUDA actions before setting 'CUDA_VISIBLE_DEVICES'.")
        
        logger.info(f"初始化模型于设备 {device}")
        # 预加载模型实例
        model_manager = ModelSingleton()
        model_manager.get_model(True, False)  # OCR模式
        model_manager.get_model(False, False)  # TXT模式
        logger.info(f'模型初始化完成于设备 {device}!')

    def decode_request(self, request):
        """处理客户端请求"""
        # 获取请求参数
        pdf_file = request.get('pdf_file')
        pdf_path = request.get('pdf_path')
        parse_method = request.get('parse_method', 'auto')
        is_json_md_dump = request.get('is_json_md_dump', False)
        return_layout = request.get('return_layout', False)
        return_info = request.get('return_info', False) 
        return_content_list = request.get('return_content_list', False)
        return_images = request.get('return_images', False)
        
        # 文件处理
        pdf_bytes = None
        pdf_name = None
        
        if pdf_file and 'content' in pdf_file:
            # 处理base64编码的文件内容
            pdf_bytes = base64.b64decode(pdf_file['content'])
            pdf_name = pdf_file.get('filename', str(uuid.uuid4()))
        elif pdf_path:
            # 处理文件路径
            pdf_name = os.path.basename(pdf_path).split('.')[0]
            is_s3_path = pdf_path.startswith("s3://")
            
            if is_s3_path:
                bucket = get_bucket_name(pdf_path)
                ak, sk, endpoint = get_s3_config(bucket)
                temp_reader = S3DataReader("", bucket=bucket, ak=ak, sk=sk, endpoint_url=endpoint)
                pdf_bytes = temp_reader.read(pdf_path)
            else:
                with open(pdf_path, "rb") as f:
                    pdf_bytes = f.read()
        
        if not pdf_bytes:
            raise HTTPException(status_code=400, detail="必须提供pdf_file或pdf_path")
        
        # 返回处理参数
        return {
            'pdf_bytes': pdf_bytes,
            'pdf_name': pdf_name,
            'parse_method': parse_method,
            'is_json_md_dump': is_json_md_dump,
            'return_layout': return_layout,
            'return_info': return_info,
            'return_content_list': return_content_list,
            'return_images': return_images
        }

    def predict(self, inputs):
        """主要处理逻辑，在分配的GPU上运行"""
        try:
            # 解包参数
            pdf_bytes = inputs['pdf_bytes']
            pdf_name = inputs['pdf_name']
            parse_method = inputs['parse_method']
            is_json_md_dump = inputs['is_json_md_dump']
            return_layout = inputs['return_layout']
            return_info = inputs['return_info']
            return_content_list = inputs['return_content_list']
            return_images = inputs['return_images']
            
            # 设置输出目录
            output_path = str(self.output_dir / pdf_name)
            output_image_path = f"{output_path}/images"
            os.makedirs(output_image_path, exist_ok=True)
            
            # 决定写入器类型
            writer = FileBasedDataWriter(output_path)
            image_writer = FileBasedDataWriter(output_image_path)
            
            # 处理PDF
            ds = PymuDocDataset(pdf_bytes)
            infer_result = None
            pipe_result = None
            
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
            
            # 使用内存写入器获取结果
            content_list_writer = MemoryDataWriter()
            md_content_writer = MemoryDataWriter()
            middle_json_writer = MemoryDataWriter()
            
            # 提取数据
            pipe_result.dump_content_list(content_list_writer, "", "images")
            pipe_result.dump_md(md_content_writer, "", "images")
            pipe_result.dump_middle_json(middle_json_writer, "")
            
            # 转换为 JSON 对象
            content_list = json.loads(content_list_writer.get_value())
            md_content = md_content_writer.get_value()
            middle_json = json.loads(middle_json_writer.get_value())
            model_json = infer_result.get_infer_res()
            
            # 写入文件（如果需要）
            if is_json_md_dump:
                writer.write_string(f"{pdf_name}_content_list.json", content_list_writer.get_value())
                writer.write_string(f"{pdf_name}.md", md_content)
                writer.write_string(f"{pdf_name}_middle.json", middle_json_writer.get_value())
                writer.write_string(f"{pdf_name}_model.json", json.dumps(model_json, indent=4, ensure_ascii=False))
                
                # 保存可视化结果
                pipe_result.draw_layout(os.path.join(output_path, f"{pdf_name}_layout.pdf"))
                pipe_result.draw_span(os.path.join(output_path, f"{pdf_name}_spans.pdf"))
                pipe_result.draw_line_sort(os.path.join(output_path, f"{pdf_name}_line_sort.pdf"))
                infer_result.draw_model(os.path.join(output_path, f"{pdf_name}_model.pdf"))
            
            # 处理图像（如果需要）
            images = {}
            if return_images:
                image_paths = glob(f"{output_image_path}/*.jpg")
                for image_path in image_paths:
                    with open(image_path, "rb") as f:
                        image_data = base64.b64encode(f.read()).decode()
                        images[os.path.basename(image_path)] = f"data:image/jpeg;base64,{image_data}"
            
            # 关闭内存写入器
            content_list_writer.close()
            md_content_writer.close()
            middle_json_writer.close()
            
            # 构建返回数据
            result = {'md_content': md_content}  # md_content 始终返回
            
            if return_layout:
                result['layout'] = model_json
            if return_info:
                result['info'] = middle_json
            if return_content_list:
                result['content_list'] = content_list
            if return_images:
                result['images'] = images
                
            return result
        except Exception as e:
            logger.exception(e)
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            self.clean_memory()
    
    def encode_response(self, response):
        """对模型输出进行编码处理，返回给客户端"""
        return response
    
    def clean_memory(self):
        """清理 GPU 内存"""
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            torch.cuda.ipc_collect()
        gc.collect()


def run_server(output_dir='output', port=8888, workers_per_device=1):
    """启动多GPU服务器"""
    server = ls.LitServer(
        MinerUAPI(output_dir=output_dir),
        accelerator='cuda',     # 使用CUDA加速
        devices='auto',         # 自动使用所有可用GPU
        workers_per_device=workers_per_device,  # 每个GPU的工作进程数
        timeout=False           # 禁用超时
    )
    server.run(port=port)


if __name__ == "__main__":
    run_server(output_dir='output', port=8888) 