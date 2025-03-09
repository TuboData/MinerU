import os
import base64
import requests
import argparse
import numpy as np
from pathlib import Path
from loguru import logger
from joblib import Parallel, delayed


def to_base64(file_path):
    """将文件转换为base64编码"""
    try:
        with open(file_path, 'rb') as f:
            return base64.b64encode(f.read()).decode('utf-8')
    except Exception as e:
        raise Exception(f'File: {file_path} - Info: {e}')


def process_pdf(file_path, url='http://127.0.0.1:8888/predict', **kwargs):
    """处理单个PDF文件"""
    try:
        # 如果文件路径以s3://开头，则直接传递路径
        if file_path.startswith('s3://'):
            payload = {
                'pdf_path': file_path,
                **kwargs
            }
        else:
            # 否则传递文件内容
            payload = {
                'pdf_file': {
                    'filename': os.path.basename(file_path),
                    'content': to_base64(file_path)
                },
                **kwargs
            }
        
        # 发送请求到服务器
        response = requests.post(url, json=payload)
        
        if response.status_code == 200:
            result = response.json()
            result['file_path'] = file_path
            logger.info(f"成功处理文件: {file_path}")
            return result
        else:
            error_msg = f"处理失败 ({response.status_code}): {response.text}"
            logger.error(error_msg)
            return {'error': error_msg, 'file_path': file_path}
    except Exception as e:
        error_msg = f"处理异常: {str(e)}"
        logger.error(f'File: {file_path} - Info: {error_msg}')
        return {'error': error_msg, 'file_path': file_path}


def process_pdf_files(files, url='http://127.0.0.1:8888/predict', n_jobs=4, **kwargs):
    """并行处理多个PDF文件"""
    logger.info(f"开始处理 {len(files)} 个文件，使用 {n_jobs} 个并行任务")
    
    results = Parallel(n_jobs=n_jobs, prefer='threads', verbose=10)(
        delayed(process_pdf)(f, url, **kwargs) for f in files
    )
    
    # 统计处理结果
    success_count = sum(1 for r in results if 'error' not in r)
    logger.info(f"处理完成: {success_count}/{len(files)} 成功")
    
    return results


def find_pdf_files(input_dir):
    """查找指定目录下的所有PDF文件"""
    pdf_files = []
    for path in Path(input_dir).rglob('*.pdf'):
        pdf_files.append(str(path))
    return pdf_files


if __name__ == '__main__':
    # 命令行参数解析
    parser = argparse.ArgumentParser(description='PDF并行处理客户端')
    parser.add_argument('--input', '-i', type=str, required=True,
                        help='输入PDF文件路径或包含PDF文件的目录')
    parser.add_argument('--url', '-u', type=str, default='http://127.0.0.1:8888/predict',
                        help='服务器URL (默认: http://127.0.0.1:8888/predict)')
    parser.add_argument('--jobs', '-j', type=int, default=4,
                        help='并行任务数 (默认: 4)')
    parser.add_argument('--output-dir', '-o', type=str, default='output',
                        help='指定输出目录 (默认: output)')
    parser.add_argument('--parse-method', '-m', type=str, default='auto',
                        choices=['auto', 'ocr', 'txt'],
                        help='解析方法: auto, ocr, txt (默认: auto)')
    parser.add_argument('--save-files', '-s', action='store_true',
                        help='保存解析结果到文件 (默认: False)')
    parser.add_argument('--layout', '-l', action='store_true',
                        help='返回布局信息 (默认: False)')
    parser.add_argument('--info', action='store_true',
                        help='返回文档信息 (默认: False)')
    parser.add_argument('--content', '-c', action='store_true',
                        help='返回内容列表 (默认: False)')
    parser.add_argument('--images', action='store_true',
                        help='返回图像 (默认: False)')
    
    args = parser.parse_args()
    
    # 确定输入文件列表
    input_path = args.input
    if os.path.isdir(input_path):
        files = find_pdf_files(input_path)
        logger.info(f"在目录 {input_path} 中找到 {len(files)} 个PDF文件")
    elif os.path.isfile(input_path) and input_path.endswith('.pdf'):
        files = [input_path]
        logger.info(f"将处理单个PDF文件: {input_path}")
    else:
        logger.error(f"无效的输入路径: {input_path}")
        exit(1)
    
    if not files:
        logger.error("没有找到PDF文件，退出")
        exit(1)
    
    # 处理文件
    kwargs = {
        'parse_method': args.parse_method,
        'is_json_md_dump': args.save_files,
        'output_dir': args.output_dir,
        'return_layout': args.layout,
        'return_info': args.info,
        'return_content_list': args.content,
        'return_images': args.images
    }
    
    # 根据文件数量自动调整并行任务数
    n_jobs = min(len(files), args.jobs)
    
    # 并行处理文件
    results = process_pdf_files(files, url=args.url, n_jobs=n_jobs, **kwargs)
    
    # 输出简要结果统计
    print("\n处理结果摘要:")
    for i, result in enumerate(results):
        status = "成功" if 'error' not in result else f"失败: {result['error']}"
        print(f"{i+1}. {result['file_path']} - {status}")
    
    # 计算成功率
    success_count = sum(1 for r in results if 'error' not in r)
    success_rate = success_count / len(files) * 100
    print(f"\n总结: 成功率 {success_rate:.2f}% ({success_count}/{len(files)})") 