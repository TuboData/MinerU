# MinerU 多GPU 并行解决方案

这个解决方案基于 [LitServe](https://github.com/Lightning-AI/litserve) 框架，允许 MinerU PDF解析服务在多个GPU上并行运行，提高处理大量PDF文件时的效率。

## 环境设置

在使用多GPU解决方案前，请确保安装以下依赖：

```bash
# 安装LitServe和其他依赖
pip install -U litserve python-multipart filetype

# 安装MinerU和依赖
pip install magic-pdf --extra-index-url https://wheels.myhloli.com

# 如果需要OCR功能，请安装PaddlePaddle
pip install paddlepaddle-gpu==3.0.0b1 -i https://www.paddlepaddle.org.cn/packages/stable/cu118
```

## 快速开始

### 1. 启动服务器

使用以下命令启动多GPU服务器：

```bash
python app_multi_gpu.py
```

默认情况下，服务器将在端口 8888 上运行，并使用所有可用的 GPU。

### 配置选项

如果需要自定义服务器配置，可以修改 `app_multi_gpu.py` 文件末尾的 `run_server` 函数调用：

```python
if __name__ == "__main__":
    run_server(
        output_dir='custom_output',  # 自定义输出目录
        port=9000,                  # 自定义端口
        workers_per_device=2        # 每个GPU的工作进程数
    )
```

### 2. 使用客户端

客户端支持并行处理多个PDF文件，并可通过命令行参数控制各种选项：

```bash
python client_multi_gpu.py --input /path/to/pdfs --jobs 8 --save-files
```

#### 命令行参数

```
usage: client_multi_gpu.py [-h] --input INPUT [--url URL] [--jobs JOBS]
                           [--output-dir OUTPUT_DIR]
                           [--parse-method {auto,ocr,txt}] [--save-files]
                           [--layout] [--info] [--content] [--images]

PDF并行处理客户端

optional arguments:
  -h, --help            显示帮助信息并退出
  --input INPUT, -i INPUT
                        输入PDF文件路径或包含PDF文件的目录
  --url URL, -u URL     服务器URL (默认: http://127.0.0.1:8888/predict)
  --jobs JOBS, -j JOBS  并行任务数 (默认: 4)
  --output-dir OUTPUT_DIR, -o OUTPUT_DIR
                        指定输出目录 (默认: output)
  --parse-method {auto,ocr,txt}, -m {auto,ocr,txt}
                        解析方法: auto, ocr, txt (默认: auto)
  --save-files, -s      保存解析结果到文件 (默认: False)
  --layout, -l          返回布局信息 (默认: False)
  --info                返回文档信息 (默认: False)
  --content, -c         返回内容列表 (默认: False)
  --images              返回图像 (默认: False)
```

### 实际示例

1. 处理单个PDF文件:

```bash
python client_multi_gpu.py -i /path/to/document.pdf -s -l -c
```

2. 批量处理目录下的所有PDF文件，使用8个并行线程：

```bash
python client_multi_gpu.py -i /path/to/pdf_folder -j 8 -s -o results
```

3. 处理S3上的PDF文件:

```bash
python client_multi_gpu.py -i s3://bucket-name/path/to/document.pdf -s
```

### 3. 通过HTTP API调用

除了使用提供的客户端，你还可以直接通过HTTP API调用多GPU服务。

#### API端点

多GPU版本的API端点为：
```
http://服务器地址:端口/predict
```

例如：`http://127.0.0.1:8888/predict`

#### 请求格式

请求格式是JSON，需要包含以下字段：

```json
{
    "pdf_file": {
        "filename": "文件名.pdf",
        "content": "BASE64编码的文件内容"
    },
    // 或者使用pdf_path（S3路径）
    "pdf_path": "s3://bucket-name/path/to/file.pdf",
    
    // 以下是可选参数
    "parse_method": "auto",  // 可选值: "auto", "ocr", "txt"
    "is_json_md_dump": true, // 是否保存结果文件
    "output_dir": "output",  // 输出目录
    "return_layout": true,   // 是否返回布局信息
    "return_info": true,     // 是否返回文档信息
    "return_content_list": true, // 是否返回内容列表
    "return_images": true    // 是否返回图像
}
```

#### Python示例代码

以下是使用Python的`requests`库调用多GPU API的示例代码：

```python
import requests
import base64
import json

def call_mineru_api(pdf_path, server_url="http://127.0.0.1:8888/predict"):
    """通过HTTP API调用MinerU多GPU服务"""
    
    # 读取PDF文件并转为base64
    with open(pdf_path, 'rb') as f:
        pdf_content = base64.b64encode(f.read()).decode('utf-8')
    
    # 构建请求数据
    payload = {
        "pdf_file": {
            "filename": pdf_path.split("/")[-1],
            "content": pdf_content
        },
        "parse_method": "auto",
        "is_json_md_dump": True,
        "return_layout": True,
        "return_info": True,
        "return_content_list": True,
        "return_images": False  # 图像数据可能很大，按需设置
    }
    
    # 发送请求
    response = requests.post(server_url, json=payload)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"错误: {response.status_code}")
        print(response.text)
        return None

# 使用示例
result = call_mineru_api("/path/to/document.pdf")
print(json.dumps(result, indent=2, ensure_ascii=False))
```

#### cURL示例

也可以使用cURL命令行工具调用API：

```bash
curl -X POST "http://127.0.0.1:8888/predict" \
  -H "Content-Type: application/json" \
  -d '{
    "pdf_path": "s3://your-bucket/path/to/file.pdf",
    "parse_method": "auto",
    "is_json_md_dump": true,
    "return_layout": true,
    "return_info": true,
    "return_content_list": true
  }'
```

#### 获取所有JSON结果

要获取MinerU生成的所有JSON文件，确保在API请求中设置以下参数：

```json
{
  "is_json_md_dump": true,
  "return_layout": true,
  "return_info": true, 
  "return_content_list": true
}
```

这样会将结果保存到服务器上的输出目录，并在API响应中包含所有类型的JSON数据。

#### 响应格式

成功的响应将包含以下字段（取决于请求中的参数）：

```json
{
  "md_content": "Markdown格式的文档内容",
  "layout": { "布局信息JSON对象" },
  "info": { "文档信息JSON对象" },
  "content_list": [ "内容列表JSON数组" ],
  "images": { "图像名称": "base64编码的图像数据" }
}
```

### 4. Docker部署

为了更方便地部署和运行多GPU版本的MinerU，我们提供了Docker支持。

#### 前提条件

- 安装Docker和Docker Compose
- 安装NVIDIA Container Toolkit (nvidia-docker2)

```bash
# 安装NVIDIA Container Toolkit
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
curl -s -L https://nvidia.github.io/nvidia-docker/gpgkey | sudo apt-key add -
curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.list | sudo tee /etc/apt/sources.list.d/nvidia-docker.list
sudo apt-get update && sudo apt-get install -y nvidia-docker2
sudo systemctl restart docker
```

#### 使用Docker Compose启动服务

1. 创建必要的目录:
```bash
mkdir -p output data
```

2. 使用Docker Compose启动服务:
```bash
docker-compose -f docker-compose.multi_gpu.yml up -d
```

3. 查看服务日志:
```bash
docker-compose -f docker-compose.multi_gpu.yml logs -f
```

#### 配置Docker环境变量

可以通过修改`docker-compose.multi_gpu.yml`文件中的环境变量来自定义服务配置：

```yaml
environment:
  - OUTPUT_DIR=/app/output
  - PORT=8888
  - WORKERS_PER_DEVICE=1  # 每个GPU的工作进程数
```

#### 直接使用Dockerfile构建和运行

如果不使用Docker Compose，也可以直接使用Dockerfile：

```bash
# 构建镜像
docker build -t mineru-multi-gpu -f Dockerfile.multi_gpu .

# 运行容器
docker run --gpus all -p 8888:8888 -v $(pwd)/output:/app/output mineru-multi-gpu
```

#### 检查GPU使用情况

可以通过以下命令检查容器内的GPU使用情况：

```bash
docker exec -it mineru-multi-gpu nvidia-smi
```

## 技术细节

### 服务器架构

服务器基于 LitServe 框架，它具有以下特点：

1. **多GPU自动负载均衡** - 自动将请求分配到可用GPU
2. **动态资源分配** - 根据资源利用率平衡工作负载
3. **内存管理** - 每个请求处理后自动清理GPU内存

### 客户端特性

1. **并行请求处理** - 使用joblib进行多线程请求
2. **自动重试机制** - 在遇到暂时性故障时自动重试
3. **结果汇总和报告** - 提供处理结果的统计信息

## 输出说明

每个PDF处理后，系统可以生成以下文件（如果启用了`--save-files`选项）：

- `{pdf_name}_content_list.json` - 文档内容的结构化表示
- `{pdf_name}.md` - Markdown格式的文档内容
- `{pdf_name}_middle.json` - 处理中间结果
- `{pdf_name}_model.json` - 模型解析结果
- `{pdf_name}_layout.pdf` - 布局可视化
- `{pdf_name}_spans.pdf` - 文字区域可视化
- `{pdf_name}_line_sort.pdf` - 行排序可视化
- `{pdf_name}_model.pdf` - 模型解析可视化
- `images/*.jpg` - 提取的图像文件 