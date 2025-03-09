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
cd projects/web_api/multi_gpu_api
python server.py
```

默认情况下，服务器将在端口 8888 上运行，并使用所有可用的 GPU。

### 配置选项

服务器的配置选项位于`server.py`文件末尾：

```python
if __name__ == '__main__':
    server = ls.LitServer(
        MinerUAPI(output_dir='/tmp'),
        accelerator='cuda',
        devices='auto',
        workers_per_device=1,
        timeout=False
    )
    server.run(port=8000)
```

您可以根据需要修改以下参数：
- `output_dir`: 输出目录
- `devices`: GPU设备选择，'auto'表示使用所有可用GPU
- `workers_per_device`: 每个GPU的工作进程数
- `port`: 服务器端口

### 2. 使用客户端

客户端支持并行处理多个PDF文件：

```bash
cd projects/web_api/multi_gpu_api
python client.py
```

也可以修改`client.py`中的参数来自定义处理行为：

```python
if __name__ == '__main__':
    files = ['demo/small_ocr.pdf']  # 修改为您的PDF文件列表
    n_jobs = np.clip(len(files), 1, 8)  # 并行任务数
    results = Parallel(n_jobs, prefer='threads', verbose=10)(
        delayed(do_parse)(p) for p in files
    )
```

### 3. 通过HTTP API调用

除了使用提供的客户端，你还可以直接通过HTTP API调用多GPU服务。

#### API端点

多GPU版本的API端点为：
```
http://服务器地址:端口/predict
```

例如：`http://127.0.0.1:8000/predict`

#### 请求格式

请求格式是JSON，需要包含以下字段：

```json
{
    "file": "BASE64编码的文件内容",
    "kwargs": {
        "parse_method": "auto",  // 可选值: "auto", "ocr", "txt"
        "debug_able": false,     // 是否启用调试模式
        // 其他可选参数
    }
}
```

#### Python示例代码

以下是使用Python调用多GPU API的示例代码：

```python
import base64
import requests

def to_b64(file_path):
    """将文件转换为base64编码"""
    with open(file_path, 'rb') as f:
        return base64.b64encode(f.read()).decode('utf-8')

def do_parse(file_path, url='http://127.0.0.1:8000/predict', **kwargs):
    """通过HTTP API调用MinerU多GPU服务"""
    try:
        response = requests.post(url, json={
            'file': to_b64(file_path),
            'kwargs': kwargs
        })

        if response.status_code == 200:
            output = response.json()
            output['file_path'] = file_path
            return output
        else:
            raise Exception(response.text)
    except Exception as e:
        print(f'File: {file_path} - Info: {e}')

# 使用示例
result = do_parse("path/to/document.pdf", parse_method="auto")
print(result)
```

#### cURL示例

也可以使用cURL命令行工具调用API：

```bash
curl -X POST "http://127.0.0.1:8000/predict" \
  -H "Content-Type: application/json" \
  -d '{
    "file": "BASE64编码的文件内容",
    "kwargs": {
        "parse_method": "auto",
        "debug_able": false
    }
  }'
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
docker-compose up -d
```

3. 查看服务日志:
```bash
docker-compose logs -f
```

#### 配置Docker环境变量

可以通过修改`docker-compose.yml`文件中的环境变量来自定义服务配置：

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
docker build -t mineru-multi-gpu -f Dockerfile.multi_gpu ../../

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

根据`server.py`的实现，每个PDF处理后，系统将返回处理结果的输出目录，您可以在该目录中找到所有生成的文件。 