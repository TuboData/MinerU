# PDF处理API服务

这是一个高性能的PDF文档处理API系统，支持异步处理PDF文件，使用MinIO作为主要存储，并与MySQL和Redis集成用于状态和元数据管理。

## 功能特点

- 异步处理PDF文件，支持长时间运行的任务
- 基于任务ID的文件存储，统一存储在MinIO中
- 将处理结果分类存储，支持按需获取不同类型的结果
- 与MySQL、Redis和MinIO集成，提供稳定的数据存储和状态管理
- 完善的监控和健康检查功能，支持Prometheus指标收集
- 任务失败自动重试功能
- 过期任务自动清理功能
- 支持配置文件和环境变量配置

## 系统要求

- Python 3.8+
- MySQL 8.0+
- Redis 6.0+
- MinIO (最新版本)

## 配置

系统支持通过配置文件和环境变量进行配置。配置文件为YAML格式，默认位置为`config.yaml`。

配置文件示例：

```yaml
# MySQL配置
mysql:
  host: localhost
  port: 3306
  user: root
  password: password
  database: pdf_processor

# Redis配置
redis:
  host: localhost
  port: 6379
  db: 0
  password: null

# MinIO配置
minio:
  endpoint: localhost:9000
  access_key: minioadmin
  secret_key: minioadmin
  secure: false
  bucket_name: pdf-processor

# 应用设置
app:
  max_workers: 4
  log_level: INFO
```

环境变量也可以用来覆盖配置文件中的设置，例如：

```
MYSQL_HOST=mysql
MYSQL_PORT=3306
REDIS_HOST=redis
MINIO_ENDPOINT=minio:9000
```

## 安装

1. 克隆代码仓库

```bash
git clone <repository-url>
cd projects/web_api
```

2. 安装依赖
- 推荐使用uv工具，具体安装方法可以查看官网
- https://docs.astral.sh/uv/

```bash
uv venv --pyhton=3.10
uv pip install -r requirements.txt
```

3. 创建配置文件

```bash
cp config.yaml.example config.yaml
# 编辑config.yaml设置您的配置
```

## 运行服务

### 使用Python直接运行

```bash
uvicorn app:app --host 0.0.0.0 --port 8000 --workers 4
```

### 使用Docker运行，需要CUDA

```bash
docker build -t tubo-pdf -f Dockerfile .
docker run -d --name tubo-pdf -p 8000:8000 tubo-pdf
```

### 使用Docker Compose运行

```bash
docker-compose up -d
```

这将启动完整的服务，包括MySQL、Redis和MinIO。

## API文档

启动服务后，访问 http://localhost:8000/docs 查看完整的API文档。

### 主要API端点

- `POST /pdf_parse`: 上传并解析PDF文件
- `POST /pdf_parse_from_minio`: 从MinIO读取并解析PDF文件
- `GET /pdf_job/{job_id}`: 获取任务状态，包含进度百分比
- `GET /pdf_result/{job_id}`: 获取完整的处理结果
- `GET /pdf_result/{job_id}/content`: 仅获取内容列表
- `GET /pdf_result/{job_id}/images`: 仅获取图像
- `GET /pdf_result/{job_id}/layout`: 仅获取布局信息
- `GET /pdf_result/{job_id}/info`: 仅获取PDF信息
- `POST /pdf_job/{job_id}/retry`: 重试失败的任务
- `POST /pdf_jobs/status`: 批量查询多个任务的状态
- `GET /health`: 健康检查
- `GET /metrics`: Prometheus监控指标
- `POST /admin/cleanup`: 清理过期任务数据

### API接口说明

最新版API进行了职责明确化：

1. **文件上传解析** - 使用`/pdf_parse`接口，必须提供PDF文件
2. **MinIO文件解析** - 使用`/pdf_parse_from_minio`接口，指定bucket和object路径

所有处理结果和原始PDF都将保存在MinIO中，路径格式为`jobs/{job_id}/`，图片保存在`jobs/{job_id}/images/`下。

## 存储架构

系统采用分层存储架构：

1. **MinIO** - 作为主要存储，保存：
   - 原始PDF文件
   - 处理结果（JSON, MD, 可视化PDF）
   - 提取的图像

2. **Redis** - 用于存储任务状态和临时数据：
   - 任务状态和进度
   - 错误信息

3. **MySQL** - 用于存储任务元数据和配置：
   - 任务记录和参数
   - 处理配置和状态历史

## 目录结构

```
projects/web_api/
│
├── app.py                 # 主应用文件
├── config.yaml            # 配置文件
├── requirements.txt       # 依赖列表
├── Dockerfile             # Docker构建文件
├── docker-compose.yml     # Docker Compose配置
├── entrypoint.sh          # Docker入口脚本
│
├── utils/                 # 工具类
│   ├── __init__.py
│   ├── config_loader.py   # 配置加载器
│   ├── mysql_utils.py     # MySQL工具类
│   ├── redis_utils.py     # Redis工具类
│   └── minio_utils.py     # MinIO工具类
│
└── tests/                 # 测试用例
    ├── __init__.py
    ├── test_api.py        # API测试
    ├── test_mysql_utils.py # MySQL工具测试
    ├── test_redis_utils.py # Redis工具测试
    └── test_minio_utils.py # MinIO工具测试
```

## 可观测性

本服务提供了完善的监控功能，主要包括：

1. 健康检查：
   - 通过`/health`端点提供服务健康状态
   - 检查所有依赖组件（MySQL、Redis、MinIO）的连接状态

2. 指标监控：
   - 通过`/metrics`端点提供Prometheus格式的监控指标
   - 可以使用Grafana创建监控面板

3. 日志：
   - 服务内置详细的日志记录
   - 记录请求、处理过程和错误信息

## 贡献

欢迎提交问题和PR来改进这个项目！
