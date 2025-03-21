FROM nvidia/cuda:12.1.0-base-ubuntu22.04 AS base

WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1


FROM base AS build

RUN sed -i 's|http://archive.ubuntu.com|https://mirrors.tuna.tsinghua.edu.cn|g' /etc/apt/sources.list;

# Update the package list and install necessary packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends  \
      build-essential \
      git \
      wget \
      curl \
      ca-certificates \
      gnupg2 \
      libgl1 \
      libgl1-mesa-glx \
      libglib2.0-0 \
      libsm6 \
      libxext6 \
      libxrender-dev \
      libgomp1 \
      pkg-config \
      cmake \
      python3.10 \
      python3.10-venv \
      python3.10-dev \
      && ln -s /usr/bin/python3.10 /usr/bin/python \
      && apt-get clean \
      && rm -rf /var/lib/apt/lists/



# Build Python dependencies
# ADD .venv/ venv/

#RUN python -m venv /app/venv
#ADD .venv/lib/python3.12/ venv/lib/python3.10/
#ADD .venv/share/ venv/share/
#ADD .venv/bin/ venv/bin-back/
#RUN mv venv/bin-back/* venv/bin/

COPY requirements.txt .
RUN grep -v "detectron2" requirements.txt > requirements_without_detectron2.txt
RUN python -m venv /app/venv && \
    . /app/venv/bin/activate && \
    pip config set global.index-url https://mirrors.aliyun.com/pypi/simple && \
    pip config set global.trusted-host mirrors.aliyun.com && \
    pip install torch==2.3.1 torchvision==0.18.1 numpy pillow  && \
    pip install -r requirements_without_detectron2.txt

RUN . /app/venv/bin/activate \
    && pip install --no-build-isolation git+https://github.com/facebookresearch/detectron2.git@9604f5995cc628619f0e4fd913453b4d7d61db3f



RUN . /app/venv/bin/activate \
    && pip uninstall -y paddlepaddle \
    && pip install -i https://www.paddlepaddle.org.cn/packages/stable/cu118/ paddlepaddle-gpu==3.0.0rc1

# COPY models
ADD models/ models/
#COPY download_models.py .
#RUN . /app/venv/bin/activate && \
#    python ./download_models.py


# Create volume for paddleocr models
RUN mkdir -p /root/.paddleocr
VOLUME [ "/root/.paddleocr" ]

# Create directories for application
RUN mkdir -p /app/utils /app/tests

# Copy configuration file
COPY config.yaml /app/config.yaml

# Copy the app and its utilities
COPY entrypoint.sh /app/entrypoint.sh
COPY docker/magic-pdf.json /root/magic-pdf.json
COPY app.py /app/app.py
COPY utils/ /app/utils/

# Expose the port that FastAPI will run on
EXPOSE 8000

# Command to run FastAPI using Uvicorn, pointing to app.py and binding to 0.0.0.0:8000
ENTRYPOINT [ "/app/entrypoint.sh" ]
CMD ["--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
