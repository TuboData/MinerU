FROM nvidia/cuda:12.1.0-base-ubuntu22.04 AS base

WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1


FROM base AS build

RUN sed -i 's@//.*archive.ubuntu.com@//mirrors.ustc.edu.cn@g' /etc/apt/sources.list

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

COPY pyproject.toml uv.lock ./

RUN export http_proxy='http://10.147.16.63:7890'; \
    export https_proxy='http://10.147.16.63:7890'; \
    curl -LsSf https://astral.sh/uv/install.sh | sh

RUN ../root/.local/bin/uv sync; \
    ../root/.local/bin/uv pip install marker-pdf; \
    ../root/.local/bin/uv pip install mineru; \
    rm -rf /root/.cache/uv

ENV http_proxy=''
ENV https_proxy=''
# COPY models

#COPY download_models.py .
#RUN . /app/venv/bin/activate && \
#    python ./download_models.py


# Create volume for paddleocr models
# ADD models/paddleocr/ /root/.paddleocr/

# Create directories for application
RUN mkdir -p /app/utils /app/tests

# Copy configuration file
COPY config.yaml /app/config.yaml

# Copy the app and its utilities
COPY entrypoint.sh /app/entrypoint.sh
# ADD models/miner-u/ models/
# COPY docker/magic-pdf.json /root/magic-pdf.json
COPY app.py /app/app.py
COPY utils/ /app/utils/
COPY busi/ /app/busi/

ADD models/marker/ /root/.cache/datalab/models/

RUN . /app/.venv/bin/activate; \
    mineru-models-download -s modelscope -m all
# Expose the port that FastAPI will run on
EXPOSE 8000

# Command to run FastAPI using Uvicorn, pointing to app.py and binding to 0.0.0.0:8000
ENTRYPOINT [ "/app/entrypoint.sh" ]
CMD ["--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
