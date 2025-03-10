"""
配置加载器
"""

import os
import logging
import yaml
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class ConfigLoader:
    """
    配置加载器，用于从YAML文件加载配置
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化配置加载器
        
        Args:
            config_path: 配置文件路径，如果为None，则使用默认路径
        """
        self.config_path = config_path or os.environ.get("CONFIG_PATH", "config.yaml")
        self.config = {}
        self.load_config()
        
    def load_config(self) -> None:
        """
        从配置文件加载配置
        """
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
                logger.info(f"Loaded configuration from {self.config_path}")
        except FileNotFoundError:
            logger.warning(f"Config file not found at {self.config_path}, using default values")
            self.config = {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing config file: {e}")
            self.config = {}
            
    def get_mysql_config(self) -> Dict[str, Any]:
        """
        获取MySQL配置
        
        Returns:
            MySQL配置字典
        """
        mysql_config = self.config.get('mysql', {})
        
        # 获取环境变量配置，优先级高于配置文件
        env_config = {
            "host": os.environ.get("MYSQL_HOST"),
            "port": os.environ.get("MYSQL_PORT"),
            "user": os.environ.get("MYSQL_USER"),
            "password": os.environ.get("MYSQL_PASSWORD"),
            "database": os.environ.get("MYSQL_DATABASE"),
        }
        
        # 移除None值
        env_config = {k: v for k, v in env_config.items() if v is not None}
        
        # 转换端口为整数
        if "port" in env_config and env_config["port"]:
            try:
                env_config["port"] = int(env_config["port"])
            except ValueError:
                del env_config["port"]
        
        # 合并配置，环境变量优先级高于配置文件
        mysql_config.update(env_config)
        
        return mysql_config
    
    def get_redis_config(self) -> Dict[str, Any]:
        """
        获取Redis配置
        
        Returns:
            Redis配置字典
        """
        redis_config = self.config.get('redis', {})
        
        # 获取环境变量配置，优先级高于配置文件
        env_config = {
            "host": os.environ.get("REDIS_HOST"),
            "port": os.environ.get("REDIS_PORT"),
            "db": os.environ.get("REDIS_DB"),
            "password": os.environ.get("REDIS_PASSWORD"),
        }
        
        # 移除None值
        env_config = {k: v for k, v in env_config.items() if v is not None}
        
        # 转换数值字段
        if "port" in env_config and env_config["port"]:
            try:
                env_config["port"] = int(env_config["port"])
            except ValueError:
                del env_config["port"]
                
        if "db" in env_config and env_config["db"]:
            try:
                env_config["db"] = int(env_config["db"])
            except ValueError:
                del env_config["db"]
        
        # 合并配置，环境变量优先级高于配置文件
        redis_config.update(env_config)
        
        # 确保decode_responses存在
        if "decode_responses" not in redis_config:
            redis_config["decode_responses"] = True
        
        return redis_config
    
    def get_minio_config(self) -> Dict[str, Any]:
        """
        获取MinIO配置
        
        Returns:
            MinIO配置字典
        """
        minio_config = self.config.get('minio', {})
        
        # 获取环境变量配置，优先级高于配置文件
        env_config = {
            "endpoint": os.environ.get("MINIO_ENDPOINT"),
            "access_key": os.environ.get("MINIO_ACCESS_KEY"),
            "secret_key": os.environ.get("MINIO_SECRET_KEY"),
            "secure": os.environ.get("MINIO_SECURE"),
            "bucket_name": os.environ.get("MINIO_BUCKET"),
        }
        
        # 移除None值
        env_config = {k: v for k, v in env_config.items() if v is not None}
        
        # 转换secure为布尔值
        if "secure" in env_config:
            env_config["secure"] = env_config["secure"].lower() == "true"
        
        # 合并配置，环境变量优先级高于配置文件
        minio_config.update(env_config)
        
        return minio_config
    
    def get_app_config(self) -> Dict[str, Any]:
        """
        获取应用配置
        
        Returns:
            应用配置字典
        """
        app_config = self.config.get('app', {})
        
        # 获取环境变量配置，优先级高于配置文件
        env_config = {
            "max_workers": os.environ.get("APP_MAX_WORKERS"),
            "log_level": os.environ.get("APP_LOG_LEVEL"),
            "clean_interval_hours": os.environ.get("APP_CLEAN_INTERVAL_HOURS"),
            "job_expiry_hours": os.environ.get("APP_JOB_EXPIRY_HOURS"),
        }
        
        # 移除None值
        env_config = {k: v for k, v in env_config.items() if v is not None}
        
        # 转换数值字段
        for key in ["max_workers", "clean_interval_hours", "job_expiry_hours"]:
            if key in env_config and env_config[key]:
                try:
                    env_config[key] = int(env_config[key])
                except ValueError:
                    del env_config[key]
        
        # 合并配置，环境变量优先级高于配置文件
        app_config.update(env_config)
        
        return app_config 