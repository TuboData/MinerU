"""
MinIO object storage utilities
"""

import os
import uuid
import logging
from typing import Dict, Optional, Any, List, BinaryIO, Union
import io
from pathlib import Path

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

class MinioUtils:
    """MinIO object storage utilities"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize MinIO client
        
        Args:
            config: MinIO configuration dictionary, or None to use environment variables
        """
        self.minio_client = None
        
        if config is None:
            # Use environment variables for configuration
            self.config = {
                "endpoint": os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
                "access_key": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
                "secret_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
                "secure": os.environ.get("MINIO_SECURE", "false").lower() == "true",
                "bucket_name": os.environ.get("MINIO_BUCKET", "pdf-processor"),
            }
        else:
            self.config = config
            
        self.connect()
        
    def connect(self) -> bool:
        """
        Connect to MinIO server
        
        Returns:
            True if connected successfully, False otherwise
        """
        try:
            endpoint = self.config["endpoint"]
            
            # Minio类不需要HTTP前缀，但需要确保格式正确
            # 如果有HTTP前缀，需要移除
            if endpoint.startswith(('http://', 'https://')):
                endpoint = endpoint.split('://', 1)[1]
            
            self.minio_client = Minio(
                endpoint=endpoint,
                access_key=self.config["access_key"],
                secret_key=self.config["secret_key"],
                secure=self.config["secure"]
            )
            
            # Ensure bucket exists
            bucket_name = self.config["bucket_name"]
            self.ensure_bucket_exists(bucket_name)
            
            logger.info(f"MinIO connection established successfully with bucket: {bucket_name}")
            return True
            
        except Exception as err:
            logger.error(f"Failed to initialize MinIO client: {err}")
            return False
    
    def is_connected(self) -> bool:
        """
        Check if MinIO connection is available
        
        Returns:
            True if connected, False otherwise
        """
        if not self.minio_client:
            return False
            
        try:
            self.minio_client.list_buckets()
            return True
        except Exception:
            return False
    
    def ensure_bucket_exists(self, bucket_name: str) -> bool:
        """
        Ensure the bucket exists, create it if necessary
        
        Args:
            bucket_name: Bucket name
            
        Returns:
            True if bucket exists or was created, False on error
        """
        if not self.minio_client:
            return False
            
        try:
            if not self.minio_client.bucket_exists(bucket_name):
                self.minio_client.make_bucket(bucket_name)
                logger.info(f"Created MinIO bucket: {bucket_name}")
            return True
        except S3Error as err:
            logger.error(f"Error ensuring bucket exists: {err}")
            return False
    
    def upload_file(self, local_path: str, object_name: Optional[str] = None) -> Optional[str]:
        """
        Upload file to MinIO
        
        Args:
            local_path: Local file path
            object_name: Object name in MinIO (defaults to filename if None)
            
        Returns:
            Object name if uploaded successfully, None otherwise
        """
        if not self.is_connected():
            logger.warning("MinIO client not available, skipping upload")
            return None
            
        try:
            bucket_name = self.config["bucket_name"]
            
            # Use filename as object name if not provided
            if object_name is None:
                object_name = os.path.basename(local_path)
                
            self.minio_client.fput_object(
                bucket_name,
                object_name,
                local_path,
            )
            
            logger.info(f"Uploaded {local_path} to MinIO as {object_name}")
            return object_name
            
        except S3Error as err:
            logger.error(f"Error uploading to MinIO: {err}")
            return None
    
    def upload_bytes(self, data: bytes, object_name: str, content_type: str = "application/octet-stream") -> bool:
        """
        Upload bytes data to MinIO
        
        Args:
            data: Bytes data to upload
            object_name: Object name in MinIO
            content_type: Content type of the data
            
        Returns:
            True if uploaded successfully, False otherwise
        """
        if not self.is_connected():
            logger.warning("MinIO client not available, skipping upload")
            return False
            
        try:
            bucket_name = self.config["bucket_name"]
            
            # Convert bytes to file-like object
            data_stream = io.BytesIO(data)
            
            self.minio_client.put_object(
                bucket_name,
                object_name,
                data_stream,
                length=len(data),
                content_type=content_type
            )
            
            logger.info(f"Uploaded bytes data to MinIO as {object_name}")
            return True
            
        except S3Error as err:
            logger.error(f"Error uploading bytes to MinIO: {err}")
            return False
    
    def upload_directory(self, local_dir: str, prefix: str = "") -> List[str]:
        """
        Upload entire directory to MinIO
        
        Args:
            local_dir: Local directory path
            prefix: Object name prefix
            
        Returns:
            List of uploaded object names
        """
        if not self.is_connected():
            logger.warning("MinIO client not available, skipping directory upload")
            return []
            
        uploaded_objects = []
        
        try:
            # Walk through directory
            for root, _, files in os.walk(local_dir):
                for file in files:
                    # Get full file path
                    file_path = os.path.join(root, file)
                    
                    # Calculate relative path from local_dir
                    rel_path = os.path.relpath(file_path, local_dir)
                    
                    # Create object name with prefix
                    object_name = os.path.join(prefix, rel_path).replace("\\", "/")
                    
                    # Upload file
                    result = self.upload_file(file_path, object_name)
                    if result:
                        uploaded_objects.append(result)
                        
            return uploaded_objects
            
        except Exception as err:
            logger.error(f"Error uploading directory to MinIO: {err}")
            return uploaded_objects
    
    def download_file(self, object_name: str, local_path: Optional[str] = None) -> Optional[str]:
        """
        Download file from MinIO
        
        Args:
            object_name: Object name in MinIO
            local_path: Local file path to save to (optional)
            
        Returns:
            Local file path if downloaded successfully, None otherwise
        """
        if not self.is_connected():
            logger.warning("MinIO client not available, skipping download")
            return None
            
        try:
            bucket_name = self.config["bucket_name"]
            
            # Generate temporary file path if not provided
            if local_path is None:
                filename = os.path.basename(object_name)
                local_path = f"/tmp/{uuid.uuid4()}_{filename}"
                
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
            
            # Download object
            self.minio_client.fget_object(bucket_name, object_name, local_path)
            
            logger.info(f"Downloaded {object_name} from MinIO to {local_path}")
            return local_path
            
        except S3Error as err:
            logger.error(f"Error downloading file from MinIO: {err}")
            return None
    
    def get_file_content(self, object_name: str) -> Optional[bytes]:
        """
        Get file content from MinIO as bytes
        
        Args:
            object_name: Object name in MinIO
            
        Returns:
            File content as bytes, or None if error
        """
        if not self.is_connected():
            logger.warning("MinIO client not available, skipping file retrieval")
            return None
            
        try:
            bucket_name = self.config["bucket_name"]
            
            # Get object data
            response = self.minio_client.get_object(bucket_name, object_name)
            
            # Read all data
            data = response.read()
            response.close()
            response.release_conn()
            
            return data
            
        except S3Error as err:
            logger.error(f"S3 error getting file content: {err}")
            return None
        except Exception as err:
            logger.error(f"Error getting file content: {err}")
            return None
            
    def get_file(self, object_name: str) -> Optional[str]:
        """
        Get file content from MinIO as string
        
        Args:
            object_name: Object name in MinIO
            
        Returns:
            File content as string, or None if error
        """
        data = self.get_file_content(object_name)
        if data is None:
            return None
            
        try:
            return data.decode('utf-8')
        except UnicodeDecodeError:
            logger.error(f"Error decoding file {object_name} as UTF-8")
            return None
    
    def list_objects(self, prefix: str = "", recursive: bool = True) -> List[str]:
        """
        List objects in MinIO bucket
        
        Args:
            prefix: Object name prefix
            recursive: Whether to list objects recursively
            
        Returns:
            List of object names
        """
        if not self.is_connected():
            logger.warning("MinIO client not available, skipping object listing")
            return []
            
        try:
            bucket_name = self.config["bucket_name"]
            
            objects = self.minio_client.list_objects(bucket_name, prefix=prefix, recursive=recursive)
            result = [obj.object_name for obj in objects]
            
            return result
            
        except S3Error as err:
            logger.error(f"Error listing objects in MinIO: {err}")
            return []
    
    def delete_object(self, object_name: str) -> bool:
        """
        Delete object from MinIO
        
        Args:
            object_name: Object name in MinIO
            
        Returns:
            True if deleted successfully, False otherwise
        """
        if not self.is_connected():
            logger.warning("MinIO client not available, skipping object deletion")
            return False
            
        try:
            bucket_name = self.config["bucket_name"]
            
            self.minio_client.remove_object(bucket_name, object_name)
            
            logger.info(f"Deleted object {object_name} from MinIO")
            return True
            
        except S3Error as err:
            logger.error(f"Error deleting object from MinIO: {err}")
            return False
    
    def delete_directory(self, prefix: str) -> bool:
        """
        Delete directory (all objects with prefix) from MinIO
        
        Args:
            prefix: Object name prefix
            
        Returns:
            True if deleted successfully, False otherwise
        """
        if not self.is_connected():
            logger.warning("MinIO client not available, skipping directory deletion")
            return False
            
        try:
            bucket_name = self.config["bucket_name"]
            logger.info(f"Starting deletion of objects with prefix '{prefix}' in bucket '{bucket_name}'")
            
            # 验证bucket是否存在
            if not self.minio_client.bucket_exists(bucket_name):
                logger.error(f"Bucket '{bucket_name}' does not exist")
                return False
                
            # 确保前缀以 '/' 结尾，这样才能正确匹配目录
            if not prefix.endswith('/') and '.' not in Path(prefix).name:  # 非文件路径应该以/结尾
                prefix = f"{prefix}/"
                logger.info(f"Adjusted prefix to '{prefix}' for directory matching")
            
            # 获取对象列表
            try:
                objects_iter = self.minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
                objects = list(objects_iter)
                logger.info(f"Found {len(objects)} objects to delete with prefix '{prefix}'")
            except Exception as list_err:
                logger.error(f"Error listing objects with prefix '{prefix}': {list_err}")
                return False
            
            if not objects:
                logger.info(f"No objects found with prefix '{prefix}'")
                return True  # 没有对象需要删除，视为成功
            
            # 准备删除对象
            delete_errors = []
            success_count = 0
            
            # 使用删除单个对象的方法来确保每个对象都被处理
            for obj in objects:
                try:
                    # 从对象中提取名称
                    if hasattr(obj, 'object_name'):
                        obj_name = obj.object_name
                    else:
                        # 跳过无法识别的对象
                        logger.warning(f"Skipping object of type {type(obj)}: {obj}")
                        continue
                        
                    # 执行删除
                    self.minio_client.remove_object(bucket_name, obj_name)
                    success_count += 1
                    
                    # 验证删除是否成功
                    try:
                        self.minio_client.stat_object(bucket_name, obj_name)
                        # 如果能执行到这里，说明对象仍然存在
                        logger.warning(f"Object {obj_name} still exists after deletion attempt")
                        delete_errors.append(f"{obj_name}: Object still exists after deletion")
                    except:
                        # 预期的异常 - 对象已被删除
                        logger.debug(f"Successfully deleted: {obj_name}")
                except Exception as del_err:
                    error_msg = f"{obj.object_name if hasattr(obj, 'object_name') else 'unknown'}: {str(del_err)}"
                    delete_errors.append(error_msg)
                    logger.error(f"Failed to delete object: {error_msg}")
            
            # 记录删除结果
            if delete_errors:
                error_sample = delete_errors[:5]
                logger.error(f"Failed to delete {len(delete_errors)}/{len(objects)} objects: {', '.join(error_sample)}" +
                            ("..." if len(delete_errors) > 5 else ""))
            
            logger.info(f"Successfully deleted {success_count}/{len(objects)} objects with prefix '{prefix}'")
            
            # 只有全部删除成功才返回True
            return success_count == len(objects)
            
        except S3Error as err:
            logger.error(f"S3Error while deleting directory from MinIO: {err}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error deleting directory from MinIO: {str(e)}")
            return False
    
    def object_exists(self, object_name: str) -> bool:
        """
        Check if object exists in MinIO
        
        Args:
            object_name: Object name in MinIO
            
        Returns:
            True if object exists, False otherwise
        """
        if not self.is_connected():
            logger.warning("MinIO client not available, skipping object check")
            return False
            
        try:
            bucket_name = self.config["bucket_name"]
            
            # Try to get object stats, will raise exception if object doesn't exist
            self.minio_client.stat_object(bucket_name, object_name)
            return True
            
        except S3Error:
            return False
    
    def close(self) -> None:
        """Close the MinIO connection"""
        # MinIO client doesn't require explicit closing
        self.minio_client = None
        logger.info("MinIO connection closed")
        
    def get_job_files(self, job_id: str) -> Dict[str, List[str]]:
        """
        获取指定任务ID的所有相关文件信息
        
        Args:
            job_id: 任务ID
            
        Returns:
            字典，包含不同类型文件的列表，例如：
            {
                "pdf": [...],
                "images": [...],
                "json": [...],
                "md": [...]
            }
        """
        if not self.is_connected():
            logger.warning("MinIO client not available, skipping file query")
            return {}
            
        try:
            result = {
                "pdf": [],
                "images": [],
                "json": [],
                "md": []
            }
            
            bucket_name = self.config["bucket_name"]
            prefix = f"jobs/{job_id}"
            
            # 获取所有对象
            objects = list(self.minio_client.list_objects(bucket_name, prefix=prefix, recursive=True))
            logger.info(f"Found {len(objects)} objects for job {job_id}")
            
            # 分类文件
            for obj in objects:
                if not hasattr(obj, 'object_name'):
                    continue
                    
                obj_name = obj.object_name
                if obj_name.endswith('.pdf'):
                    result["pdf"].append(obj_name)
                elif obj_name.endswith('.json'):
                    result["json"].append(obj_name)
                elif obj_name.endswith('.md'):
                    result["md"].append(obj_name)
                elif '/images/' in obj_name:
                    result["images"].append(obj_name)
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting job files from MinIO: {str(e)}")
            return {} 