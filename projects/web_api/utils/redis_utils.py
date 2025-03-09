"""
Redis utilities for job status and caching
"""

import os
import time
import logging
import json
from typing import Dict, Optional, Any, List, Union

import redis

logger = logging.getLogger(__name__)

class RedisUtils:
    """Redis utilities for job status and caching"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Redis connection
        
        Args:
            config: Redis configuration dictionary, or None to use environment variables
        """
        self.redis_client = None
        
        if config is None:
            # Use environment variables for configuration
            self.config = {
                "host": os.environ.get("REDIS_HOST", "localhost"),
                "port": int(os.environ.get("REDIS_PORT", 6379)),
                "db": int(os.environ.get("REDIS_DB", 0)),
                "password": os.environ.get("REDIS_PASSWORD", None),
                "decode_responses": True  # Auto-decode Redis responses to strings
            }
        else:
            self.config = config
            
        self.connect()
    
    def connect(self) -> bool:
        """
        Connect to Redis server
        
        Returns:
            True if connected successfully, False otherwise
        """
        try:
            self.redis_client = redis.Redis(**self.config)
            self.redis_client.ping()
            logger.info("Redis connection established successfully")
            return True
        except redis.ConnectionError as err:
            logger.error(f"Failed to connect to Redis: {err}")
            return False
    
    def is_connected(self) -> bool:
        """
        Check if Redis connection is available
        
        Returns:
            True if connected, False otherwise
        """
        if not self.redis_client:
            return False
            
        try:
            self.redis_client.ping()
            return True
        except redis.RedisError:
            return False
    
    def update_job_status(self, job_id: str, status: str, 
                          progress: float = 0, error: Optional[str] = None,
                          expiry_seconds: int = 86400) -> bool:
        """
        Update job status in Redis
        
        Args:
            job_id: Unique job identifier
            status: Job status
            progress: Job progress (0-100)
            error: Error message (if any)
            expiry_seconds: Key expiry time in seconds
            
        Returns:
            True if updated successfully, False otherwise
        """
        if not self.is_connected():
            logger.warning("Redis client not available, skipping status update")
            return False
            
        try:
            job_key = f"pdf_job:{job_id}"
            job_data = {
                "status": status,
                "progress": progress,
                "updated_at": time.time()
            }
            
            if error:
                job_data["error"] = error
                
            self.redis_client.hset(job_key, mapping=job_data)
            
            # Set expiry time
            if expiry_seconds > 0:
                self.redis_client.expire(job_key, expiry_seconds)
                
            logger.info(f"Updated job {job_id} status to {status} in Redis")
            return True
            
        except redis.RedisError as err:
            logger.error(f"Error updating Redis: {err}")
            return False
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job status from Redis
        
        Args:
            job_id: Unique job identifier
            
        Returns:
            Job status dictionary, or None if not found
        """
        if not self.is_connected():
            logger.warning("Redis client not available, skipping status query")
            return None
            
        try:
            job_key = f"pdf_job:{job_id}"
            job_data = self.redis_client.hgetall(job_key)
            
            if not job_data:
                return None
            
            # Convert progress to float if it exists and is a string
            if "progress" in job_data and isinstance(job_data["progress"], str):
                try:
                    job_data["progress"] = float(job_data["progress"])
                except (ValueError, TypeError):
                    job_data["progress"] = 0.0
                
            return job_data
            
        except redis.RedisError as err:
            logger.error(f"Error querying Redis: {err}")
            return None
    
    def update_job_progress(self, job_id: str, progress: float) -> bool:
        """
        Update only the job progress
        
        Args:
            job_id: Unique job identifier
            progress: Job progress (0-100)
            
        Returns:
            True if updated successfully, False otherwise
        """
        if not self.is_connected():
            logger.warning("Redis client not available, skipping progress update")
            return False
            
        try:
            job_key = f"pdf_job:{job_id}"
            
            # Update progress and timestamp
            self.redis_client.hset(job_key, mapping={
                "progress": progress,
                "updated_at": time.time()
            })
            
            logger.info(f"Updated job {job_id} progress to {progress}% in Redis")
            return True
            
        except redis.RedisError as err:
            logger.error(f"Error updating progress in Redis: {err}")
            return False
    
    def delete_job(self, job_id: str) -> bool:
        """
        Delete job data from Redis
        
        Args:
            job_id: Unique job identifier
            
        Returns:
            True if deleted successfully, False otherwise
        """
        if not self.is_connected():
            logger.warning("Redis client not available, skipping job deletion")
            return False
            
        try:
            job_key = f"pdf_job:{job_id}"
            deleted = self.redis_client.delete(job_key)
            
            if deleted:
                logger.info(f"Deleted job {job_id} data from Redis")
                return True
            else:
                logger.warning(f"Job {job_id} not found in Redis")
                return False
                
        except redis.RedisError as err:
            logger.error(f"Error deleting job from Redis: {err}")
            return False
    
    def get_all_jobs(self, pattern: str = "pdf_job:*") -> List[Dict[str, Any]]:
        """
        Get all jobs matching the pattern
        
        Args:
            pattern: Redis key pattern
            
        Returns:
            List of job data dictionaries
        """
        if not self.is_connected():
            logger.warning("Redis client not available, skipping jobs query")
            return []
            
        try:
            result = []
            
            # Get all keys matching the pattern
            keys = self.redis_client.keys(pattern)
            
            # Get data for each key
            for key in keys:
                job_id = key.split(":")[-1]
                job_data = self.get_job_status(job_id)
                
                if job_data:
                    job_data["job_id"] = job_id
                    result.append(job_data)
                    
            return result
            
        except redis.RedisError as err:
            logger.error(f"Error querying Redis for jobs: {err}")
            return []
    
    def close(self) -> None:
        """Close the Redis connection"""
        if self.redis_client:
            try:
                self.redis_client.close()
                logger.info("Redis connection closed")
            except redis.RedisError as err:
                logger.error(f"Error closing Redis connection: {err}")
            finally:
                self.redis_client = None 