"""
MySQL database utilities
"""

import os
import logging
from typing import Dict, List, Optional, Any, Union, Tuple
import mysql.connector
from mysql.connector import pooling
import time
import json
from datetime import datetime
import re

logger = logging.getLogger(__name__)

class MySQLUtils:
    """MySQL database utilities"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize MySQL connection pool
        
        Args:
            config: MySQL configuration dictionary, or None to use environment variables
        """
        self.db_pool = None
        
        if config is None:
            # Use environment variables for configuration
            self.config = {
                "host": os.environ.get("MYSQL_HOST", "localhost"),
                "port": int(os.environ.get("MYSQL_PORT", 3306)),
                "user": os.environ.get("MYSQL_USER", "root"),
                "password": os.environ.get("MYSQL_PASSWORD", "password"),
                "database": os.environ.get("MYSQL_DATABASE", "pdf_processor"),
            }
        else:
            self.config = config
            
        # 输出配置信息以便调试
        logger.info(f"MySQL connection config: {self.config}")
            
        # 先确保数据库存在，再初始化连接池
        self.ensure_database_exists()
        self.initialize_pool()
    
    def ensure_database_exists(self) -> None:
        """确保数据库存在，如果不存在则创建"""
        try:
            # 创建一个不指定数据库的临时连接
            temp_config = self.config.copy()
            if 'database' in temp_config:
                db_name = temp_config.pop('database')
            else:
                db_name = 'pdf_processor'
            
            # 验证数据库名只包含合法字符
            if not re.match(r'^[a-zA-Z0-9_]+$', db_name):
                logger.error(f"Invalid database name: {db_name}")
                raise ValueError("Database name can only contain letters, numbers and underscores")
                
            conn = mysql.connector.connect(**temp_config)
            cursor = conn.cursor()
            
            # 检查数据库是否存在
            cursor.execute("SHOW DATABASES LIKE %s", (db_name,))
            result = cursor.fetchone()
            
            # 如果数据库不存在，则创建
            if not result:
                logger.info(f"Database '{db_name}' does not exist. Creating...")
                # MySQL连接器不支持参数化DDL语句，但我们已验证数据库名是安全的
                cursor.execute(f"CREATE DATABASE {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
                logger.info(f"Database '{db_name}' created successfully")
            else:
                logger.info(f"Database '{db_name}' already exists")
                
            # 使用该数据库
            cursor.execute(f"USE {db_name}")
            
            cursor.close()
            conn.close()
        except mysql.connector.Error as err:
            logger.error(f"Failed to ensure database exists: {err}")
    
    def initialize_pool(self) -> None:
        """Initialize the database connection pool"""
        try:
            self.db_pool = pooling.MySQLConnectionPool(
                pool_name="pdf_processor_pool",
                pool_size=5,
                **self.config
            )
            logger.info("MySQL connection pool created successfully")
        except mysql.connector.Error as err:
            logger.error(f"Failed to create MySQL connection pool: {err}")
    
    def execute_query(self, query: str, params: Optional[Union[Dict, Tuple, List]] = None) -> List[Dict]:
        """
        Execute a database query and return results
        
        Args:
            query: SQL query to execute
            params: Parameters for the query
            
        Returns:
            List of dictionaries containing the results
        """
        if not self.db_pool:
            logger.warning("MySQL connection pool not available")
            return []
            
        conn = None
        cursor = None
        results = []
        
        try:
            # 设置获取连接的超时时间
            conn = self.db_pool.get_connection()
            cursor = conn.cursor(dictionary=True)
            
            # 设置查询超时
            cursor.execute("SET SESSION MAX_EXECUTION_TIME = 3000")  # 3秒查询超时
            
            # 添加执行查询的超时保护
            start_time = time.time()
            cursor.execute(query, params or ())
            
            # 检查查询是否花费了太长时间
            if time.time() - start_time > 2:  # 如果查询超过2秒
                logger.warning(f"Slow query detected: {query}")
            
            if cursor.with_rows:
                results = cursor.fetchall()
                
            conn.commit()
            return results
            
        except mysql.connector.Error as err:
            logger.error(f"Error executing query: {err}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass  # 忽略回滚失败的错误
            return []
            
        except Exception as err:
            logger.error(f"Unexpected error executing query: {err}")
            return []
            
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass  # 忽略关闭游标失败的错误
            if conn:
                try:
                    conn.close()
                except:
                    pass  # 忽略关闭连接失败的错误
    
    def execute_update(self, query: str, params: Optional[Union[Dict, Tuple, List]] = None) -> int:
        """
        Execute an update query and return affected rows
        
        Args:
            query: SQL update query to execute
            params: Parameters for the query
            
        Returns:
            Number of affected rows
        """
        if not self.db_pool:
            logger.warning("MySQL connection pool not available")
            return 0
            
        conn = None
        cursor = None
        
        try:
            conn = self.db_pool.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            
            affected_rows = cursor.rowcount
            conn.commit()
            return affected_rows
            
        except mysql.connector.Error as err:
            logger.error(f"Error executing update: {err}")
            if conn:
                conn.rollback()
            return 0
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def create_tables(self) -> bool:
        """
        Create necessary database tables
        
        Returns:
            True if tables created successfully, False otherwise
        """
        create_jobs_table_query = """
        CREATE TABLE IF NOT EXISTS pdf_jobs (
            id VARCHAR(36) PRIMARY KEY,
            pdf_name VARCHAR(255) NOT NULL,
            status VARCHAR(20) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            completed_at TIMESTAMP NULL,
            error_message TEXT NULL
        )
        """
        
        conn = None
        cursor = None
        
        try:
            # 检查连接池是否可用，如果不可用则尝试创建临时连接
            if self.db_pool:
                conn = self.db_pool.get_connection()
            else:
                logger.warning("Connection pool not available, creating temporary connection")
                # 使用self.config获取连接参数，确保可以连接到指定的数据库
                conn = mysql.connector.connect(**self.config)
                
            cursor = conn.cursor()
            
            # 设置当前数据库
            db_name = self.config.get("database", "pdf_processor")
            # 验证数据库名是否安全
            if not re.match(r'^[a-zA-Z0-9_]+$', db_name):
                logger.error(f"Invalid database name: {db_name}")
                raise ValueError("Database name can only contain letters, numbers and underscores")
                
            # 使用参数化查询设置数据库（MySQL不支持参数化USE语句，但我们已验证数据库名是安全的）
            cursor.execute(f"USE {db_name}")
            
            # 执行表的创建语句
            cursor.execute(create_jobs_table_query)
            # 提交事务
            conn.commit()
            logger.info("Database tables created successfully")
            return True
        except Exception as err:
            logger.error(f"Error creating database tables: {err}")
            if conn:
                conn.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                
    def save_job(self, job_id: str, pdf_name: str, status: str, 
                error_message: Optional[str] = None) -> bool:
        """
        Save job information to database
        
        Args:
            job_id: Unique job identifier
            pdf_name: Name of the PDF file
            status: Job status ('pending', 'processing', 'completed', 'failed')
            error_message: Error message if job failed
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            # 检查任务是否已存在
            check_query = "SELECT id FROM pdf_jobs WHERE id = %s"
            results = self.execute_query(check_query, (job_id,))
            
            if results:
                # 如果存在，则更新
                update_query = """
                UPDATE pdf_jobs 
                SET pdf_name = %s, status = %s, error_message = %s
                WHERE id = %s
                """
                self.execute_update(update_query, (pdf_name, status, error_message, job_id))
                logger.info(f"Updated existing job {job_id} in database")
            else:
                # 如果不存在，则插入新记录
                insert_query = """
                INSERT INTO pdf_jobs (id, pdf_name, status, error_message)
                VALUES (%s, %s, %s, %s)
                """
                self.execute_update(insert_query, (job_id, pdf_name, status, error_message))
                logger.info(f"Inserted new job {job_id} into database")
                
            return True
        except Exception as err:
            logger.error(f"Error saving job to database: {err}")
            return False
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job information from the database
        
        Args:
            job_id: Unique job identifier
            
        Returns:
            Job information dictionary, or None if not found
        """
        job_query = """
        SELECT * FROM pdf_jobs WHERE id = %s
        """
        
        results = self.execute_query(job_query, (job_id,))
        if results:
            return results[0]
        return None
    
    def update_job(self, job_id: str, update_data: Dict[str, Any]) -> bool:
        """
        Update job information
        
        Args:
            job_id: Unique job identifier
            update_data: Dictionary with fields to update
            
        Returns:
            True if updated successfully, False otherwise
        """
        if not update_data:
            logger.warning(f"No update data provided for job {job_id}")
            return False
            
        try:
            # 构建UPDATE语句
            set_clauses = []
            params = []
            
            # 处理状态变更
            if "status" in update_data:
                set_clauses.append("status = %s")
                params.append(update_data["status"])
                
                # 如果状态为完成或失败，自动更新完成时间
                if update_data["status"] in ["completed", "failed"]:
                    set_clauses.append("completed_at = %s")
                    params.append(datetime.now())
                    
            # 处理错误信息
            if "error_message" in update_data:
                set_clauses.append("error_message = %s")
                params.append(update_data["error_message"])
                
            # 处理PDF名称
            if "pdf_name" in update_data:
                set_clauses.append("pdf_name = %s")
                params.append(update_data["pdf_name"])
                
            # 如果没有需要更新的字段，则返回False
            if not set_clauses:
                logger.warning("No valid fields to update")
                return False
                
            # 添加任务ID作为WHERE条件参数
            params.append(job_id)
            
            # 构建完整的UPDATE语句
            update_query = f"""
            UPDATE pdf_jobs 
            SET {', '.join(set_clauses)}
            WHERE id = %s
            """
            
            # 执行更新
            rows_affected = self.execute_update(update_query, tuple(params))
            
            if rows_affected > 0:
                logger.info(f"Job {job_id} updated successfully in MySQL")
                return True
            else:
                logger.warning(f"Job {job_id} not found or no changes made")
                return False
                
        except Exception as err:
            logger.error(f"Error updating job in MySQL: {err}")
            return False
    
    def delete_job(self, job_id: str) -> bool:
        """
        Delete a job and its parameters from the database
        
        Args:
            job_id: Unique job identifier
            
        Returns:
            True if deleted successfully, False otherwise
        """
        try:
            # 由于有外键约束，删除主表记录会自动删除关联的参数记录
            delete_query = "DELETE FROM pdf_jobs WHERE id = %s"
            
            affected_rows = self.execute_update(delete_query, (job_id,))
            if affected_rows > 0:
                logger.info(f"Job {job_id} deleted successfully")
                return True
            else:
                logger.warning(f"Job {job_id} not found or not deleted")
                return False
                
        except Exception as err:
            logger.error(f"Error deleting job from MySQL: {err}")
            return False
    
    def close(self) -> None:
        """Close the database connection pool"""
        # MySQL Connector automatically handles connection pool cleanup
        self.db_pool = None
        logger.info("MySQL connection pool closed")

    def save_pdf_job_result(self, job_id: str, result_dict: Dict[str, Any]) -> bool:
        """
        完整处理PDF任务结果保存和状态更新 - 只更新状态，不保存结果数据
        
        Args:
            job_id: 任务ID
            result_dict: 包含所有结果数据的字典（仅用于日志记录，不存储）
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            # 记录结果数据大小，但不保存
            result_size = len(str(result_dict)) if result_dict else 0
            logger.info(f"Job {job_id} completed with result data size: {result_size} bytes (stored in MinIO)")
            
            # 只更新任务状态为已完成
            update_data = {
                "status": "completed",
                "progress": 100.0,
                "completed_at": datetime.now()
            }
            
            # 使用update_job方法更新状态
            updated = self.update_job(job_id, update_data)
            
            # 如果update_job失败，使用save_job作为备份方案
            if not updated:
                logger.warning(f"Failed to update job {job_id} using update_job, trying save_job")
                self.save_job(
                    job_id=job_id,
                    pdf_name="", # 这里不需要更新pdf_name
                    status="completed",
                    error_message=None
                )
            
            logger.info(f"PDF job {job_id} status updated to completed successfully")
            return True
            
        except Exception as err:
            logger.error(f"Error updating PDF job status: {err}")
            return False
            
    def save_pdf_job_error(self, job_id: str, error_message: str) -> bool:
        """
        处理PDF任务失败状态的保存
        
        Args:
            job_id: 任务ID
            error_message: 错误信息
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            # 更新任务失败状态
            update_data = {
                "status": "failed",
                "progress": 0.0,
                "completed_at": datetime.now(),
                "error_message": error_message
            }
            
            # 尝试更新任务状态
            updated = self.update_job(job_id, update_data)
            
            # 如果update_job失败，使用save_job作为备份方案
            if not updated:
                logger.warning(f"Failed to update failed job {job_id} using update_job, trying save_job")
                self.save_job(
                    job_id=job_id,
                    pdf_name="", # 这里不需要更新pdf_name
                    status="failed",
                    error_message=error_message
                )
            
            logger.info(f"PDF job {job_id} error status saved successfully")
            return True
            
        except Exception as err:
            logger.error(f"Error saving PDF job error status: {err}")
            return False 