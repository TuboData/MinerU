"""
测试MySQL工具类
"""

import unittest
import os
import uuid
from unittest.mock import patch, MagicMock
from datetime import datetime

import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool

from utils.mysql_utils import MySQLUtils

class TestMySQLUtils(unittest.TestCase):
    """MySQL工具类测试"""

    def setUp(self):
        """设置测试环境"""
        # 创建测试环境变量
        self.env_patcher = patch.dict('os.environ', {
            'MYSQL_HOST': 'test_host',
            'MYSQL_PORT': '3306',
            'MYSQL_USER': 'test_user',
            'MYSQL_PASSWORD': 'test_password',
            'MYSQL_DATABASE': 'test_db'
        })
        self.env_patcher.start()
        
        # 模拟MySQLConnectionPool
        self.pool_patcher = patch('mysql.connector.pooling.MySQLConnectionPool')
        self.mock_pool = self.pool_patcher.start()
        
        # 创建连接池模拟
        self.mock_pool_instance = MagicMock()
        self.mock_pool.return_value = self.mock_pool_instance
        
        # 模拟数据库连接和游标
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_pool_instance.get_connection.return_value = self.mock_conn
        self.mock_conn.cursor.return_value = self.mock_cursor
        
        # 创建工具类实例
        self.mysql_utils = MySQLUtils()
        
    def tearDown(self):
        """清理测试环境"""
        # 停止所有patch
        self.env_patcher.stop()
        self.pool_patcher.stop()
        
    def test_initialize_pool(self):
        """测试连接池初始化"""
        # 验证连接池创建时使用了正确的配置
        self.mock_pool.assert_called_once()
        
        call_args = self.mock_pool.call_args[1]
        self.assertEqual(call_args['pool_name'], "pdf_processor_pool")
        self.assertEqual(call_args['host'], "test_host")
        self.assertEqual(call_args['user'], "test_user")
        self.assertEqual(call_args['password'], "test_password")
        self.assertEqual(call_args['database'], "test_db")
        
    def test_execute_query(self):
        """测试执行查询"""
        # 设置模拟返回值
        self.mock_cursor.with_rows = True
        self.mock_cursor.fetchall.return_value = [
            {'id': 1, 'name': 'test1'},
            {'id': 2, 'name': 'test2'}
        ]
        
        # 执行查询
        result = self.mysql_utils.execute_query("SELECT * FROM test_table")
        
        # 验证调用
        self.mock_pool_instance.get_connection.assert_called_once()
        self.mock_conn.cursor.assert_called_once()
        self.mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table", ())
        self.mock_cursor.fetchall.assert_called_once()
        self.mock_conn.commit.assert_called_once()
        
        # 验证结果
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['name'], 'test1')
        self.assertEqual(result[1]['name'], 'test2')
        
    def test_execute_update(self):
        """测试执行更新"""
        # 设置模拟返回值
        self.mock_cursor.rowcount = 1
        
        # 执行更新
        affected_rows = self.mysql_utils.execute_update(
            "UPDATE test_table SET name = %s WHERE id = %s",
            ("new_name", 1)
        )
        
        # 验证调用
        self.mock_pool_instance.get_connection.assert_called_once()
        self.mock_conn.cursor.assert_called_once()
        self.mock_cursor.execute.assert_called_once_with(
            "UPDATE test_table SET name = %s WHERE id = %s",
            ("new_name", 1)
        )
        self.mock_conn.commit.assert_called_once()
        
        # 验证结果
        self.assertEqual(affected_rows, 1)
        
    def test_create_tables(self):
        """测试创建表"""
        # 执行创建表
        result = self.mysql_utils.create_tables()
        
        # 验证调用
        self.assertEqual(self.mock_cursor.execute.call_count, 2)
        self.mock_conn.commit.assert_called_once_with()
        
        # 验证结果
        self.assertTrue(result)
        
    def test_save_job(self):
        """测试保存任务"""
        # 设置模拟返回值 - 任务存在
        self.mock_cursor.fetchall.return_value = [{'id': 'test_job_id'}]
        
        # 执行保存
        job_id = "test_job_id"
        result = self.mysql_utils.save_job(
            job_id=job_id,
            pdf_name="test.pdf",
            output_path="/tmp/output",
            status="processing",
            progress=50.0
        )
        
        # 验证调用
        self.assertEqual(self.mock_cursor.execute.call_count, 2)
        self.mock_conn.commit.assert_called_once()
        
        # 验证结果
        self.assertTrue(result)
        
    def test_save_job_new(self):
        """测试保存新任务"""
        # 设置模拟返回值 - 任务不存在
        self.mock_cursor.fetchall.return_value = []
        
        # 执行保存
        job_id = "test_job_id"
        result = self.mysql_utils.save_job(
            job_id=job_id,
            pdf_name="test.pdf",
            output_path="/tmp/output",
            status="pending",
            progress=0.0
        )
        
        # 验证调用
        self.assertEqual(self.mock_cursor.execute.call_count, 2)
        self.mock_conn.commit.assert_called_once_with()  # 确保只调用一次 commit
        
        # 验证结果
        self.assertTrue(result)
        
    def test_get_job(self):
        """测试获取任务信息"""
        # 模拟查询结果
        self.mock_cursor.fetchall.return_value = [(
            "test_job_id", "test.pdf", "completed", 100.0, 
            datetime.now(), datetime.now(), datetime.now(), None
        )]
        
        # 执行查询
        job_id = "test_job_id"
        result = self.mysql_utils.get_job(job_id)
        
        # 验证调用
        self.mock_pool_instance.get_connection.assert_called_once()
        self.mock_conn.cursor.assert_called_once()
        self.assertEqual(result["id"], "test_job_id")

    def test_error_handling(self):
        """测试错误处理"""
        # 模拟连接失败
        self.mock_cursor.execute.side_effect = mysql.connector.Error("Test error")
        
        # 执行查询
        result = self.mysql_utils.execute_query("SELECT * FROM test_table")
        
        # 验证结果
        self.assertEqual(result, [])
        self.mock_conn.rollback.assert_called_once()


if __name__ == '__main__':
    unittest.main() 