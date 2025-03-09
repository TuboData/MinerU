"""
测试Redis工具类
"""

import unittest
import os
import time
from unittest.mock import patch, MagicMock

import redis

from utils.redis_utils import RedisUtils

class TestRedisUtils(unittest.TestCase):
    """Redis工具类测试"""

    def setUp(self):
        """设置测试环境"""
        # 创建测试环境变量
        self.env_patcher = patch.dict('os.environ', {
            'REDIS_HOST': 'test_host',
            'REDIS_PORT': '6379',
            'REDIS_DB': '0',
            'REDIS_PASSWORD': 'test_password'
        })
        self.env_patcher.start()
        
        # 模拟Redis客户端
        self.redis_patcher = patch('redis.Redis')
        self.mock_redis = self.redis_patcher.start()
        
        # 创建Redis客户端模拟
        self.mock_redis_instance = MagicMock()
        self.mock_redis.return_value = self.mock_redis_instance
        
        # 创建工具类实例
        self.redis_utils = RedisUtils()
        
    def tearDown(self):
        """清理测试环境"""
        # 停止所有patch
        self.env_patcher.stop()
        self.redis_patcher.stop()
        
    def test_connect(self):
        """测试连接Redis"""
        # 验证Redis客户端创建时使用了正确的配置
        self.mock_redis.assert_called_once()
        
        call_args = self.mock_redis.call_args[1]
        self.assertEqual(call_args['host'], "test_host")
        self.assertEqual(call_args['port'], 6379)
        self.assertEqual(call_args['db'], 0)
        self.assertEqual(call_args['password'], "test_password")
        self.assertEqual(call_args['decode_responses'], True)
        
        # 验证ping被调用
        self.mock_redis_instance.ping.assert_called_once()
        
    def test_is_connected(self):
        """测试检查连接状态"""
        # 设置模拟返回值
        self.mock_redis_instance.ping.return_value = True
        
        # 检查连接状态
        result = self.redis_utils.is_connected()
        
        # 验证调用
        self.mock_redis_instance.ping.assert_called()
        
        # 验证结果
        self.assertTrue(result)
        
    def test_is_connected_failure(self):
        """测试连接失败的情况"""
        # 设置模拟抛出异常
        self.mock_redis_instance.ping.side_effect = redis.RedisError("Connection error")
        
        # 重置mock计数，确保之前的调用不会影响测试
        self.mock_redis_instance.ping.reset_mock()
        
        # 检查连接状态
        result = self.redis_utils.is_connected()
        
        # 验证调用
        self.mock_redis_instance.ping.assert_called_once()  # 确保只调用一次
        
        # 验证结果
        self.assertFalse(result)
        
    def test_update_job_status(self):
        """测试更新任务状态"""
        # 设置时间模拟
        current_time = 1234567890.0
        time_patcher = patch('time.time', return_value=current_time)
        time_patcher.start()
        
        # 执行更新
        job_id = "test_job_id"
        status = "processing"
        progress = 50.0
        error = "test error"
        
        result = self.redis_utils.update_job_status(job_id, status, progress, error)
        
        # 验证调用
        self.mock_redis_instance.hset.assert_called_once()
        self.mock_redis_instance.expire.assert_called_once()
        
        # 验证参数
        job_key = f"pdf_job:{job_id}"
        expected_data = {
            "status": status,
            "progress": progress,
            "updated_at": current_time,
            "error": error
        }
        
        # 检查调用参数，根据实际实现 hset(job_key, mapping=job_data)
        hset_args = self.mock_redis_instance.hset.call_args
        # 检查位置参数和关键字参数
        self.assertEqual(hset_args[0][0], job_key)  # 第一个位置参数是job_key
        self.assertEqual(hset_args[1]['mapping'], expected_data)  # 关键字参数mapping是job_data
        
        # 验证过期时间设置
        expire_args = self.mock_redis_instance.expire.call_args[0]
        self.assertEqual(expire_args[0], job_key)
        self.assertEqual(expire_args[1], 86400)  # 1天的秒数
        
        # 验证结果
        self.assertTrue(result)
        
        # 停止时间模拟
        time_patcher.stop()
        
    def test_get_job_status(self):
        """测试获取任务状态"""
        # 设置模拟返回值
        job_data = {
            "status": "completed",
            "progress": "100.0",
            "updated_at": "1234567890.0",
            "error": ""
        }
        self.mock_redis_instance.hgetall.return_value = job_data
        
        # 执行获取
        job_id = "test_job_id"
        result = self.redis_utils.get_job_status(job_id)
        
        # 验证调用
        job_key = f"pdf_job:{job_id}"
        self.mock_redis_instance.hgetall.assert_called_once_with(job_key)
        
        # 验证结果
        self.assertEqual(result, job_data)
        
    def test_get_job_status_not_found(self):
        """测试获取不存在的任务状态"""
        # 设置模拟返回值
        self.mock_redis_instance.hgetall.return_value = {}
        
        # 执行获取
        job_id = "nonexistent_job_id"
        result = self.redis_utils.get_job_status(job_id)
        
        # 验证调用
        job_key = f"pdf_job:{job_id}"
        self.mock_redis_instance.hgetall.assert_called_once_with(job_key)
        
        # 验证结果
        self.assertIsNone(result)
        
    def test_update_job_progress(self):
        """测试更新任务进度"""
        # 设置时间模拟
        current_time = 1234567890.0
        time_patcher = patch('time.time', return_value=current_time)
        time_patcher.start()
        
        # 执行更新
        job_id = "test_job_id"
        progress = 75.5
        
        result = self.redis_utils.update_job_progress(job_id, progress)
        
        # 验证调用
        job_key = f"pdf_job:{job_id}"
        expected_data = {
            "progress": progress,
            "updated_at": current_time
        }
        
        self.mock_redis_instance.hset.assert_called_once_with(
            job_key, 
            mapping=expected_data
        )
        
        # 验证结果
        self.assertTrue(result)
        
        # 停止时间模拟
        time_patcher.stop()
        
    def test_delete_job(self):
        """测试删除任务"""
        # 设置模拟返回值
        self.mock_redis_instance.delete.return_value = 1
        
        # 执行删除
        job_id = "test_job_id"
        result = self.redis_utils.delete_job(job_id)
        
        # 验证调用
        job_key = f"pdf_job:{job_id}"
        self.mock_redis_instance.delete.assert_called_once_with(job_key)
        
        # 验证结果
        self.assertTrue(result)
        
    def test_delete_nonexistent_job(self):
        """测试删除不存在的任务"""
        # 设置模拟返回值
        self.mock_redis_instance.delete.return_value = 0
        
        # 执行删除
        job_id = "nonexistent_job_id"
        result = self.redis_utils.delete_job(job_id)
        
        # 验证调用
        job_key = f"pdf_job:{job_id}"
        self.mock_redis_instance.delete.assert_called_once_with(job_key)
        
        # 验证结果
        self.assertFalse(result)
        
    def test_get_all_jobs(self):
        """测试获取所有任务"""
        # 设置模拟返回值
        keys = ["pdf_job:job1", "pdf_job:job2"]
        self.mock_redis_instance.keys.return_value = keys
        
        # 模拟get_job_status方法
        job1_data = {"status": "completed", "progress": 100.0}
        job2_data = {"status": "processing", "progress": 50.0}
        
        self.redis_utils.get_job_status = MagicMock()
        self.redis_utils.get_job_status.side_effect = lambda job_id: job1_data if job_id == "job1" else job2_data
        
        # 执行获取
        result = self.redis_utils.get_all_jobs()
        
        # 验证调用
        self.mock_redis_instance.keys.assert_called_once_with("pdf_job:*")
        self.assertEqual(self.redis_utils.get_job_status.call_count, 2)
        
        # 验证结果
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["job_id"], "job1")
        self.assertEqual(result[0]["status"], "completed")
        self.assertEqual(result[1]["job_id"], "job2")
        self.assertEqual(result[1]["status"], "processing")
        
    def test_error_handling(self):
        """测试错误处理"""
        # 设置模拟抛出异常
        self.mock_redis_instance.hset.side_effect = redis.RedisError("Test error")
        
        # 执行更新
        result = self.redis_utils.update_job_status("test_job_id", "failed")
        
        # 验证结果
        self.assertFalse(result)
        
    def test_close(self):
        """测试关闭连接"""
        # 执行关闭
        self.redis_utils.close()
        
        # 验证调用
        self.mock_redis_instance.close.assert_called_once()


if __name__ == '__main__':
    unittest.main() 