"""
测试API端点
"""

import unittest
import json
import uuid
from unittest.mock import patch, MagicMock, AsyncMock

from fastapi.testclient import TestClient
from fastapi import UploadFile

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app, JobStatus, JobInfo

class TestAPI(unittest.TestCase):
    """API端点测试"""

    def setUp(self):
        """设置测试环境"""
        self.client = TestClient(app)
        
        # 模拟工具类
        self.mysql_patcher = patch('app.mysql_utils')
        self.redis_patcher = patch('app.redis_utils')
        self.minio_patcher = patch('app.minio_utils')
        
        self.mock_mysql = self.mysql_patcher.start()
        self.mock_redis = self.redis_patcher.start()
        self.mock_minio = self.minio_patcher.start()
        
        # 模拟后台任务
        self.background_tasks_patcher = patch('app.BackgroundTasks')
        self.mock_background_tasks = self.background_tasks_patcher.start()
        self.mock_background_tasks_instance = MagicMock()
        self.mock_background_tasks.return_value = self.mock_background_tasks_instance
        
        # 模拟UUID
        self.uuid_patcher = patch('uuid.uuid4')
        self.mock_uuid = self.uuid_patcher.start()
        self.mock_uuid.return_value = "test-job-id"
        
        # 清空任务字典
        app.jobs = {}
        
    def tearDown(self):
        """清理测试环境"""
        self.mysql_patcher.stop()
        self.redis_patcher.stop()
        self.minio_patcher.stop()
        self.background_tasks_patcher.stop()
        self.uuid_patcher.stop()
        
    def test_health_check(self):
        """测试健康检查端点"""
        # 设置模拟返回值
        self.mock_mysql.is_connected.return_value = True
        self.mock_redis.is_connected.return_value = True
        self.mock_minio.is_connected.return_value = True
        
        # 发送请求
        response = self.client.get("/health")
        
        # 验证响应
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["components"]["mysql"], True)
        self.assertEqual(data["components"]["redis"], True)
        self.assertEqual(data["components"]["minio"], True)
        
    def test_health_check_degraded(self):
        """测试健康检查端点 - 部分组件不可用"""
        # 设置模拟返回值
        self.mock_mysql.is_connected.return_value = True
        self.mock_redis.is_connected.return_value = False
        self.mock_minio.is_connected.return_value = True
        
        # 发送请求
        response = self.client.get("/health")
        
        # 验证响应
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "degraded")
        self.assertEqual(data["components"]["mysql"], True)
        self.assertEqual(data["components"]["redis"], False)
        self.assertEqual(data["components"]["minio"], True)
        
    @patch('app.process_pdf_background')
    def test_pdf_parse(self, mock_process):
        """测试PDF解析端点"""
        # 模拟文件上传
        test_file_content = b"test pdf content"
        files = {"pdf_file": ("test.pdf", test_file_content, "application/pdf")}
        
        # 发送请求
        response = self.client.post(
            "/pdf_parse",
            files=files,
            data={
                "parse_method": "auto",
                "is_json_md_dump": "true",
                "return_content_list": "true"
            }
        )
        
        # 验证响应
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["job_id"], "test-job-id")
        self.assertEqual(data["status"], JobStatus.PENDING)
        
        # 验证任务添加
        self.mock_background_tasks_instance.add_task.assert_called_once()
        
        # 验证Redis和MySQL调用
        self.mock_redis.update_job_status.assert_called_once()
        self.mock_mysql.save_job.assert_called_once()
        
    def test_get_job_status(self):
        """测试获取任务状态端点"""
        # 创建测试任务
        job_id = "test-job-id"
        app.jobs[job_id] = JobInfo(job_id, "test.pdf")
        app.jobs[job_id].status = JobStatus.PROCESSING
        app.jobs[job_id].progress = 50.0
        
        # 发送请求
        response = self.client.get(f"/pdf_job/{job_id}")
        
        # 验证响应
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["job_id"], job_id)
        self.assertEqual(data["status"], JobStatus.PROCESSING)
        self.assertEqual(data["progress"], 50.0)
        
    def test_get_job_status_not_found(self):
        """测试获取不存在任务状态"""
        # 发送请求
        response = self.client.get("/pdf_job/nonexistent-job")
        
        # 验证响应
        self.assertEqual(response.status_code, 404)
        
    def test_get_job_result(self):
        """测试获取任务结果端点"""
        # 创建测试任务
        job_id = "test-job-id"
        app.jobs[job_id] = JobInfo(job_id, "test.pdf")
        app.jobs[job_id].status = JobStatus.COMPLETED
        app.jobs[job_id].result = {
            "content_list": [{"text": "Test content"}],
            "images": [{"id": 1, "data": "base64data"}]
        }
        
        # 发送请求
        response = self.client.get(f"/pdf_result/{job_id}")
        
        # 验证响应
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["job_id"], job_id)
        self.assertEqual(data["content_list"][0]["text"], "Test content")
        
    def test_get_job_result_not_completed(self):
        """测试获取未完成任务结果"""
        # 创建测试任务
        job_id = "test-job-id"
        app.jobs[job_id] = JobInfo(job_id, "test.pdf")
        app.jobs[job_id].status = JobStatus.PROCESSING
        
        # 发送请求
        response = self.client.get(f"/pdf_result/{job_id}")
        
        # 验证响应
        self.assertEqual(response.status_code, 400)
        
    def test_get_job_content(self):
        """测试获取任务内容列表端点"""
        # 创建测试任务
        job_id = "test-job-id"
        app.jobs[job_id] = JobInfo(job_id, "test.pdf")
        app.jobs[job_id].status = JobStatus.COMPLETED
        app.jobs[job_id].result = {
            "content_list": [{"text": "Test content"}]
        }
        
        # 发送请求
        response = self.client.get(f"/pdf_result/{job_id}/content")
        
        # 验证响应
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["job_id"], job_id)
        self.assertEqual(data["content_list"][0]["text"], "Test content")
        
    def test_get_job_images(self):
        """测试获取任务图像端点"""
        # 创建测试任务
        job_id = "test-job-id"
        app.jobs[job_id] = JobInfo(job_id, "test.pdf")
        app.jobs[job_id].status = JobStatus.COMPLETED
        app.jobs[job_id].result = {
            "images": [{"id": 1, "data": "base64data"}]
        }
        
        # 发送请求
        response = self.client.get(f"/pdf_result/{job_id}/images")
        
        # 验证响应
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["job_id"], job_id)
        self.assertEqual(data["images"][0]["id"], 1)
        
    @patch('app.process_pdf_background')
    def test_pdf_parse_from_minio(self, mock_process):
        """测试从MinIO解析PDF端点"""
        # 设置模拟返回值
        self.mock_minio.get_file_content.return_value = b"test pdf content"
        
        # 发送请求
        response = self.client.post(
            "/pdf_parse_from_minio",
            data={
                "bucket_name": "test-bucket",
                "object_name": "test/test.pdf",
                "parse_method": "auto",
                "is_json_md_dump": "true",
                "return_content_list": "true"
            }
        )
        
        # 验证响应
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["job_id"], "test-job-id")
        self.assertEqual(data["status"], JobStatus.PENDING)
        
        # 验证MinIO调用
        self.mock_minio.get_file_content.assert_called_once()
        
        # 验证任务添加
        self.mock_background_tasks_instance.add_task.assert_called_once()
        
    def test_delete_job(self):
        """测试删除任务端点"""
        # 创建测试任务
        job_id = "test-job-id"
        app.jobs[job_id] = JobInfo(job_id, "test.pdf")
        app.jobs[job_id].output_path = "/tmp/output/test-job-id"
        
        # 发送请求
        response = self.client.delete(f"/pdf_job/{job_id}")
        
        # 验证响应
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["job_id"], job_id)
        self.assertEqual(data["status"], "deleted")
        
        # 验证任务删除
        self.assertNotIn(job_id, app.jobs)
        
        # 验证Redis和MinIO调用
        self.mock_redis.delete_job.assert_called_once()
        self.mock_minio.delete_directory.assert_not_called()  # 因为delete_files=False
        
    def test_delete_job_with_files(self):
        """测试删除任务和文件端点"""
        # 创建测试任务
        job_id = "test-job-id"
        app.jobs[job_id] = JobInfo(job_id, "test.pdf")
        app.jobs[job_id].output_path = "/tmp/output/test-job-id"
        
        # 发送请求
        response = self.client.delete(f"/pdf_job/{job_id}?delete_files=true")
        
        # 验证响应
        self.assertEqual(response.status_code, 200)
        
        # 验证MinIO调用
        self.mock_minio.delete_directory.assert_called_once()
        
    def test_multiple_job_status(self):
        """测试批量获取任务状态端点"""
        # 创建测试任务
        job_id1 = "test-job-id-1"
        job_id2 = "test-job-id-2"
        app.jobs[job_id1] = JobInfo(job_id1, "test1.pdf")
        app.jobs[job_id1].status = JobStatus.COMPLETED
        app.jobs[job_id1].progress = 100.0
        app.jobs[job_id2] = JobInfo(job_id2, "test2.pdf")
        app.jobs[job_id2].status = JobStatus.PROCESSING
        app.jobs[job_id2].progress = 50.0
        
        # 发送请求
        response = self.client.post(
            "/pdf_jobs/status",
            json={"job_ids": [job_id1, job_id2, "nonexistent-job"]}
        )
        
        # 验证响应
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data[job_id1]["status"], JobStatus.COMPLETED)
        self.assertEqual(data[job_id1]["progress"], 100.0)
        self.assertEqual(data[job_id2]["status"], JobStatus.PROCESSING)
        self.assertEqual(data[job_id2]["progress"], 50.0)
        self.assertEqual(data["nonexistent-job"]["status"], "not_found")


if __name__ == '__main__':
    unittest.main() 