"""
测试MinIO工具类
"""

import unittest
import os
import io
import uuid
from unittest.mock import patch, MagicMock, mock_open

from minio import Minio
from minio.error import S3Error

from utils.minio_utils import MinioUtils

class TestMinioUtils(unittest.TestCase):
    """MinIO工具类测试"""

    def setUp(self):
        """设置测试环境"""
        # 修改 test_endpoint 为有效的地址，例如 minio:9000
        self.env_patcher = patch.dict('os.environ', {
            'MINIO_ENDPOINT': 'localhost:9000',  # 确保与实际运行环境一致
            'MINIO_ACCESS_KEY': 'rag_flow',
            'MINIO_SECRET_KEY': 'infini_rag_flow',
            'MINIO_SECURE': 'false',
            'MINIO_BUCKET': 'test-bucket',
        })
        self.env_patcher.start()

        # 模拟Minio客户端
        self.minio_patcher = patch('utils.minio_utils.Minio')  # 确保路径正确
        self.mock_minio = self.minio_patcher.start()

        # 创建Minio客户端模拟
        self.mock_minio_instance = MagicMock()
        self.mock_minio.return_value = self.mock_minio_instance

        # 设置bucket_exists返回值
        self.mock_minio_instance.bucket_exists.return_value = True

        # 创建工具类实例
        self.minio_utils = MinioUtils()

    def test_connect(self):
        """测试连接MinIO"""
        # 验证Minio客户端创建时使用了正确的配置
        self.mock_minio.assert_called_once()

        call_args = self.mock_minio.call_args[1]
        self.assertEqual(call_args['endpoint'], "localhost:9000")
        self.assertEqual(call_args['access_key'], "rag_flow")
        self.assertEqual(call_args['secret_key'], "infini_rag_flow")
        self.assertEqual(call_args['secure'], False)

        # 验证bucket_exists被调用
        self.mock_minio_instance.bucket_exists.assert_called_once_with("test-bucket")

    def tearDown(self):
        """清理测试环境"""
        # 停止所有patch
        self.env_patcher.stop()
        self.minio_patcher.stop()

    def test_is_connected(self):
        """测试检查连接状态"""
        # 设置模拟返回值
        self.mock_minio_instance.list_buckets.return_value = [MagicMock()]

        # 检查连接状态
        result = self.minio_utils.is_connected()

        # 验证调用
        self.mock_minio_instance.list_buckets.assert_called_once()

        # 验证结果
        self.assertTrue(result)

    def test_is_connected_failure(self):
        """测试连接失败的情况"""
        # 设置模拟抛出异常
        self.mock_minio_instance.list_buckets.side_effect = S3Error(
            "Connection error",  # code
            "Resource not found",  # message
            "resource",  # resource
            "request_id",  # request_id
            "host_id",  # host_id
            "response"  # response
        )

        # 检查连接状态
        result = self.minio_utils.is_connected()

        # 验证调用
        self.mock_minio_instance.list_buckets.assert_called_once()

        # 验证结果
        self.assertFalse(result)

    def test_ensure_bucket_exists_already_exists(self):
        """测试确保存储桶存在 - 已存在的情况"""
        # 设置模拟返回值
        self.mock_minio_instance.bucket_exists.return_value = True

        # 执行确保存储桶存在
        bucket_name = "test-bucket"
        result = self.minio_utils.ensure_bucket_exists(bucket_name)

        # 验证调用
        self.mock_minio_instance.bucket_exists.assert_called_with(bucket_name)
        self.mock_minio_instance.make_bucket.assert_not_called()

        # 验证结果
        self.assertTrue(result)

    def test_ensure_bucket_exists_create_new(self):
        """测试确保存储桶存在 - 创建新存储桶的情况"""
        # 设置模拟返回值
        self.mock_minio_instance.bucket_exists.return_value = False

        # 执行确保存储桶存在
        bucket_name = "new-bucket"
        result = self.minio_utils.ensure_bucket_exists(bucket_name)

        # 验证调用
        self.mock_minio_instance.bucket_exists.assert_called_with(bucket_name)
        self.mock_minio_instance.make_bucket.assert_called_once_with(bucket_name)

        # 验证结果
        self.assertTrue(result)

    def test_upload_file(self):
        """测试上传文件"""
        # 执行上传
        local_path = "/tmp/test_file.txt"
        object_name = "test/test_file.txt"

        result = self.minio_utils.upload_file(local_path, object_name)

        # 验证调用
        self.mock_minio_instance.fput_object.assert_called_once_with(
            "test-bucket",
            object_name,
            local_path
        )

        # 验证结果
        self.assertEqual(result, object_name)

    def test_upload_file_default_object_name(self):
        """测试上传文件 - 默认对象名称"""
        # 执行上传
        local_path = "/tmp/test_file.txt"

        result = self.minio_utils.upload_file(local_path)

        # 验证调用
        self.mock_minio_instance.fput_object.assert_called_once_with(
            "test-bucket",
            "test_file.txt",
            local_path
        )

        # 验证结果
        self.assertEqual(result, "test_file.txt")

    def test_upload_bytes(self):
        """测试上传字节数据"""
        # 设置模拟返回值
        self.mock_minio_instance.put_object.return_value = None
        
        # 执行上传
        data = b"test data"
        object_name = "test/test_file.txt"
        content_type = "text/plain"
        
        result = self.minio_utils.upload_bytes(data, object_name, content_type)
        
        # 验证调用
        self.mock_minio_instance.put_object.assert_called_once()
        
        # 验证参数
        call_args = self.mock_minio_instance.put_object.call_args[0]
        self.assertEqual(call_args[0], "test-bucket")  # 确保桶名正确
        self.assertEqual(call_args[1], object_name)    # 确保对象名正确
        # 检查文件内容（第三个参数应该是数据流）
        self.assertIsNotNone(call_args[2])
        
        # 检查关键字参数
        call_kwargs = self.mock_minio_instance.put_object.call_args[1]
        self.assertEqual(call_kwargs.get('length'), len(data))  # 确保长度参数正确传递
        self.assertEqual(call_kwargs.get('content_type'), content_type)  # 确保内容类型正确传递
        
        # 验证结果
        self.assertTrue(result)

    @patch('os.walk')
    def test_upload_directory(self, mock_walk):
        """测试上传目录"""
        # 设置模拟返回值
        mock_walk.return_value = [
            ("/tmp/test_dir", [], ["file1.txt", "file2.txt"]),
            ("/tmp/test_dir/subdir", [], ["file3.txt"])
        ]

        # 模拟upload_file方法
        self.minio_utils.upload_file = MagicMock()
        self.minio_utils.upload_file.side_effect = lambda path, name: name

        # 执行上传
        local_dir = "/tmp/test_dir"
        prefix = "test_prefix"

        result = self.minio_utils.upload_directory(local_dir, prefix)

        # 验证调用
        self.assertEqual(self.minio_utils.upload_file.call_count, 3)

        # 验证结果
        self.assertEqual(len(result), 3)

    @patch('os.makedirs')
    def test_download_file(self, mock_makedirs):
        """测试下载文件"""
        # 执行下载
        object_name = "test/test_file.txt"
        local_path = "/tmp/downloaded_file.txt"

        result = self.minio_utils.download_file(object_name, local_path)

        # 验证调用
        mock_makedirs.assert_called_once()
        self.mock_minio_instance.fget_object.assert_called_once_with(
            "test-bucket",
            object_name,
            local_path
        )

        # 验证结果
        self.assertEqual(result, local_path)

    @patch('uuid.uuid4')
    @patch('os.makedirs')
    def test_download_file_default_path(self, mock_makedirs, mock_uuid):
        """测试下载文件 - 默认本地路径"""
        # 设置模拟返回值
        mock_uuid.return_value = "test-uuid"

        # 执行下载
        object_name = "test/test_file.txt"

        result = self.minio_utils.download_file(object_name)

        # 验证调用
        mock_makedirs.assert_called_once()
        self.mock_minio_instance.fget_object.assert_called_once_with(
            "test-bucket",
            object_name,
            "/tmp/test-uuid_test_file.txt"
        )

        # 验证结果
        self.assertEqual(result, "/tmp/test-uuid_test_file.txt")

    def test_get_file_content(self):
        """测试获取文件内容"""
        # 设置模拟返回值
        mock_response = MagicMock()
        mock_response.read.return_value = b"test file content"
        self.mock_minio_instance.get_object.return_value = mock_response

        # 执行获取
        object_name = "test/test_file.txt"

        result = self.minio_utils.get_file_content(object_name)

        # 验证调用
        self.mock_minio_instance.get_object.assert_called_once_with(
            "test-bucket",
            object_name
        )
        mock_response.read.assert_called_once()
        mock_response.close.assert_called_once()
        mock_response.release_conn.assert_called_once()

        # 验证结果
        self.assertEqual(result, b"test file content")

    def test_list_objects(self):
        """测试列出对象"""
        # 设置模拟返回值
        mock_obj1 = MagicMock()
        mock_obj1.object_name = "test/file1.txt"
        mock_obj2 = MagicMock()
        mock_obj2.object_name = "test/file2.txt"

        self.mock_minio_instance.list_objects.return_value = [mock_obj1, mock_obj2]

        # 执行列出
        prefix = "test"

        result = self.minio_utils.list_objects(prefix)

        # 验证调用
        self.mock_minio_instance.list_objects.assert_called_once_with(
            "test-bucket",
            prefix=prefix,
            recursive=True
        )

        # 验证结果
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], "test/file1.txt")
        self.assertEqual(result[1], "test/file2.txt")

    def test_delete_object(self):
        """测试删除对象"""
        # 执行删除
        object_name = "test/test_file.txt"

        result = self.minio_utils.delete_object(object_name)

        # 验证调用
        self.mock_minio_instance.remove_object.assert_called_once_with(
            "test-bucket",
            object_name
        )

        # 验证结果
        self.assertTrue(result)

    def test_delete_directory(self):
        """测试删除目录"""
        # 设置模拟返回值
        mock_obj1 = MagicMock()
        mock_obj1.object_name = "test/file1.txt"
        mock_obj2 = MagicMock()
        mock_obj2.object_name = "test/file2.txt"

        self.mock_minio_instance.list_objects.return_value = [mock_obj1, mock_obj2]
        self.mock_minio_instance.remove_objects.return_value = []

        # 执行删除
        prefix = "test"

        result = self.minio_utils.delete_directory(prefix)

        # 验证调用
        self.mock_minio_instance.list_objects.assert_called_once_with(
            "test-bucket",
            prefix=prefix,
            recursive=True
        )
        self.mock_minio_instance.remove_objects.assert_called_once()

        # 验证结果
        self.assertTrue(result)

    def test_object_exists(self):
        """测试检查对象是否存在"""
        # 执行检查
        object_name = "test/test_file.txt"

        result = self.minio_utils.object_exists(object_name)

        # 验证调用
        self.mock_minio_instance.stat_object.assert_called_once_with(
            "test-bucket",
            object_name
        )

        # 验证结果
        self.assertTrue(result)

    def test_object_not_exists(self):
        """测试检查对象不存在"""
        # 设置模拟抛出异常
        mock_response = MagicMock()
        self.mock_minio_instance.stat_object.side_effect = S3Error(
            code="NoSuchKey",  # code
            message="The specified key does not exist",  # message
            resource="/test-bucket/test/nonexistent_file.txt",  # resource
            request_id="test-request-id",  # request_id
            host_id="test-host-id",  # host_id
            response=mock_response,  # response
            bucket_name="test-bucket",  # bucket_name
            object_name="test/nonexistent_file.txt"  # object_name
        )
        
        # 执行检查
        object_name = "test/nonexistent_file.txt"
        
        result = self.minio_utils.object_exists(object_name)
        
        # 验证调用
        self.mock_minio_instance.stat_object.assert_called_once_with(
            "test-bucket",
            object_name
        )
        
        # 验证结果
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main() 