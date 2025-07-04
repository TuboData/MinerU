from io import BytesIO
from loguru import logger

from minio import Minio

from utils import MinioUtils
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.config.parser import ConfigParser

minio_utils = MinioUtils()
logger.add("logs/marker-doc-{time}.log")

class MarkerDoc(object):
    @staticmethod
    def handle(
            bucket_name: str,
            folder_path: str,
            minio_url: str,
            minio_access_key: str,
            minio_secret_key: str) -> str:
        logger.info(f"Started: pdf_id: {folder_path}, minio_url: {minio_url}, minio_access_key: {minio_access_key}")
        minio = Minio(minio_url, access_key=minio_access_key, secret_key=minio_secret_key, secure=False)
        minio_utils.ensure_bucket_exists(minio, bucket_name)
        pdf_path = f"{folder_path}/src.pdf"
        pdf_bytes = minio_utils.get_file_content(minio, bucket_name, pdf_path)
        result_md = MarkerDoc._doHandle(BytesIO(pdf_bytes))
        logger.info(f"{folder_path} marker finished: markdown length: {len(result_md)}")
        md_path = f"{folder_path}/marker.md"
        try:
            result_md_bytes = result_md.encode("utf-8")
            minio.put_object(bucket_name, md_path, BytesIO(result_md_bytes), len(result_md_bytes))
            logger.info(f"{folder_path} stored to {md_path}")
        except Exception as ex:
            logger.error(f"Failed store {folder_path} to  {bucket_name}/{md_path}", ex)

        return result_md

    @staticmethod
    def _doHandle(pdf_bytes: BytesIO):
        config = {
            "output_format": "markdown",
            "disable_image_extraction": True
        }
        config_parser = ConfigParser(config)

        converter = PdfConverter(
            config=config_parser.generate_config_dict(),
            artifact_dict=create_model_dict(),
            processor_list=config_parser.get_processors(),
            renderer=config_parser.get_renderer(),
            llm_service=config_parser.get_llm_service()
        )
        return converter(pdf_bytes).markdown
