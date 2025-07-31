import os
import subprocess
import tempfile
from pathlib import Path


class DocHelper:
    image_type_set = {".jpg", ".jpeg", ".png", ".gif", ".bmp"}
    miner_u_supported_type_set = set(image_type_set.union([".pdf"]))
    @staticmethod
    def ensure_bytes_pdf(file_name, doc_bytes):
        base, ext = os.path.splitext(file_name)
        ext = ext.lower()
        if ext in DocHelper.miner_u_supported_type_set:
            return doc_bytes
        elif ext in ('.doc', '.docx'):
            return DocHelper.__convert_word_bytes_to_pdf(doc_bytes, ext)
        elif ext in ('.ppt', '.pptx'):
            return DocHelper.__convert_ppt_to_pdf(doc_bytes, ext)
        else:
            raise Exception(f"不支持的文件扩展名: {ext}")

    @staticmethod
    def __convert_word_bytes_to_pdf(input_bytes: bytes, file_extension: str = "docx") -> bytes:
        """
        将Word文档的bytes数据转换为PDF的bytes数据

        参数:
        input_bytes (bytes): Word文档的二进制数据
        file_extension (str): 文件扩展名 ("doc" 或 "docx")，默认为 "docx"

        返回:
        bytes: 生成的PDF二进制数据

        异常:
        ValueError: 如果转换失败或文件扩展名无效
        RuntimeError: 如果LibreOffice未安装或转换过程失败
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建输入文件路径
            input_filename = f"document{file_extension}"
            input_path = os.path.join(temp_dir, input_filename)

            # 写入输入文件
            with open(input_path, "wb") as input_file:
                input_file.write(input_bytes)

            # 准备转换命令
            command = [
                "libreoffice",
                "--headless",
                "--convert-to", "pdf",
                "--outdir", temp_dir,
                input_path
            ]

            # logger.info(f"开始转换文档 (大小: {len(input_bytes)} 字节)")

            # 执行转换
            try:
                result = subprocess.run(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=60  # 设置60秒超时
                )
            except subprocess.TimeoutExpired:
                # logger.error("转换超时")
                raise RuntimeError("文档转换超时") from None

            # 检查转换结果
            if result.returncode != 0:
                error_msg = f"转换失败 (错误代码 {result.returncode}):\n{result.stderr}"
                # logger.error(error_msg)
                raise RuntimeError(error_msg)

            # 构建输出文件路径
            output_path = os.path.join(temp_dir, f"document.pdf")

            # 检查输出文件是否存在
            if not os.path.exists(output_path):
                # 尝试备用文件名（旧版LibreOffice有时使用不同命名）
                alt_output_path = os.path.join(temp_dir, f"document.{file_extension}.pdf")
                if os.path.exists(alt_output_path):
                    output_path = alt_output_path
                    # logger.warning("使用备用输出文件名")
                else:
                    error_msg = f"PDF文件未生成。临时目录内容: {os.listdir(temp_dir)}"
                    # logger.error(error_msg)
                    raise RuntimeError("PDF文件未生成")

            # 读取PDF内容
            with open(output_path, "rb") as pdf_file:
                pdf_bytes = pdf_file.read()

            # logger.info(f"转换成功! PDF大小: {len(pdf_bytes)} 字节")
            return pdf_bytes

    @staticmethod
    def __convert_ppt_to_pdf(input_bytes: bytes, extension: str) -> bytes:
        """
        将PPT/PPTX文件转换为PDF格式

        参数:
        input_bytes: PPT/PPTX文件的字节内容
        extension: 文件扩展名 ('.ppt' 或 '.pptx')

        返回:
        PDF文件的字节内容

        异常:
        ValueError: 不支持的扩展名
        RuntimeError: 转换失败
        """

        # 创建临时目录
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # 写入输入文件
            input_file = tmp_path / f"input{extension}"
            with open(input_file, 'wb') as f:
                f.write(input_bytes)

            # 设置输出文件路径
            output_file = tmp_path / "output.pdf"

            # 构建转换命令
            command = [
                'soffice',
                '--headless',  # 无GUI模式
                '--convert-to', 'pdf',
                '--outdir', str(tmp_path),
                str(input_file)
            ]

            # 执行转换
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False  # 自行处理错误
            )

            # 检查转换结果
            if result.returncode != 0 or not output_file.exists():
                error_msg = result.stderr.decode('utf-8', errors='ignore') or "Unknown error"
                raise RuntimeError(f"Conversion failed with error: {error_msg}")

            # 读取PDF内容
            with open(output_file, 'rb') as f:
                pdf_bytes = f.read()

        return pdf_bytes

if __name__ == "__main__":
    # 测试代码
    print(DocHelper.miner_u_supported_type_set)