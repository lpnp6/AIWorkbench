"""Storage adapters: file://, minio:// and s3:// readers plus the scheme registry."""

from .file_reader import FileSourceReader
from .minio_reader import MinioSourceReader
from .registry import SourceReaderRegistry
from .s3_reader import S3SourceReader

__all__ = [
    "FileSourceReader",
    "MinioSourceReader",
    "S3SourceReader",
    "SourceReaderRegistry",
]
