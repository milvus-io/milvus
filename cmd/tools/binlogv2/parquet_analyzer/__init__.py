"""
Parquet Analyzer Package
A toolkit for analyzing parquet files, including metadata parsing and vector deserialization functionality
"""

from .meta_parser import ParquetMetaParser
from .vector_deserializer import VectorDeserializer
from .analyzer import ParquetAnalyzer

__version__ = "1.0.0"
__all__ = ["ParquetMetaParser", "VectorDeserializer", "ParquetAnalyzer"] 