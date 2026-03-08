"""
Parquet Metadata Parser Component
Responsible for parsing parquet file metadata information
"""

import pyarrow.parquet as pq
import json
from pathlib import Path
from typing import Dict, List, Any, Optional


class ParquetMetaParser:
    """Parquet file Metadata parser"""
    
    def __init__(self, file_path: str):
        """
        Initialize parser
        
        Args:
            file_path: parquet file path
        """
        self.file_path = Path(file_path)
        self.parquet_file = None
        self.metadata = None
        self.schema = None
        
    def load(self) -> bool:
        """
        Load parquet file
        
        Returns:
            bool: whether loading was successful
        """
        try:
            self.parquet_file = pq.ParquetFile(self.file_path)
            self.metadata = self.parquet_file.metadata
            self.schema = self.parquet_file.schema_arrow
            return True
        except Exception as e:
            print(f"❌ Failed to load parquet file: {e}")
            return False
    
    def get_basic_info(self) -> Dict[str, Any]:
        """
        Get basic information
        
        Returns:
            Dict: dictionary containing basic file information
        """
        if not self.metadata:
            return {}
        
        file_size = self.file_path.stat().st_size
        return {
            "name": self.file_path.name,
            "size_bytes": file_size,
            "size_mb": file_size / 1024 / 1024,
            "num_rows": self.metadata.num_rows,
            "num_columns": self.metadata.num_columns,
            "num_row_groups": self.metadata.num_row_groups,
            "created_by": self.metadata.created_by,
            "format_version": self.metadata.format_version,
            "footer_size_bytes": self.metadata.serialized_size
        }
    
    def get_file_metadata(self) -> Dict[str, str]:
        """
        Get file-level metadata
        
        Returns:
            Dict: file-level metadata dictionary
        """
        if not self.metadata or not self.metadata.metadata:
            return {}
        
        result = {}
        for key, value in self.metadata.metadata.items():
            key_str = key.decode() if isinstance(key, bytes) else key
            value_str = value.decode() if isinstance(value, bytes) else value
            result[key_str] = value_str
        
        return result
    
    def get_schema_metadata(self) -> List[Dict[str, Any]]:
        """
        Get Schema-level metadata
        
        Returns:
            List: Schema-level metadata list
        """
        if not self.schema:
            return []
        
        result = []
        for i, field in enumerate(self.schema):
            if field.metadata:
                field_metadata = {}
                for k, v in field.metadata.items():
                    k_str = k.decode() if isinstance(k, bytes) else k
                    v_str = v.decode() if isinstance(v, bytes) else v
                    field_metadata[k_str] = v_str
                
                result.append({
                    "column_index": i,
                    "column_name": field.name,
                    "column_type": str(field.type),
                    "metadata": field_metadata
                })
        
        return result
    
    def get_column_statistics(self) -> List[Dict[str, Any]]:
        """
        Get column statistics
        
        Returns:
            List: column statistics list
        """
        if not self.metadata:
            return []
        
        result = []
        for i in range(self.metadata.num_columns):
            col_meta = self.metadata.row_group(0).column(i)
            col_stats = {
                "column_name": col_meta.path_in_schema,
                "compression": str(col_meta.compression),
                "encodings": [str(enc) for enc in col_meta.encodings],
                "file_offset": col_meta.file_offset,
                "compressed_size": col_meta.total_compressed_size,
                "uncompressed_size": col_meta.total_uncompressed_size
            }
            
            if col_meta.statistics:
                stats = col_meta.statistics
                col_stats["statistics"] = {}
                if hasattr(stats, 'null_count') and stats.null_count is not None:
                    col_stats["statistics"]["null_count"] = stats.null_count
                if hasattr(stats, 'distinct_count') and stats.distinct_count is not None:
                    col_stats["statistics"]["distinct_count"] = stats.distinct_count
                if hasattr(stats, 'min') and stats.min is not None:
                    if isinstance(stats.min, bytes):
                        col_stats["statistics"]["min"] = stats.min.hex()
                    else:
                        col_stats["statistics"]["min"] = stats.min
                if hasattr(stats, 'max') and stats.max is not None:
                    if isinstance(stats.max, bytes):
                        col_stats["statistics"]["max"] = stats.max.hex()
                    else:
                        col_stats["statistics"]["max"] = stats.max
            
            result.append(col_stats)
        
        return result
    
    def get_row_group_info(self) -> List[Dict[str, Any]]:
        """
        Get row group information
        
        Returns:
            List: row group information list
        """
        if not self.metadata:
            return []
        
        result = []
        for i in range(self.metadata.num_row_groups):
            row_group = self.metadata.row_group(i)
            result.append({
                "rowgroup_index": i,
                "num_rows": row_group.num_rows,
                "total_byte_size": row_group.total_byte_size,
                "num_columns": row_group.num_columns
            })
        
        return result
    
    def parse_row_group_metadata(self, metadata_str: str) -> List[Dict[str, Any]]:
        """
        Parse row group metadata string
        
        Args:
            metadata_str: metadata string in format "bytes|rows|offset;bytes|rows|offset;..."
            
        Returns:
            List: parsed row group metadata
        """
        if not metadata_str:
            return []
        
        result = []
        groups = metadata_str.split(';')
        
        for i, group in enumerate(groups):
            if not group.strip():
                continue
                
            parts = group.split('|')
            if len(parts) >= 3:
                try:
                    bytes_size = int(parts[0])
                    rows = int(parts[1])
                    offset = int(parts[2])
                    
                    result.append({
                        "rowgroup_index": i,
                        "bytes": bytes_size,
                        "rows": rows,
                        "offset": offset
                    })
                except (ValueError, IndexError):
                    print(f"⚠️  Warning: Invalid row group metadata format: {group}")
                    continue
        
        return result
    
    def format_row_group_metadata(self, metadata_str: str) -> str:
        """
        Format row group metadata string to readable format
        
        Args:
            metadata_str: metadata string in format "bytes|rows|offset;bytes|rows|offset;..."
            
        Returns:
            str: formatted string
        """
        parsed = self.parse_row_group_metadata(metadata_str)
        
        if not parsed:
            return "No valid row group metadata found"
        
        lines = []
        for group in parsed:
            lines.append(f"row group {group['rowgroup_index']}: {group['bytes']} bytes, {group['rows']} rows, {group['offset']} offset")
        
        return '\n'.join(lines)
    
    def parse_group_field_id_list(self, field_list_str: str) -> List[Dict[str, Any]]:
        """
        Parse group field ID list string
        
        Args:
            field_list_str: field list string in format "field_ids;field_ids;..."
            
        Returns:
            List: parsed field group metadata
        """
        if not field_list_str:
            return []
        
        result = []
        groups = field_list_str.split(';')
        
        for i, group in enumerate(groups):
            if not group.strip():
                continue
                
            try:
                field_ids = [int(fid.strip()) for fid in group.split(',') if fid.strip()]
                
                result.append({
                    "group_index": i,
                    "field_ids": field_ids,
                    "field_count": len(field_ids)
                })
            except (ValueError, IndexError):
                print(f"⚠️  Warning: Invalid field group format: {group}")
                continue
        
        return result
    
    def format_group_field_id_list(self, field_list_str: str) -> str:
        """
        Format group field ID list string to readable format
        
        Args:
            field_list_str: field list string in format "field_ids;field_ids;..."
            
        Returns:
            str: formatted string
        """
        parsed = self.parse_group_field_id_list(field_list_str)
        
        if not parsed:
            return "No valid field group metadata found"
        
        lines = []
        for group in parsed:
            field_ids_str = ','.join(map(str, group['field_ids']))
            lines.append(f"column group {group['group_index']}: {field_ids_str}")
        
        return '\n'.join(lines)
    
    def parse_custom_metadata(self, metadata_dict: Dict[str, str]) -> Dict[str, Any]:
        """
        Parse custom metadata and format special fields
        
        Args:
            metadata_dict: metadata dictionary
            
        Returns:
            Dict: parsed metadata with formatted special fields
        """
        result = {
            "raw_metadata": metadata_dict,
            "formatted_metadata": {}
        }
        
        for key, value in metadata_dict.items():
            if key == "row_group_metadata":
                result["formatted_metadata"]["row_group_metadata"] = {
                    "raw": value,
                    "parsed": self.parse_row_group_metadata(value),
                    "formatted": self.format_row_group_metadata(value)
                }
            elif key == "group_field_id_list":
                result["formatted_metadata"]["group_field_id_list"] = {
                    "raw": value,
                    "parsed": self.parse_group_field_id_list(value),
                    "formatted": self.format_group_field_id_list(value)
                }
            else:
                result["formatted_metadata"][key] = value
        
        return result
    
    def get_metadata_summary(self) -> Dict[str, Any]:
        """
        Get metadata summary information
        
        Returns:
            Dict: metadata summary
        """
        file_metadata = self.get_file_metadata()
        schema_metadata = self.get_schema_metadata()
        
        summary = {
            "file_metadata_count": len(file_metadata),
            "schema_metadata_count": len(schema_metadata),
            "total_metadata_count": len(file_metadata) + len(schema_metadata)
        }
        
        return summary
    
    def export_metadata(self, output_file: Optional[str] = None) -> str:
        """
        Export metadata to JSON file
        
        Args:
            output_file: output file path, if None will auto-generate
            
        Returns:
            str: output file path
        """
        if output_file is None:
            output_file = f"{self.file_path.stem}_metadata.json"
        
        file_metadata = self.get_file_metadata()
        parsed_metadata = self.parse_custom_metadata(file_metadata)
        
        metadata_export = {
            "file_info": self.get_basic_info(),
            "file_metadata": file_metadata,
            "parsed_metadata": parsed_metadata,
            "schema_metadata": self.get_schema_metadata(),
            "column_statistics": self.get_column_statistics(),
            "row_group_info": self.get_row_group_info(),
            "metadata_summary": self.get_metadata_summary()
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(metadata_export, f, indent=2, ensure_ascii=False)
        
        return output_file
    
    def print_summary(self):
        """Print metadata summary"""
        if not self.metadata:
            print("❌ No parquet file loaded")
            return
        
        basic_info = self.get_basic_info()
        summary = self.get_metadata_summary()
        file_metadata = self.get_file_metadata()
        
        print(f"📊 Parquet File Metadata Summary: {basic_info['name']}")
        print("=" * 60)
        print(f"  File Size: {basic_info['size_mb']:.2f} MB")
        print(f"  Total Rows: {basic_info['num_rows']:,}")
        print(f"  Columns: {basic_info['num_columns']}")
        print(f"  Row Groups: {basic_info['num_row_groups']}")
        print(f"  Created By: {basic_info['created_by']}")
        print(f"  Parquet Version: {basic_info['format_version']}")
        print(f"  Footer Size: {basic_info['footer_size_bytes']:,} bytes")
        print(f"  File-level Metadata: {summary['file_metadata_count']} items")
        print(f"  Schema-level Metadata: {summary['schema_metadata_count']} items")
        print(f"  Total Metadata: {summary['total_metadata_count']} items")
        
        # Print formatted custom metadata
        if file_metadata:
            parsed_metadata = self.parse_custom_metadata(file_metadata)
            formatted = parsed_metadata.get("formatted_metadata", {})
            
            if "row_group_metadata" in formatted:
                print("\n📋 Row Group Metadata:")
                print("-" * 30)
                print(formatted["row_group_metadata"]["formatted"])
            
            if "group_field_id_list" in formatted:
                print("\n📋 Column Group Metadata:")
                print("-" * 30)
                print(formatted["group_field_id_list"]["formatted"])
    
    def print_formatted_metadata(self, metadata_key: str = None):
        """
        Print formatted metadata for specific keys
        
        Args:
            metadata_key: specific metadata key to format, if None prints all
        """
        if not self.metadata:
            print("❌ No parquet file loaded")
            return
        
        file_metadata = self.get_file_metadata()
        if not file_metadata:
            print("❌ No file metadata found")
            return
        
        parsed_metadata = self.parse_custom_metadata(file_metadata)
        formatted = parsed_metadata.get("formatted_metadata", {})
        
        if metadata_key:
            if metadata_key in formatted:
                print(f"\n📋 {metadata_key.upper()} Metadata:")
                print("-" * 50)
                print(formatted[metadata_key]["formatted"])
            else:
                print(f"❌ Metadata key '{metadata_key}' not found")
        else:
            # Print all formatted metadata
            for key, value in formatted.items():
                if isinstance(value, dict) and "formatted" in value:
                    print(f"\n📋 {key.upper()} Metadata:")
                    print("-" * 50)
                    print(value["formatted"])
                else:
                    print(f"\n📋 {key.upper()}: {value}") 