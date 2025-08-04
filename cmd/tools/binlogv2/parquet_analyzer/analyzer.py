"""
Parquet Analyzer Main Component
Main analyzer that integrates metadata parsing and vector deserialization functionality
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional

from .meta_parser import ParquetMetaParser
from .vector_deserializer import VectorDeserializer


class ParquetAnalyzer:
    """Main Parquet file analyzer class"""
    
    def __init__(self, file_path: str):
        """
        Initialize analyzer
        
        Args:
            file_path: parquet file path
        """
        self.file_path = Path(file_path)
        self.meta_parser = ParquetMetaParser(file_path)
        self.vector_deserializer = VectorDeserializer()
        
    def load(self) -> bool:
        """
        Load parquet file
        
        Returns:
            bool: whether loading was successful
        """
        return self.meta_parser.load()
    
    def analyze_metadata(self) -> Dict[str, Any]:
        """
        Analyze metadata information
        
        Returns:
            Dict: metadata analysis results
        """
        if not self.meta_parser.metadata:
            return {}
        
        return {
            "basic_info": self.meta_parser.get_basic_info(),
            "file_metadata": self.meta_parser.get_file_metadata(),
            "schema_metadata": self.meta_parser.get_schema_metadata(),
            "column_statistics": self.meta_parser.get_column_statistics(),
            "row_group_info": self.meta_parser.get_row_group_info(),
            "metadata_summary": self.meta_parser.get_metadata_summary()
        }
    
    def analyze_vectors(self) -> List[Dict[str, Any]]:
        """
        Analyze vector data
        
        Returns:
            List: vector analysis results list
        """
        if not self.meta_parser.metadata:
            return []
        
        vector_analysis = []
        column_stats = self.meta_parser.get_column_statistics()
        
        for col_stats in column_stats:
            if "statistics" in col_stats and col_stats["statistics"]:
                stats = col_stats["statistics"]
                col_name = col_stats["column_name"]
                
                # Check if there's binary data (vector)
                if "min" in stats:
                    min_value = stats["min"]
                    if isinstance(min_value, bytes):
                        min_analysis = VectorDeserializer.deserialize_with_analysis(
                            min_value, col_name
                        )
                        if min_analysis:
                            vector_analysis.append({
                                "column_name": col_name,
                                "stat_type": "min",
                                "analysis": min_analysis
                            })
                    elif isinstance(min_value, str) and len(min_value) > 32:
                        # May be hex string, try to convert back to bytes
                        try:
                            min_bytes = bytes.fromhex(min_value)
                            min_analysis = VectorDeserializer.deserialize_with_analysis(
                                min_bytes, col_name
                            )
                            if min_analysis:
                                vector_analysis.append({
                                    "column_name": col_name,
                                    "stat_type": "min",
                                    "analysis": min_analysis
                                })
                        except ValueError:
                            pass
                
                if "max" in stats:
                    max_value = stats["max"]
                    if isinstance(max_value, bytes):
                        max_analysis = VectorDeserializer.deserialize_with_analysis(
                            max_value, col_name
                        )
                        if max_analysis:
                            vector_analysis.append({
                                "column_name": col_name,
                                "stat_type": "max",
                                "analysis": max_analysis
                            })
                    elif isinstance(max_value, str) and len(max_value) > 32:
                        # May be hex string, try to convert back to bytes
                        try:
                            max_bytes = bytes.fromhex(max_value)
                            max_analysis = VectorDeserializer.deserialize_with_analysis(
                                max_bytes, col_name
                            )
                            if max_analysis:
                                vector_analysis.append({
                                    "column_name": col_name,
                                    "stat_type": "max",
                                    "analysis": max_analysis
                                })
                        except ValueError:
                            pass
        
        return vector_analysis
    
    def analyze(self) -> Dict[str, Any]:
        """
        Complete parquet file analysis
        
        Returns:
            Dict: complete analysis results
        """
        if not self.load():
            return {}
        
        return {
            "metadata": self.analyze_metadata(),
            "vectors": self.analyze_vectors()
        }
    
    def export_analysis(self, output_file: Optional[str] = None) -> str:
        """
        Export analysis results
        
        Args:
            output_file: output file path, if None will auto-generate
            
        Returns:
            str: output file path
        """
        if output_file is None:
            output_file = f"{self.file_path.stem}_analysis.json"
        
        analysis_result = self.analyze()
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(analysis_result, f, indent=2, ensure_ascii=False)
        
        return output_file
    
    def print_summary(self):
        """Print analysis summary"""
        if not self.meta_parser.metadata:
            print("❌ No parquet file loaded")
            return
        
        # Print metadata summary
        self.meta_parser.print_summary()
        
        # Print vector analysis summary
        vector_analysis = self.analyze_vectors()
        if vector_analysis:
            print(f"\n🔍 Vector Analysis Summary:")
            print("=" * 60)
            for vec_analysis in vector_analysis:
                col_name = vec_analysis["column_name"]
                stat_type = vec_analysis["stat_type"]
                analysis = vec_analysis["analysis"]
                
                print(f"  Column: {col_name} ({stat_type})")
                print(f"    Vector Type: {analysis['vector_type']}")
                print(f"    Dimension: {analysis['dimension']}")
                
                if "statistics" in analysis and analysis["statistics"]:
                    stats = analysis["statistics"]
                    print(f"    Min: {stats.get('min', 'N/A')}")
                    print(f"    Max: {stats.get('max', 'N/A')}")
                    print(f"    Mean: {stats.get('mean', 'N/A')}")
                    print(f"    Std: {stats.get('std', 'N/A')}")
                
                if analysis["vector_type"] == "BinaryVector" and "statistics" in analysis:
                    stats = analysis["statistics"]
                    print(f"    Zero Count: {stats.get('zero_count', 'N/A')}")
                    print(f"    One Count: {stats.get('one_count', 'N/A')}")
                
                print()
    
    def get_vector_samples(self, column_name: str, sample_count: int = 5) -> List[Dict[str, Any]]:
        """
        Get vector sample data
        
        Args:
            column_name: column name
            sample_count: number of samples
            
        Returns:
            List: vector sample list
        """
        # This can be extended to read samples from actual data
        # Currently returns min/max from statistics as samples
        vector_analysis = self.analyze_vectors()
        samples = []
        
        for vec_analysis in vector_analysis:
            if vec_analysis["column_name"] == column_name:
                analysis = vec_analysis["analysis"]
                samples.append({
                    "type": vec_analysis["stat_type"],
                    "vector_type": analysis["vector_type"],
                    "dimension": analysis["dimension"],
                    "data": analysis["deserialized"][:sample_count] if analysis["deserialized"] else [],
                    "statistics": analysis.get("statistics", {})
                })
        
        return samples
    
    def compare_vectors(self, column_name: str) -> Dict[str, Any]:
        """
        Compare different vector statistics for the same column
        
        Args:
            column_name: column name
            
        Returns:
            Dict: comparison results
        """
        vector_analysis = self.analyze_vectors()
        column_vectors = [v for v in vector_analysis if v["column_name"] == column_name]
        
        if len(column_vectors) < 2:
            return {}
        
        comparison = {
            "column_name": column_name,
            "vector_count": len(column_vectors),
            "comparison": {}
        }
        
        for vec_analysis in column_vectors:
            stat_type = vec_analysis["stat_type"]
            analysis = vec_analysis["analysis"]
            
            comparison["comparison"][stat_type] = {
                "vector_type": analysis["vector_type"],
                "dimension": analysis["dimension"],
                "statistics": analysis.get("statistics", {})
            }
        
        return comparison
    
    def validate_vector_consistency(self) -> Dict[str, Any]:
        """
        Validate vector data consistency
        
        Returns:
            Dict: validation results
        """
        vector_analysis = self.analyze_vectors()
        validation_result = {
            "total_vectors": len(vector_analysis),
            "consistent_columns": [],
            "inconsistent_columns": [],
            "details": {}
        }
        
        # Group by column
        columns = {}
        for vec_analysis in vector_analysis:
            col_name = vec_analysis["column_name"]
            if col_name not in columns:
                columns[col_name] = []
            columns[col_name].append(vec_analysis)
        
        for col_name, vec_list in columns.items():
            if len(vec_list) >= 2:
                # Check if vector types are consistent for the same column
                vector_types = set(v["analysis"]["vector_type"] for v in vec_list)
                dimensions = set(v["analysis"]["dimension"] for v in vec_list)
                
                is_consistent = len(vector_types) == 1 and len(dimensions) == 1
                
                validation_result["details"][col_name] = {
                    "vector_types": list(vector_types),
                    "dimensions": list(dimensions),
                    "is_consistent": is_consistent,
                    "vector_count": len(vec_list)
                }
                
                if is_consistent:
                    validation_result["consistent_columns"].append(col_name)
                else:
                    validation_result["inconsistent_columns"].append(col_name)
        
        return validation_result
    
    def query_by_id(self, id_value: Any, id_column: str = None) -> Dict[str, Any]:
        """
        Query data by ID value
        
        Args:
            id_value: ID value to search for
            id_column: ID column name (if None, will try to find primary key column)
            
        Returns:
            Dict: query results
        """
        try:
            import pandas as pd
            import pyarrow.parquet as pq
        except ImportError:
            return {"error": "pandas and pyarrow are required for ID query"}
        
        if not self.meta_parser.metadata:
            return {"error": "Parquet file not loaded"}
        
        try:
            # Read the parquet file
            df = pd.read_parquet(self.file_path)
            
            # If no ID column specified, try to find primary key column
            if id_column is None:
                # Common primary key column names
                pk_candidates = ['id', 'ID', 'Id', 'pk', 'PK', 'primary_key', 'row_id', 'RowID']
                for candidate in pk_candidates:
                    if candidate in df.columns:
                        id_column = candidate
                        break
                
                if id_column is None:
                    # If no common PK found, use the first column
                    id_column = df.columns[0]
            
            if id_column not in df.columns:
                return {
                    "error": f"ID column '{id_column}' not found in the data",
                    "available_columns": list(df.columns)
                }
            
            # Query by ID
            result = df[df[id_column] == id_value]
            
            if result.empty:
                return {
                    "found": False,
                    "id_column": id_column,
                    "id_value": id_value,
                    "message": f"No record found with {id_column} = {id_value}"
                }
            
            # Convert to dict for JSON serialization
            record = result.iloc[0].to_dict()
            
            # Handle vector columns if present
            vector_columns = []
            for col_name, value in record.items():
                if isinstance(value, bytes) and len(value) > 32:
                    # This might be a vector, try to deserialize
                    try:
                        vector_analysis = VectorDeserializer.deserialize_with_analysis(value, col_name)
                        if vector_analysis:
                            vector_columns.append({
                                "column_name": col_name,
                                "analysis": vector_analysis
                            })
                            # Replace bytes with analysis summary
                            if vector_analysis["vector_type"] == "JSON":
                                # For JSON, show the actual content
                                record[col_name] = vector_analysis["deserialized"]
                            elif vector_analysis["vector_type"] == "Array":
                                # For Array, show the actual content
                                record[col_name] = vector_analysis["deserialized"]
                            else:
                                # For vectors, show type and dimension
                                record[col_name] = {
                                    "vector_type": vector_analysis["vector_type"],
                                    "dimension": vector_analysis["dimension"],
                                    "data_preview": vector_analysis["deserialized"][:5] if vector_analysis["deserialized"] else []
                                }
                    except Exception:
                        # If deserialization fails, keep as bytes but truncate for display
                        record[col_name] = f"<binary data: {len(value)} bytes>"
            
            return {
                "found": True,
                "id_column": id_column,
                "id_value": id_value,
                "record": record,
                "vector_columns": vector_columns,
                "total_columns": len(df.columns),
                "total_rows": len(df)
            }
            
        except Exception as e:
            return {"error": f"Query failed: {str(e)}"}
    
    def get_id_column_info(self) -> Dict[str, Any]:
        """
        Get information about ID columns in the data
        
        Returns:
            Dict: ID column information
        """
        try:
            import pandas as pd
        except ImportError:
            return {"error": "pandas is required for ID column analysis"}
        
        if not self.meta_parser.metadata:
            return {"error": "Parquet file not loaded"}
        
        try:
            df = pd.read_parquet(self.file_path)
            
            # Find potential ID columns
            id_columns = []
            for col in df.columns:
                col_data = df[col]
                
                # Check if column looks like an ID column
                is_unique = col_data.nunique() == len(col_data)
                is_numeric = pd.api.types.is_numeric_dtype(col_data)
                is_integer = pd.api.types.is_integer_dtype(col_data)
                
                id_columns.append({
                    "column_name": col,
                    "is_unique": is_unique,
                    "is_numeric": is_numeric,
                    "is_integer": is_integer,
                    "unique_count": col_data.nunique(),
                    "total_count": len(col_data),
                    "min_value": col_data.min() if is_numeric else None,
                    "max_value": col_data.max() if is_numeric else None,
                    "sample_values": col_data.head(5).tolist()
                })
            
            return {
                "total_columns": len(df.columns),
                "total_rows": len(df),
                "id_columns": id_columns,
                "recommended_id_column": self._get_recommended_id_column(id_columns)
            }
            
        except Exception as e:
            return {"error": f"ID column analysis failed: {str(e)}"}
    
    def _get_recommended_id_column(self, id_columns: List[Dict[str, Any]]) -> str:
        """
        Get recommended ID column based on heuristics
        
        Args:
            id_columns: List of ID column information
            
        Returns:
            str: Recommended ID column name
        """
        # Priority order for ID columns
        priority_names = ['id', 'ID', 'Id', 'pk', 'PK', 'primary_key', 'row_id', 'RowID']
        
        # First, look for columns with priority names that are unique
        for priority_name in priority_names:
            for col_info in id_columns:
                if (col_info["column_name"].lower() == priority_name.lower() and 
                    col_info["is_unique"]):
                    return col_info["column_name"]
        
        # Then, look for any unique integer column
        for col_info in id_columns:
            if col_info["is_unique"] and col_info["is_integer"]:
                return col_info["column_name"]
        
        # Finally, look for any unique column
        for col_info in id_columns:
            if col_info["is_unique"]:
                return col_info["column_name"]
        
        # If no unique column found, return the first column
        return id_columns[0]["column_name"] if id_columns else "" 