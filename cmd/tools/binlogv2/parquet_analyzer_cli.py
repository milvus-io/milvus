#!/usr/bin/env python3
"""
Parquet Analyzer Command Line Tool
Provides a simple command line interface to use the parquet analyzer
"""

import argparse
import sys
import json
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

# Add current directory to Python path
sys.path.append(str(Path(__file__).parent))

from parquet_analyzer import ParquetAnalyzer, ParquetMetaParser, VectorDeserializer


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Parquet Analyzer Command Line Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Usage Examples:
  python parquet_analyzer_cli.py analyze test_large_batch.parquet
  python parquet_analyzer_cli.py metadata test_large_batch.parquet
  python parquet_analyzer_cli.py vector test_large_batch.parquet
  python parquet_analyzer_cli.py export test_large_batch.parquet --output result.json
  python parquet_analyzer_cli.py data test_large_batch.parquet --rows 10 --output data.json
  python parquet_analyzer_cli.py query test_large_batch.parquet --id-value 123
  python parquet_analyzer_cli.py query test_large_batch.parquet --id-value 123 --id-column user_id
        """
    )
    
    parser.add_argument(
        "command",
        choices=["analyze", "metadata", "vector", "export", "data", "query"],
        help="Command to execute"
    )
    
    parser.add_argument(
        "file",
        help="Parquet file path"
    )
    
    parser.add_argument(
        "--output", "-o",
        help="Output file path (for export and data commands)"
    )
    
    parser.add_argument(
        "--rows", "-r",
        type=int,
        default=10,
        help="Number of rows to export (only for data command, default: 10 rows)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    
    # Query-specific arguments
    parser.add_argument(
        "--id-value", "-i",
        help="ID value to query (for query command)"
    )
    
    parser.add_argument(
        "--id-column", "-c",
        help="ID column name (for query command, auto-detected if not specified)"
    )
    
    args = parser.parse_args()
    
    # Check if file exists
    if not Path(args.file).exists():
        print(f"❌ File does not exist: {args.file}")
        sys.exit(1)
    
    if args.command == "analyze":
        analyze_file(args.file, args.verbose)
    elif args.command == "metadata":
        analyze_metadata(args.file, args.verbose)
    elif args.command == "vector":
        analyze_vectors(args.file, args.verbose)
    elif args.command == "export":
        export_analysis(args.file, args.output, args.verbose)
    elif args.command == "data":
        export_data(args.file, args.output, args.rows, args.verbose)
    elif args.command == "query":
        query_by_id(args.file, args.id_value, args.id_column, args.verbose)


def analyze_file(file_path: str, verbose: bool = False):
    """Analyze parquet file"""
    print(f"🔍 Analyzing parquet file: {Path(file_path).name}")
    print("=" * 60)
    
    analyzer = ParquetAnalyzer(file_path)
    
    if not analyzer.load():
        print("❌ Failed to load parquet file")
        sys.exit(1)
    
    # Print summary
    analyzer.print_summary()
    
    if verbose:
        # Detailed analysis
        analysis = analyzer.analyze()
        
        print(f"\n📊 Detailed Analysis Results:")
        print(f"  File Info: {analysis['metadata']['basic_info']['name']}")
        print(f"  Size: {analysis['metadata']['basic_info']['size_mb']:.2f} MB")
        print(f"  Rows: {analysis['metadata']['basic_info']['num_rows']:,}")
        print(f"  Columns: {analysis['metadata']['basic_info']['num_columns']}")
        
        # Display vector analysis
        if analysis['vectors']:
            print(f"\n🔍 Vector Analysis:")
            for vec_analysis in analysis['vectors']:
                col_name = vec_analysis['column_name']
                stat_type = vec_analysis['stat_type']
                analysis_data = vec_analysis['analysis']
                
                print(f"  {col_name} ({stat_type}):")
                print(f"    Vector Type: {analysis_data['vector_type']}")
                print(f"    Dimension: {analysis_data['dimension']}")
                
                if analysis_data['statistics']:
                    stats = analysis_data['statistics']
                    print(f"    Min: {stats.get('min', 'N/A')}")
                    print(f"    Max: {stats.get('max', 'N/A')}")
                    print(f"    Mean: {stats.get('mean', 'N/A')}")
                    print(f"    Std: {stats.get('std', 'N/A')}")


def analyze_metadata(file_path: str, verbose: bool = False):
    """Analyze metadata"""
    print(f"📄 Analyzing metadata: {Path(file_path).name}")
    print("=" * 60)
    
    meta_parser = ParquetMetaParser(file_path)
    
    if not meta_parser.load():
        print("❌ Failed to load parquet file")
        sys.exit(1)
    
    # Basic information
    basic_info = meta_parser.get_basic_info()
    print(f"📊 File Information:")
    print(f"  Name: {basic_info['name']}")
    print(f"  Size: {basic_info['size_mb']:.2f} MB")
    print(f"  Rows: {basic_info['num_rows']:,}")
    print(f"  Columns: {basic_info['num_columns']}")
    print(f"  Row Groups: {basic_info['num_row_groups']}")
    print(f"  Created By: {basic_info['created_by']}")
    print(f"  Parquet Version: {basic_info['format_version']}")
    
    # File-level metadata
    file_metadata = meta_parser.get_file_metadata()
    if file_metadata:
        print(f"\n📄 File-level Metadata:")
        for key, value in file_metadata.items():
            print(f"  {key}: {value}")
    
    # Schema-level metadata
    schema_metadata = meta_parser.get_schema_metadata()
    if schema_metadata:
        print(f"\n📋 Schema-level Metadata:")
        for field in schema_metadata:
            print(f"  {field['column_name']}: {field['column_type']}")
            for k, v in field['metadata'].items():
                print(f"    {k}: {v}")
    
    # Column statistics
    column_stats = meta_parser.get_column_statistics()
    if column_stats:
        print(f"\n📈 Column Statistics:")
        for col_stats in column_stats:
            print(f"  {col_stats['column_name']}:")
            print(f"    Compression: {col_stats['compression']}")
            print(f"    Encodings: {', '.join(col_stats['encodings'])}")
            print(f"    Compressed Size: {col_stats['compressed_size']:,} bytes")
            print(f"    Uncompressed Size: {col_stats['uncompressed_size']:,} bytes")
            
            if 'statistics' in col_stats and col_stats['statistics']:
                stats = col_stats['statistics']
                if 'null_count' in stats:
                    print(f"    Null Count: {stats['null_count']}")
                if 'distinct_count' in stats:
                    print(f"    Distinct Count: {stats['distinct_count']}")
                if 'min' in stats:
                    print(f"    Min: {stats['min']}")
                if 'max' in stats:
                    print(f"    Max: {stats['max']}")


def analyze_vectors(file_path: str, verbose: bool = False):
    """Analyze vector data"""
    print(f"🔍 Analyzing vector data: {Path(file_path).name}")
    print("=" * 60)
    
    analyzer = ParquetAnalyzer(file_path)
    
    if not analyzer.load():
        print("❌ Failed to load parquet file")
        sys.exit(1)
    
    vector_analysis = analyzer.analyze_vectors()
    
    if not vector_analysis:
        print("❌ No vector data found")
        return
    
    print(f"📊 Found {len(vector_analysis)} vector statistics:")
    
    for vec_analysis in vector_analysis:
        col_name = vec_analysis['column_name']
        stat_type = vec_analysis['stat_type']
        analysis = vec_analysis['analysis']
        
        print(f"\n🔍 {col_name} ({stat_type}):")
        print(f"  Vector Type: {analysis['vector_type']}")
        print(f"  Dimension: {analysis['dimension']}")
        
        if analysis['statistics']:
            stats = analysis['statistics']
            print(f"  Min: {stats.get('min', 'N/A')}")
            print(f"  Max: {stats.get('max', 'N/A')}")
            print(f"  Mean: {stats.get('mean', 'N/A')}")
            print(f"  Std: {stats.get('std', 'N/A')}")
            
            if analysis['vector_type'] == "BinaryVector":
                print(f"  Zero Count: {stats.get('zero_count', 'N/A')}")
                print(f"  One Count: {stats.get('one_count', 'N/A')}")
        
        if verbose and analysis['deserialized']:
            print(f"  First 5 elements: {analysis['deserialized'][:5]}")
    
    # Validate consistency
    validation = analyzer.validate_vector_consistency()
    print(f"\n✅ Vector Consistency Validation:")
    print(f"  Total Vectors: {validation['total_vectors']}")
    print(f"  Consistent Columns: {validation['consistent_columns']}")
    print(f"  Inconsistent Columns: {validation['inconsistent_columns']}")


def export_analysis(file_path: str, output_file: str = None, verbose: bool = False):
    """Export analysis results"""
    print(f"💾 Exporting analysis results: {Path(file_path).name}")
    print("=" * 60)
    
    analyzer = ParquetAnalyzer(file_path)
    
    if not analyzer.load():
        print("❌ Failed to load parquet file")
        sys.exit(1)
    
    # Export analysis results
    output_file = analyzer.export_analysis(output_file)
    
    print(f"✅ Analysis results exported to: {output_file}")
    
    if verbose:
        # Show file size
        file_size = Path(output_file).stat().st_size
        print(f"📊 Output file size: {file_size:,} bytes ({file_size/1024:.2f} KB)")
        
        # Show summary
        analysis = analyzer.analyze()
        print(f"📈 Analysis Summary:")
        print(f"  Metadata Count: {analysis['metadata']['metadata_summary']['total_metadata_count']}")
        print(f"  Vector Count: {len(analysis['vectors'])}")


def export_data(file_path: str, output_file: str = None, num_rows: int = 10, verbose: bool = False):
    """Export first N rows of parquet file data"""
    print(f"📊 Exporting data: {Path(file_path).name}")
    print("=" * 60)
    
    try:
        # Read parquet file
        table = pq.read_table(file_path)
        df = table.to_pandas()
        
        # Get first N rows
        data_subset = df.head(num_rows)
        
        # Process vector columns, convert bytes to readable format
        processed_data = []
        for idx, row in data_subset.iterrows():
            row_dict = {}
            for col_name, value in row.items():
                if isinstance(value, bytes):
                    # Try to deserialize as vector
                    try:
                        vec_analysis = VectorDeserializer.deserialize_with_analysis(value, col_name)
                        if vec_analysis and vec_analysis['deserialized']:
                            if vec_analysis['vector_type'] == "JSON":
                                # For JSON, show the actual content
                                row_dict[col_name] = vec_analysis['deserialized']
                            elif vec_analysis['vector_type'] == "Array":
                                # For Array, show the actual content
                                row_dict[col_name] = vec_analysis['deserialized']
                            else:
                                # For vectors, show type and dimension
                                row_dict[col_name] = {
                                    "vector_type": vec_analysis['vector_type'],
                                    "dimension": vec_analysis['dimension'],
                                    "data": vec_analysis['deserialized'][:10],  # Only show first 10 elements
                                    "raw_hex": value.hex()[:50] + "..." if len(value.hex()) > 50 else value.hex()
                                }
                        else:
                            row_dict[col_name] = {
                                "type": "binary",
                                "size": len(value),
                                "hex": value.hex()[:50] + "..." if len(value.hex()) > 50 else value.hex()
                            }
                    except Exception as e:
                        row_dict[col_name] = {
                            "type": "binary",
                            "size": len(value),
                            "hex": value.hex()[:50] + "..." if len(value.hex()) > 50 else value.hex(),
                            "error": str(e)
                        }
                else:
                    row_dict[col_name] = value
            processed_data.append(row_dict)
        
        # Prepare output
        result = {
            "file_info": {
                "name": Path(file_path).name,
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "exported_rows": len(processed_data)
            },
            "columns": list(df.columns),
            "data": processed_data
        }
        
        # Determine output file
        if not output_file:
            output_file = f"{Path(file_path).stem}_data_{num_rows}rows.json"
        
        # Save to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Data exported to: {output_file}")
        print(f"📊 Exported {len(processed_data)} rows (total {len(df)} rows)")
        print(f"📋 Columns: {len(df.columns)}")
        
        if verbose:
            print(f"\n📈 Data Preview:")
            for i, row_data in enumerate(processed_data[:3]):  # Only show first 3 rows preview
                print(f"  Row {i+1}:")
                for col_name, value in row_data.items():
                    if isinstance(value, dict) and 'vector_type' in value:
                        print(f"    {col_name}: {value['vector_type']}({value['dimension']}) - {value['data'][:5]}...")
                    elif isinstance(value, dict) and 'type' in value:
                        print(f"    {col_name}: {value['type']} ({value['size']} bytes)")
                    else:
                        print(f"    {col_name}: {value}")
                print()
        
        return output_file
        
    except Exception as e:
        print(f"❌ Failed to export data: {e}")
        sys.exit(1)


def query_by_id(file_path: str, id_value: str = None, id_column: str = None, verbose: bool = False):
    """Query data by ID value"""
    print(f"🔍 Querying by ID: {Path(file_path).name}")
    print("=" * 60)
    
    analyzer = ParquetAnalyzer(file_path)
    
    if not analyzer.load():
        print("❌ Failed to load parquet file")
        sys.exit(1)
    
    # If no ID value provided, show ID column information
    if id_value is None:
        print("📋 ID Column Information:")
        print("-" * 40)
        
        id_info = analyzer.get_id_column_info()
        
        if "error" in id_info:
            print(f"❌ {id_info['error']}")
            sys.exit(1)
        
        print(f"📊 Total rows: {id_info['total_rows']}")
        print(f"📋 Total columns: {id_info['total_columns']}")
        print(f"🎯 Recommended ID column: {id_info['recommended_id_column']}")
        print()
        
        print("📋 Available ID columns:")
        for col_info in id_info['id_columns']:
            status = "✅" if col_info['is_unique'] else "⚠️"
            print(f"  {status} {col_info['column_name']}")
            print(f"    - Unique: {col_info['is_unique']}")
            print(f"    - Type: {'Integer' if col_info['is_integer'] else 'Numeric' if col_info['is_numeric'] else 'Other'}")
            print(f"    - Range: {col_info['min_value']} to {col_info['max_value']}" if col_info['is_numeric'] else "    - Range: N/A")
            print(f"    - Sample values: {col_info['sample_values'][:3]}")
            print()
        
        print("💡 Usage: python parquet_analyzer_cli.py query <file> --id-value <value> [--id-column <column>]")
        return
    
    # Convert ID value to appropriate type
    try:
        # Try to convert to integer first
        if id_value.isdigit():
            id_value = int(id_value)
        elif id_value.replace('.', '').replace('-', '').isdigit():
            id_value = float(id_value)
    except ValueError:
        # Keep as string if conversion fails
        pass
    
    # Perform the query
    result = analyzer.query_by_id(id_value, id_column)
    
    if "error" in result:
        print(f"❌ Query failed: {result['error']}")
        sys.exit(1)
    
    if not result['found']:
        print(f"❌ {result['message']}")
        return
    
    # Display results
    print(f"✅ Found record with {result['id_column']} = {result['id_value']}")
    print(f"📊 Total columns: {result['total_columns']}")
    print(f"📈 Total rows in file: {result['total_rows']}")
    print()
    
    print("📋 Record Data:")
    print("-" * 40)
    
    for col_name, value in result['record'].items():
        if isinstance(value, dict) and 'vector_type' in value:
            # Vector data
            print(f"  {col_name}:")
            print(f"    Type: {value['vector_type']}")
            print(f"    Dimension: {value['dimension']}")
            print(f"    Data preview: {value['data_preview'][:5]}...")
        elif isinstance(value, dict) and 'name' in value:
            # JSON data (likely a person record)
            print(f"  {col_name}:")
            for key, val in value.items():
                print(f"    {key}: {val}")
        elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], str):
            # String array data
            print(f"  {col_name}: {value}")
        elif isinstance(value, list):
            # Array data
            print(f"  {col_name}: {value}")
        elif isinstance(value, str) and value.startswith('<binary data:'):
            # Binary data
            print(f"  {col_name}: {value}")
        else:
            # Regular data
            print(f"  {col_name}: {value}")
    
    # Show vector analysis if available
    if result['vector_columns']:
        print()
        print("🔍 Vector Analysis:")
        print("-" * 40)
        for vec_info in result['vector_columns']:
            col_name = vec_info['column_name']
            analysis = vec_info['analysis']
            print(f"  {col_name}:")
            print(f"    Type: {analysis['vector_type']}")
            print(f"    Dimension: {analysis['dimension']}")
            if 'statistics' in analysis:
                stats = analysis['statistics']
                print(f"    Statistics: {stats}")
    
    if verbose:
        print()
        print("🔍 Detailed Analysis:")
        print("-" * 40)
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main() 