#!/usr/bin/env python3
"""
Parquet to JSON Export Tool
Specialized for exporting parquet file data to JSON format
"""

import argparse
import json
import sys
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
from parquet_analyzer import VectorDeserializer


def export_parquet_to_json(parquet_file: str, output_file: str = None, 
                          num_rows: int = None, start_row: int = 0,
                          include_vectors: bool = True, 
                          vector_format: str = "deserialized",
                          pretty_print: bool = True):
    """
    Export parquet file to JSON format
    
    Args:
        parquet_file: parquet file path
        output_file: output JSON file path
        num_rows: number of rows to export (None means all)
        start_row: starting row number (0-based)
        include_vectors: whether to include vector data
        vector_format: vector format ("deserialized", "hex", "both")
        pretty_print: whether to pretty print output
    """
    
    print(f"📊 Exporting parquet file: {Path(parquet_file).name}")
    print("=" * 60)
    
    try:
        # Read parquet file
        table = pq.read_table(parquet_file)
        df = table.to_pandas()
        
        total_rows = len(df)
        print(f"📋 File Information:")
        print(f"  Total Rows: {total_rows:,}")
        print(f"  Columns: {len(df.columns)}")
        print(f"  Column Names: {', '.join(df.columns)}")
        
        # Determine export row range
        if num_rows is None:
            end_row = total_rows
            num_rows = total_rows - start_row
        else:
            end_row = min(start_row + num_rows, total_rows)
            num_rows = end_row - start_row
        
        if start_row >= total_rows:
            print(f"❌ Starting row {start_row} exceeds file range (0-{total_rows-1})")
            return False
        
        print(f"📈 Export Range: Row {start_row} to Row {end_row-1} (Total {num_rows} rows)")
        
        # Get data for specified range
        data_subset = df.iloc[start_row:end_row]
        
        # Process data
        processed_data = []
        for idx, row in data_subset.iterrows():
            row_dict = {}
            for col_name, value in row.items():
                if isinstance(value, bytes) and include_vectors:
                    # Process vector columns
                    try:
                        vec_analysis = VectorDeserializer.deserialize_with_analysis(value, col_name)
                        if vec_analysis and vec_analysis['deserialized']:
                            if vector_format == "deserialized":
                                row_dict[col_name] = {
                                    "type": vec_analysis['vector_type'],
                                    "dimension": vec_analysis['dimension'],
                                    "data": vec_analysis['deserialized']
                                }
                            elif vector_format == "hex":
                                row_dict[col_name] = {
                                    "type": vec_analysis['vector_type'],
                                    "dimension": vec_analysis['dimension'],
                                    "hex": value.hex()
                                }
                            elif vector_format == "both":
                                row_dict[col_name] = {
                                    "type": vec_analysis['vector_type'],
                                    "dimension": vec_analysis['dimension'],
                                    "data": vec_analysis['deserialized'],
                                    "hex": value.hex()
                                }
                        else:
                            row_dict[col_name] = {
                                "type": "binary",
                                "size": len(value),
                                "hex": value.hex()
                            }
                    except Exception as e:
                        row_dict[col_name] = {
                            "type": "binary",
                            "size": len(value),
                            "hex": value.hex(),
                            "error": str(e)
                        }
                elif isinstance(value, bytes) and not include_vectors:
                    # When not including vectors, only show basic information
                    row_dict[col_name] = {
                        "type": "binary",
                        "size": len(value),
                        "hex": value.hex()[:50] + "..." if len(value.hex()) > 50 else value.hex()
                    }
                else:
                    row_dict[col_name] = value
            processed_data.append(row_dict)
        
        # Prepare output structure
        result = {
            "export_info": {
                "source_file": Path(parquet_file).name,
                "total_rows": total_rows,
                "exported_rows": len(processed_data),
                "start_row": start_row,
                "end_row": end_row - 1,
                "columns": list(df.columns),
                "vector_format": vector_format if include_vectors else "excluded"
            },
            "data": processed_data
        }
        
        # Determine output file
        if not output_file:
            base_name = Path(parquet_file).stem
            output_file = f"{base_name}_export_{start_row}-{end_row-1}.json"
        
        # Save to file
        with open(output_file, 'w', encoding='utf-8') as f:
            if pretty_print:
                json.dump(result, f, ensure_ascii=False, indent=2)
            else:
                json.dump(result, f, ensure_ascii=False, separators=(',', ':'))
        
        # Output statistics
        file_size = Path(output_file).stat().st_size
        print(f"✅ Export completed!")
        print(f"📁 Output file: {output_file}")
        print(f"📊 File size: {file_size:,} bytes ({file_size/1024:.2f} KB)")
        print(f"📈 Exported rows: {len(processed_data)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Export failed: {e}")
        return False


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Parquet to JSON Export Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Usage Examples:
  python export_to_json.py test_large_batch.parquet
  python export_to_json.py test_large_batch.parquet --rows 100 --output data.json
  python export_to_json.py test_large_batch.parquet --start 1000 --rows 50
  python export_to_json.py test_large_batch.parquet --vector-format hex
        """
    )
    
    parser.add_argument(
        "parquet_file",
        help="Parquet file path"
    )
    
    parser.add_argument(
        "--output", "-o",
        help="Output JSON file path"
    )
    
    parser.add_argument(
        "--rows", "-r",
        type=int,
        help="Number of rows to export (default: all)"
    )
    
    parser.add_argument(
        "--start", "-s",
        type=int,
        default=0,
        help="Starting row number (default: 0)"
    )
    
    parser.add_argument(
        "--no-vectors",
        action="store_true",
        help="Exclude vector data"
    )
    
    parser.add_argument(
        "--vector-format",
        choices=["deserialized", "hex", "both"],
        default="deserialized",
        help="Vector data format (default: deserialized)"
    )
    
    parser.add_argument(
        "--no-pretty",
        action="store_true",
        help="Don't pretty print JSON output (compressed format)"
    )
    
    args = parser.parse_args()
    
    # Check if file exists
    if not Path(args.parquet_file).exists():
        print(f"❌ File does not exist: {args.parquet_file}")
        sys.exit(1)
    
    # Execute export
    success = export_parquet_to_json(
        parquet_file=args.parquet_file,
        output_file=args.output,
        num_rows=args.rows,
        start_row=args.start,
        include_vectors=not args.no_vectors,
        vector_format=args.vector_format,
        pretty_print=not args.no_pretty
    )
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main() 