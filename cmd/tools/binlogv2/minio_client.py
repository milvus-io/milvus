#!/usr/bin/env python3
"""
MinIO Client for Parquet Analysis
Downloads files from MinIO and passes them to parquet_analyzer_cli.py for analysis
"""

import argparse
import sys
import os
import tempfile
import subprocess
from pathlib import Path
from typing import Optional, List, Dict, Any
import json

try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    print("❌ MinIO client library not found. Please install it:")
    print("   pip install minio")
    sys.exit(1)


class MinioParquetAnalyzer:
    """MinIO client for downloading and analyzing Parquet files and Milvus binlog files"""
    
    def __init__(self, endpoint: str, port: int = 9001, secure: bool = False, 
                 access_key: str = None, secret_key: str = None):
        """
        Initialize MinIO client
        
        Args:
            endpoint: MinIO server endpoint (hostname/IP)
            port: MinIO server port (default: 9000)
            secure: Use HTTPS (default: False)
            access_key: MinIO access key (optional, for public buckets can be None)
            secret_key: MinIO secret key (optional, for public buckets can be None)
        """
        self.endpoint = endpoint
        self.port = port
        self.secure = secure
        self.access_key = access_key
        self.secret_key = secret_key
        
        # Initialize MinIO client
        try:
            self.client = Minio(
                f"{endpoint}:{port}",
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
            print(f"✅ Connected to MinIO server: {endpoint}:{port}")
        except Exception as e:
            print(f"❌ Failed to connect to MinIO server: {e}")
            sys.exit(1)
    
    def list_buckets(self) -> List[Dict[str, Any]]:
        """List all buckets"""
        try:
            buckets = []
            for bucket in self.client.list_buckets():
                buckets.append({
                    'name': bucket.name,
                    'creation_date': bucket.creation_date.isoformat() if bucket.creation_date else None
                })
            return buckets
        except S3Error as e:
            print(f"❌ Failed to list buckets: {e}")
            return []
    
    def list_objects(self, bucket_name: str, prefix: str = "", recursive: bool = True) -> List[Dict[str, Any]]:
        """List objects in a bucket"""
        try:
            objects = []
            for obj in self.client.list_objects(bucket_name, prefix=prefix, recursive=recursive):
                objects.append({
                    'name': obj.object_name,
                    'size': obj.size,
                    'last_modified': obj.last_modified.isoformat() if obj.last_modified else None,
                    'etag': obj.etag
                })
            return objects
        except S3Error as e:
            print(f"❌ Failed to list objects in bucket '{bucket_name}': {e}")
            return []
    
    def filter_objects(self, objects: List[Dict[str, Any]], 
                      prefix: str = None, suffix: str = None, contains: str = None,
                      size_min: int = None, size_max: int = None,
                      date_from: str = None, date_to: str = None) -> List[Dict[str, Any]]:
        """
        Filter objects based on various criteria
        
        Args:
            objects: List of objects to filter
            prefix: Filter by object name prefix
            suffix: Filter by object name suffix
            contains: Filter by object name containing string
            size_min: Minimum size in MB
            size_max: Maximum size in MB
            date_from: Filter objects modified after date (YYYY-MM-DD)
            date_to: Filter objects modified before date (YYYY-MM-DD)
        
        Returns:
            Filtered list of objects
        """
        filtered = objects
        
        if prefix:
            filtered = [obj for obj in filtered if obj['name'].startswith(prefix)]
        
        if suffix:
            filtered = [obj for obj in filtered if obj['name'].endswith(suffix)]
        
        if contains:
            # Support complex logic with parentheses, OR (comma) and AND (&) logic
            filtered = self._apply_contains_filter(filtered, contains)
        
        if size_min is not None:
            size_min_bytes = size_min * 1024 * 1024
            filtered = [obj for obj in filtered if obj['size'] >= size_min_bytes]
        
        if size_max is not None:
            size_max_bytes = size_max * 1024 * 1024
            filtered = [obj for obj in filtered if obj['size'] <= size_max_bytes]
        
        if date_from:
            try:
                from_date = datetime.datetime.fromisoformat(date_from).date()
                filtered = [obj for obj in filtered 
                           if obj['last_modified'] and 
                           datetime.datetime.fromisoformat(obj['last_modified']).date() >= from_date]
            except ValueError:
                print(f"⚠️  Invalid date format for --filter-date-from: {date_from}")
        
        if date_to:
            try:
                to_date = datetime.datetime.fromisoformat(date_to).date()
                filtered = [obj for obj in filtered 
                           if obj['last_modified'] and 
                           datetime.datetime.fromisoformat(obj['last_modified']).date() <= to_date]
            except ValueError:
                print(f"⚠️  Invalid date format for --filter-date-to: {date_to}")
        
        return filtered

    def _apply_contains_filter(self, objects: List[Dict[str, Any]], contains_expr: str) -> List[Dict[str, Any]]:
        """
        Apply complex contains filter with parentheses support
        
        Args:
            objects: List of objects to filter
            contains_expr: Complex contains expression with parentheses, OR (comma), and AND (&) logic
        
        Returns:
            Filtered list of objects
        """
        def evaluate_expression(expr: str, obj_name: str) -> bool:
            """Evaluate a single expression for an object name"""
            expr = expr.strip()
            
            # Handle parentheses first
            if '(' in expr and ')' in expr:
                # Find the innermost parentheses
                start = expr.rfind('(')
                end = expr.find(')', start)
                if start != -1 and end != -1:
                    # Extract the content inside parentheses
                    inner_expr = expr[start+1:end]
                    # Evaluate the inner expression
                    inner_result = evaluate_expression(inner_expr, obj_name)
                    # Replace the parentheses expression with the result
                    new_expr = expr[:start] + ('true' if inner_result else 'false') + expr[end+1:]
                    return evaluate_expression(new_expr, obj_name)
            
            # Handle AND logic (&)
            if '&' in expr:
                parts = [p.strip() for p in expr.split('&')]
                return all(evaluate_expression(part, obj_name) for part in parts)
            
            # Handle OR logic (,)
            if ',' in expr:
                parts = [p.strip() for p in expr.split(',')]
                return any(evaluate_expression(part, obj_name) for part in parts)
            
            # Single keyword
            return expr in obj_name
        
        return [obj for obj in objects if evaluate_expression(contains_expr, obj['name'])]

    def download_file(self, bucket_name: str, object_name: str, local_path: str = None) -> Optional[str]:
        """
        Download a file from MinIO
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object in the bucket
            local_path: Local path to save the file (optional, will use temp file if not provided)
        
        Returns:
            Local file path if successful, None otherwise
        """
        try:
            if not local_path:
                # Create temporary file
                temp_dir = tempfile.gettempdir()
                filename = Path(object_name).name
                local_path = os.path.join(temp_dir, f"minio_{filename}")
            
            print(f"📥 Downloading {object_name} from bucket {bucket_name}...")
            self.client.fget_object(bucket_name, object_name, local_path)
            print(f"✅ Downloaded to: {local_path}")
            return local_path
            
        except S3Error as e:
            print(f"❌ Failed to download {object_name}: {e}")
            return None
        except Exception as e:
            print(f"❌ Unexpected error downloading {object_name}: {e}")
            return None
    
    def analyze_parquet_from_minio(self, bucket_name: str, object_name: str, 
                                 command: str = "analyze", output_file: str = None,
                                 rows: int = 10, verbose: bool = False, 
                                 id_value: str = None, id_column: str = None) -> bool:
        """
        Download Parquet file from MinIO and analyze it using parquet_analyzer_cli.py
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object in the bucket
            command: Analysis command (analyze, metadata, vector, export, data)
            output_file: Output file path for export/data commands
            rows: Number of rows to export (for data command)
            verbose: Verbose output
        
        Returns:
            True if successful, False otherwise
        """
        # Download the file
        local_path = self.download_file(bucket_name, object_name)
        if not local_path:
            return False
        
        try:
            # Build command for parquet_analyzer_cli.py
            cli_script = Path(__file__).parent / "parquet_analyzer_cli.py"
            if not cli_script.exists():
                print(f"❌ parquet_analyzer_cli.py not found at: {cli_script}")
                return False
            
            cmd = [sys.executable, str(cli_script), command, local_path]
            
            # Add optional arguments
            if output_file:
                cmd.extend(["--output", output_file])
            if rows != 10:
                cmd.extend(["--rows", str(rows)])
            if verbose:
                cmd.append("--verbose")
            if id_value:
                cmd.extend(["--id-value", str(id_value)])
            if id_column:
                cmd.extend(["--id-column", id_column])
            
            print(f"🔍 Running analysis command: {' '.join(cmd)}")
            print("=" * 60)
            
            # Execute the command
            result = subprocess.run(cmd, capture_output=False, text=True)
            
            if result.returncode == 0:
                print("✅ Analysis completed successfully")
                return True
            else:
                print(f"❌ Analysis failed with return code: {result.returncode}")
                return False
                
        except Exception as e:
            print(f"❌ Failed to run analysis: {e}")
            return False
        finally:
            # Clean up temporary file if it was created
            if local_path and local_path.startswith(tempfile.gettempdir()):
                try:
                    os.remove(local_path)
                    print(f"🧹 Cleaned up temporary file: {local_path}")
                except:
                    pass
    
    def interactive_mode(self):
        """Interactive mode for browsing and analyzing files"""
        print("🔍 MinIO Interactive Mode")
        print("=" * 40)
        
        # List buckets
        buckets = self.list_buckets()
        if not buckets:
            print("❌ No buckets found or access denied")
            return
        
        print(f"📦 Found {len(buckets)} bucket(s):")
        for i, bucket in enumerate(buckets):
            print(f"  {i+1}. {bucket['name']}")
        
        # Select bucket
        while True:
            try:
                choice = input(f"\nSelect bucket (1-{len(buckets)}) or 'q' to quit: ").strip()
                if choice.lower() == 'q':
                    return
                
                bucket_idx = int(choice) - 1
                if 0 <= bucket_idx < len(buckets):
                    selected_bucket = buckets[bucket_idx]['name']
                    break
                else:
                    print("❌ Invalid selection")
            except ValueError:
                print("❌ Please enter a valid number")
        
        # Main interactive loop
        while True:
            print(f"\n📁 Current bucket: '{selected_bucket}'")
            print("=" * 50)
            
            # List objects in selected bucket
            print(f"📁 Objects in bucket '{selected_bucket}':")
            objects = self.list_objects(selected_bucket)
            
            if not objects:
                print("❌ No objects found in this bucket")
                return
            
            # Apply filters if user wants to
            print("\n🔍 Filter options:")
            print("  1. No filter (show all)")
            print("  2. Apply custom filters")
            
            filter_choice = input("Select filter option (1-2): ").strip()
            
            if filter_choice == "2":
                print("\n📋 Available filters:")
                print("  - prefix: Filter by object name prefix")
                print("  - suffix: Filter by object name suffix") 
                print("  - contains: Filter by object name containing string(s). Use comma for OR logic, & for AND logic, () for grouping")
                print("  - size_min: Filter by minimum size in MB")
                print("  - size_max: Filter by maximum size in MB")
                print("  - date_from: Filter by modification date (YYYY-MM-DD)")
                print("  - date_to: Filter by modification date (YYYY-MM-DD)")
                
                prefix = input("Prefix filter (or press Enter to skip): ").strip() or None
                suffix = input("Suffix filter (or press Enter to skip): ").strip() or None
                contains = input("Contains filter (or press Enter to skip): ").strip() or None
                
                size_min_str = input("Minimum size in MB (or press Enter to skip): ").strip()
                size_min = int(size_min_str) if size_min_str else None
                
                size_max_str = input("Maximum size in MB (or press Enter to skip): ").strip()
                size_max = int(size_max_str) if size_max_str else None
                
                date_from = input("Date from (YYYY-MM-DD, or press Enter to skip): ").strip() or None
                date_to = input("Date to (YYYY-MM-DD, or press Enter to skip): ").strip() or None
                
                original_count = len(objects)
                objects = self.filter_objects(
                    objects, prefix, suffix, contains, size_min, size_max, date_from, date_to
                )
                
                if original_count != len(objects):
                    print(f"🔍 Applied filters: {original_count} → {len(objects)} objects")
            
            # Filter Parquet files and Milvus binlog files
            # Milvus binlog files don't have .parquet extension but are actually Parquet format
            parquet_files = []
            for obj in objects:
                name = obj['name'].lower()
                # Check for .parquet files
                if name.endswith('.parquet'):
                    parquet_files.append(obj)
                # Check for Milvus binlog files (insert_log, delta_log, etc.)
                elif any(log_type in name for log_type in ['insert_log', 'delta_log', 'stats_log', 'index_files']):
                    parquet_files.append(obj)
            
            if not parquet_files:
                print("❌ No Parquet files or Milvus binlog files found in this bucket")
                return
            
            print(f"📊 Found {len(parquet_files)} Parquet file(s):")
            for i, obj in enumerate(parquet_files):
                size_mb = obj['size'] / (1024 * 1024)
                modified_str = ""
                if obj['last_modified']:
                    modified_str = f" (modified: {obj['last_modified'][:10]})"
                print(f"  {i+1}. {obj['name']} ({size_mb:.2f} MB){modified_str}")
            
            # Select file
            while True:
                try:
                    choice = input(f"\nSelect file (1-{len(parquet_files)}) or 'b' to change bucket or 'q' to quit: ").strip()
                    if choice.lower() == 'q':
                        return
                    elif choice.lower() == 'b':
                        # Go back to bucket selection
                        break
                    
                    file_idx = int(choice) - 1
                    if 0 <= file_idx < len(parquet_files):
                        selected_file = parquet_files[file_idx]['name']
                        break
                    else:
                        print("❌ Invalid selection")
                except ValueError:
                    print("❌ Please enter a valid number")
            
            if choice.lower() == 'b':
                # Re-select bucket
                print(f"\n📦 Available buckets:")
                for i, bucket in enumerate(buckets):
                    print(f"  {i+1}. {bucket['name']}")
                
                while True:
                    try:
                        choice = input(f"\nSelect bucket (1-{len(buckets)}) or 'q' to quit: ").strip()
                        if choice.lower() == 'q':
                            return
                        
                        bucket_idx = int(choice) - 1
                        if 0 <= bucket_idx < len(buckets):
                            selected_bucket = buckets[bucket_idx]['name']
                            break
                        else:
                            print("❌ Invalid selection")
                    except ValueError:
                        print("❌ Please enter a valid number")
                continue
            
            # File analysis loop
            while True:
                print(f"\n📄 Selected file: {selected_file}")
                print("-" * 40)
                
                # Select analysis command
                commands = ["analyze", "metadata", "vector", "export", "data", "query"]
                print(f"🔍 Available analysis commands:")
                for i, cmd in enumerate(commands):
                    print(f"  {i+1}. {cmd}")
                
                while True:
                    try:
                        choice = input(f"\nSelect command (1-{len(commands)}) or 'f' to change file or 'b' to change bucket or 'q' to quit: ").strip()
                        if choice.lower() == 'q':
                            return
                        elif choice.lower() == 'b':
                            # Go back to bucket selection
                            break
                        elif choice.lower() == 'f':
                            # Go back to file selection
                            break
                        
                        cmd_idx = int(choice) - 1
                        if 0 <= cmd_idx < len(commands):
                            selected_command = commands[cmd_idx]
                            break
                        else:
                            print("❌ Invalid selection")
                    except ValueError:
                        print("❌ Please enter a valid number")
                
                if choice.lower() in ['b', 'f']:
                    break
                
                # Additional options
                output_file = None
                rows = 10
                verbose = False
                id_value = None
                id_column = None
                
                if selected_command in ["export", "data"]:
                    output_choice = input("Enter output file path (or press Enter for default): ").strip()
                    if output_choice:
                        output_file = output_choice
                
                if selected_command == "data":
                    rows_choice = input("Enter number of rows to export (default: 10): ").strip()
                    if rows_choice:
                        try:
                            rows = int(rows_choice)
                        except ValueError:
                            print("❌ Invalid number, using default: 10")
                
                if selected_command == "query":
                    id_value_choice = input("Enter ID value to query (or press Enter to see available ID columns): ").strip()
                    if id_value_choice:
                        id_value = id_value_choice
                        id_column_choice = input("Enter ID column name (or press Enter for auto-detection): ").strip()
                        if id_column_choice:
                            id_column = id_column_choice
                
                verbose_choice = input("Verbose output? (y/N): ").strip().lower()
                verbose = verbose_choice in ['y', 'yes']
                
                # Run analysis
                print(f"\n🚀 Starting analysis...")
                success = self.analyze_parquet_from_minio(
                    selected_bucket, selected_file, selected_command, 
                    output_file, rows, verbose, id_value, id_column
                )
                
                if success:
                    print("✅ Analysis completed successfully!")
                else:
                    print("❌ Analysis failed")
                
                # Ask if user wants to continue with the same file
                continue_choice = input(f"\nContinue with the same file '{selected_file}'? (y/N): ").strip().lower()
                if continue_choice not in ['y', 'yes']:
                    break
            
            if choice.lower() == 'b':
                continue


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="MinIO Client for Parquet Analysis and Milvus Binlog Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode
  python minio_client.py --endpoint localhost --interactive
  
  # Analyze specific Parquet file
  python minio_client.py --endpoint localhost --bucket mybucket --object data.parquet --command analyze
  
  # Analyze Milvus binlog file
  python minio_client.py --endpoint localhost --bucket a-bucket --object files/insert_log/459761955352871853/459761955352871854/459761955353071864/0 --command analyze
  
  # Query by ID
  python minio_client.py --endpoint localhost --bucket a-bucket --object files/insert_log/459761955352871853/459761955352871854/459761955353071864/0 --command query --id-value 123
  
  # Export metadata
  python minio_client.py --endpoint localhost --bucket mybucket --object data.parquet --command export --output result.json
  
  # List buckets
  python minio_client.py --endpoint localhost --list-buckets
  
  # List objects in bucket
  python minio_client.py --endpoint localhost --bucket mybucket --list-objects
  
  # Filter objects by prefix (insert_log files only)
  python minio_client.py --endpoint localhost --bucket mybucket --list-objects --filter-prefix "files/insert_log/"
  
  # Filter objects by size (files larger than 1MB)
  python minio_client.py --endpoint localhost --bucket mybucket --list-objects --filter-size-min 1
  
  # Filter objects by date range
  python minio_client.py --endpoint localhost --bucket mybucket --list-objects --filter-date-from "2024-01-01" --filter-date-to "2024-01-31"
  
  # Combine multiple filters
  python minio_client.py --endpoint localhost --bucket mybucket --list-objects --filter-prefix "files/" --filter-size-min 0.5 --filter-contains "insert"
  
  # Filter with OR logic (files containing 'insert' OR 'delete')
  python minio_client.py --endpoint localhost --bucket mybucket --list-objects --filter-contains "insert,delete"
  
  # Filter with AND logic (files containing 'insert' AND 'log')
  python minio_client.py --endpoint localhost --bucket mybucket --list-objects --filter-contains "insert&log"
  
  # Filter with parentheses grouping ((insert OR delete) AND log)
  python minio_client.py --endpoint localhost --bucket mybucket --list-objects --filter-contains "(insert,delete)&log"
  
  # Complex nested parentheses ((insert OR delete) AND (log OR bin))
  python minio_client.py --endpoint localhost --bucket mybucket --list-objects --filter-contains "(insert,delete)&(log,bin)"
        """
    )
    
    # Connection arguments
    parser.add_argument("--endpoint", "-e", required=True, help="MinIO server endpoint")
    parser.add_argument("--port", "-p", type=int, default=9000, help="MinIO server port (default: 9000)")
    parser.add_argument("--secure", "-s", action="store_true", help="Use HTTPS")
    parser.add_argument("--access-key", "-a", help="MinIO access key")
    parser.add_argument("--secret-key", "-k", help="MinIO secret key")
    
    # Operation arguments
    parser.add_argument("--interactive", "-i", action="store_true", help="Interactive mode")
    parser.add_argument("--list-buckets", "-b", action="store_true", help="List all buckets")
    parser.add_argument("--bucket", help="Bucket name")
    parser.add_argument("--list-objects", "-l", action="store_true", help="List objects in bucket")
    parser.add_argument("--object", "-o", help="Object name in bucket")
    
    # Filter arguments
    parser.add_argument("--filter-prefix", help="Filter objects by prefix (e.g., 'files/insert_log/')")
    parser.add_argument("--filter-suffix", help="Filter objects by suffix (e.g., '.parquet')")
    parser.add_argument("--filter-contains", help="Filter objects containing specific string(s). Use comma for OR logic (e.g., 'insert,delete'), use & for AND logic (e.g., 'insert&log'), use () for grouping (e.g., '(insert,delete)&log')")
    parser.add_argument("--filter-size-min", type=int, help="Filter objects by minimum size in MB")
    parser.add_argument("--filter-size-max", type=int, help="Filter objects by maximum size in MB")
    parser.add_argument("--filter-date-from", help="Filter objects modified after date (YYYY-MM-DD)")
    parser.add_argument("--filter-date-to", help="Filter objects modified before date (YYYY-MM-DD)")
    
    # Analysis arguments
    parser.add_argument("--command", "-c", choices=["analyze", "metadata", "vector", "export", "data", "query"],
                       default="analyze", help="Analysis command (default: analyze)")
    parser.add_argument("--output", help="Output file path (for export/data commands)")
    parser.add_argument("--rows", "-r", type=int, default=10, help="Number of rows to export (for data command)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--id-value", "-q", help="ID value to query (for query command)")
    parser.add_argument("--id-column", help="ID column name (for query command, auto-detected if not specified)")
    
    args = parser.parse_args()
    
    # Initialize MinIO client
    client = MinioParquetAnalyzer(
        endpoint=args.endpoint,
        port=args.port,
        secure=args.secure,
        access_key=args.access_key,
        secret_key=args.secret_key
    )
    
    # Execute requested operation
    if args.interactive:
        client.interactive_mode()
    elif args.list_buckets:
        buckets = client.list_buckets()
        if buckets:
            print(f"📦 Found {len(buckets)} bucket(s):")
            for bucket in buckets:
                print(f"  - {bucket['name']}")
        else:
            print("❌ No buckets found or access denied")
    elif args.list_objects:
        if not args.bucket:
            print("❌ --bucket is required for --list-objects")
            sys.exit(1)
        
        objects = client.list_objects(args.bucket)
        if objects:
            # Apply filters if specified
            original_count = len(objects)
            objects = client.filter_objects(
                objects,
                prefix=args.filter_prefix,
                suffix=args.filter_suffix,
                contains=args.filter_contains,
                size_min=args.filter_size_min,
                size_max=args.filter_size_max,
                date_from=args.filter_date_from,
                date_to=args.filter_date_to
            )
            
            if original_count != len(objects):
                print(f"🔍 Applied filters: {original_count} → {len(objects)} objects")
            
            if objects:
                print(f"📁 Found {len(objects)} object(s) in bucket '{args.bucket}':")
                for obj in objects:
                    size_mb = obj['size'] / (1024 * 1024)
                    modified_str = ""
                    if obj['last_modified']:
                        modified_str = f" (modified: {obj['last_modified'][:10]})"
                    print(f"  - {obj['name']} ({size_mb:.2f} MB){modified_str}")
            else:
                print("❌ No objects match the specified filters")
        else:
            print("❌ No objects found or access denied")
    elif args.object:
        if not args.bucket:
            print("❌ --bucket is required when specifying --object")
            sys.exit(1)
        
        success = client.analyze_parquet_from_minio(
            args.bucket, args.object, args.command, 
            args.output, args.rows, args.verbose, args.id_value, args.id_column
        )
        
        if not success:
            sys.exit(1)
    else:
        print("❌ No operation specified. Use --interactive, --list-buckets, --list-objects, or specify --object")
        sys.exit(1)


if __name__ == "__main__":
    main() 