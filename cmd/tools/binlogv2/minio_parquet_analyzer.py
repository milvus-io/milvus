#!/usr/bin/env python3
"""
MinIO Parquet Analyzer - Integrated Tool
Combines MinIO client with parquet_analyzer_cli.py for seamless analysis
"""

import argparse
import sys
import os
import tempfile
import subprocess
import json
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    print("❌ MinIO client library not found. Please install it:")
    print("   pip install minio")
    sys.exit(1)


class MinioParquetAnalyzer:
    """Integrated MinIO and Parquet Analyzer"""
    
    def __init__(self, endpoint: str, port: int = 9000, secure: bool = False, 
                 access_key: str = None, secret_key: str = None):
        """Initialize the integrated analyzer"""
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
        
        # Check if parquet_analyzer_cli.py exists
        self.cli_script = Path(__file__).parent / "parquet_analyzer_cli.py"
        if not self.cli_script.exists():
            print(f"❌ parquet_analyzer_cli.py not found at: {self.cli_script}")
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
    
    def list_parquet_files(self, bucket_name: str, prefix: str = "") -> List[Dict[str, Any]]:
        """List Parquet files in a bucket"""
        try:
            parquet_files = []
            for obj in self.client.list_objects(bucket_name, prefix=prefix, recursive=True):
                if obj.object_name.lower().endswith('.parquet'):
                    parquet_files.append({
                        'name': obj.object_name,
                        'size': obj.size,
                        'size_mb': obj.size / (1024 * 1024),
                        'last_modified': obj.last_modified.isoformat() if obj.last_modified else None,
                        'etag': obj.etag
                    })
            return parquet_files
        except S3Error as e:
            print(f"❌ Failed to list objects in bucket '{bucket_name}': {e}")
            return []
    
    def download_and_analyze(self, bucket_name: str, object_name: str, 
                           command: str = "analyze", output_file: str = None,
                           rows: int = 10, verbose: bool = False, 
                           keep_local: bool = False) -> Dict[str, Any]:
        """
        Download Parquet file from MinIO and analyze it
        
        Returns:
            Dictionary with analysis results and metadata
        """
        result = {
            'success': False,
            'bucket': bucket_name,
            'object': object_name,
            'command': command,
            'local_path': None,
            'analysis_output': None,
            'error': None,
            'timestamp': datetime.now().isoformat()
        }
        
        # Download the file
        try:
            local_path = self._download_file(bucket_name, object_name, keep_local)
            if not local_path:
                result['error'] = "Failed to download file"
                return result
            
            result['local_path'] = local_path
            
            # Run analysis
            analysis_result = self._run_analysis(local_path, command, output_file, rows, verbose)
            result['analysis_output'] = analysis_result
            
            if analysis_result['success']:
                result['success'] = True
            else:
                result['error'] = analysis_result['error']
                
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def _download_file(self, bucket_name: str, object_name: str, keep_local: bool = False) -> Optional[str]:
        """Download file from MinIO"""
        try:
            if keep_local:
                # Save to current directory
                local_path = Path(object_name).name
            else:
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
    
    def _run_analysis(self, local_path: str, command: str, output_file: str = None,
                     rows: int = 10, verbose: bool = False) -> Dict[str, Any]:
        """Run parquet_analyzer_cli.py on local file"""
        result = {
            'success': False,
            'command': command,
            'output_file': output_file,
            'error': None
        }
        
        try:
            # Build command
            cmd = [sys.executable, str(self.cli_script), command, local_path]
            
            # Add optional arguments
            if output_file:
                cmd.extend(["--output", output_file])
            if rows != 10:
                cmd.extend(["--rows", str(rows)])
            if verbose:
                cmd.append("--verbose")
            
            print(f"🔍 Running analysis: {' '.join(cmd)}")
            print("=" * 60)
            
            # Execute the command
            process = subprocess.run(cmd, capture_output=True, text=True)
            
            result['return_code'] = process.returncode
            result['stdout'] = process.stdout
            result['stderr'] = process.stderr
            
            if process.returncode == 0:
                result['success'] = True
                print("✅ Analysis completed successfully")
            else:
                result['error'] = f"Analysis failed with return code: {process.returncode}"
                print(f"❌ {result['error']}")
                if process.stderr:
                    print(f"Error output: {process.stderr}")
                
        except Exception as e:
            result['error'] = str(e)
            print(f"❌ Failed to run analysis: {e}")
        
        return result
    
    def batch_analyze(self, bucket_name: str, prefix: str = "", command: str = "analyze",
                     output_dir: str = None, verbose: bool = False) -> List[Dict[str, Any]]:
        """
        Analyze multiple Parquet files in a bucket
        
        Args:
            bucket_name: Name of the bucket
            prefix: Prefix to filter files
            command: Analysis command
            output_dir: Directory to save output files
            verbose: Verbose output
        
        Returns:
            List of analysis results
        """
        print(f"🔍 Batch analyzing Parquet files in bucket '{bucket_name}'")
        if prefix:
            print(f"📁 Filtering by prefix: '{prefix}'")
        
        # List Parquet files
        parquet_files = self.list_parquet_files(bucket_name, prefix)
        
        if not parquet_files:
            print("❌ No Parquet files found")
            return []
        
        print(f"📊 Found {len(parquet_files)} Parquet file(s)")
        
        # Create output directory if specified
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            print(f"📁 Output directory: {output_dir}")
        
        results = []
        
        for i, file_info in enumerate(parquet_files, 1):
            print(f"\n{'='*60}")
            print(f"📊 Processing file {i}/{len(parquet_files)}: {file_info['name']}")
            print(f"📏 Size: {file_info['size_mb']:.2f} MB")
            
            # Determine output file
            output_file = None
            if output_dir:
                base_name = Path(file_info['name']).stem
                output_file = os.path.join(output_dir, f"{base_name}_{command}_result.json")
            
            # Analyze the file
            result = self.download_and_analyze(
                bucket_name, file_info['name'], command, output_file, verbose=verbose
            )
            
            results.append(result)
            
            # Clean up temporary file
            if result['local_path'] and result['local_path'].startswith(tempfile.gettempdir()):
                try:
                    os.remove(result['local_path'])
                except:
                    pass
        
        # Print summary
        successful = sum(1 for r in results if r['success'])
        failed = len(results) - successful
        
        print(f"\n{'='*60}")
        print(f"📊 Batch Analysis Summary:")
        print(f"  Total files: {len(results)}")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
        
        return results
    
    def interactive_mode(self):
        """Interactive mode for browsing and analyzing files"""
        print("🔍 MinIO Parquet Analyzer - Interactive Mode")
        print("=" * 50)
        
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
        
        # List Parquet files in selected bucket
        print(f"\n📁 Parquet files in bucket '{selected_bucket}':")
        parquet_files = self.list_parquet_files(selected_bucket)
        
        if not parquet_files:
            print("❌ No Parquet files found in this bucket")
            return
        
        print(f"📊 Found {len(parquet_files)} Parquet file(s):")
        for i, obj in enumerate(parquet_files):
            print(f"  {i+1}. {obj['name']} ({obj['size_mb']:.2f} MB)")
        
        # Select operation mode
        print(f"\n🔍 Operation modes:")
        print("  1. Analyze single file")
        print("  2. Batch analyze all files")
        print("  3. Select specific files")
        
        while True:
            try:
                mode_choice = input(f"\nSelect mode (1-3) or 'q' to quit: ").strip()
                if mode_choice.lower() == 'q':
                    return
                
                mode = int(mode_choice)
                if mode in [1, 2, 3]:
                    break
                else:
                    print("❌ Invalid selection")
            except ValueError:
                print("❌ Please enter a valid number")
        
        if mode == 1:
            # Single file analysis
            self._interactive_single_file(selected_bucket, parquet_files)
        elif mode == 2:
            # Batch analysis
            self._interactive_batch_analysis(selected_bucket, parquet_files)
        elif mode == 3:
            # Select specific files
            self._interactive_select_files(selected_bucket, parquet_files)
    
    def _interactive_single_file(self, bucket_name: str, parquet_files: List[Dict[str, Any]]):
        """Interactive single file analysis"""
        # Select file
        while True:
            try:
                choice = input(f"\nSelect file (1-{len(parquet_files)}) or 'q' to quit: ").strip()
                if choice.lower() == 'q':
                    return
                
                file_idx = int(choice) - 1
                if 0 <= file_idx < len(parquet_files):
                    selected_file = parquet_files[file_idx]['name']
                    break
                else:
                    print("❌ Invalid selection")
            except ValueError:
                print("❌ Please enter a valid number")
        
        # Select analysis command
        commands = ["analyze", "metadata", "vector", "export", "data"]
        print(f"\n🔍 Available analysis commands:")
        for i, cmd in enumerate(commands):
            print(f"  {i+1}. {cmd}")
        
        while True:
            try:
                choice = input(f"\nSelect command (1-{len(commands)}) or 'q' to quit: ").strip()
                if choice.lower() == 'q':
                    return
                
                cmd_idx = int(choice) - 1
                if 0 <= cmd_idx < len(commands):
                    selected_command = commands[cmd_idx]
                    break
                else:
                    print("❌ Invalid selection")
            except ValueError:
                print("❌ Please enter a valid number")
        
        # Additional options
        output_file = None
        rows = 10
        verbose = False
        
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
        
        verbose_choice = input("Verbose output? (y/N): ").strip().lower()
        verbose = verbose_choice in ['y', 'yes']
        
        # Run analysis
        print(f"\n🚀 Starting analysis...")
        result = self.download_and_analyze(
            bucket_name, selected_file, selected_command, 
            output_file, rows, verbose
        )
        
        if result['success']:
            print("✅ Analysis completed successfully!")
        else:
            print(f"❌ Analysis failed: {result['error']}")
    
    def _interactive_batch_analysis(self, bucket_name: str, parquet_files: List[Dict[str, Any]]):
        """Interactive batch analysis"""
        print(f"\n📊 Batch analysis for {len(parquet_files)} files")
        
        # Select command
        commands = ["analyze", "metadata", "vector", "export", "data"]
        print(f"\n🔍 Available analysis commands:")
        for i, cmd in enumerate(commands):
            print(f"  {i+1}. {cmd}")
        
        while True:
            try:
                choice = input(f"\nSelect command (1-{len(commands)}) or 'q' to quit: ").strip()
                if choice.lower() == 'q':
                    return
                
                cmd_idx = int(choice) - 1
                if 0 <= cmd_idx < len(commands):
                    selected_command = commands[cmd_idx]
                    break
                else:
                    print("❌ Invalid selection")
            except ValueError:
                print("❌ Please enter a valid number")
        
        # Output directory
        output_dir = input("Enter output directory (or press Enter for current dir): ").strip()
        if not output_dir:
            output_dir = "."
        
        verbose_choice = input("Verbose output? (y/N): ").strip().lower()
        verbose = verbose_choice in ['y', 'yes']
        
        # Run batch analysis
        print(f"\n🚀 Starting batch analysis...")
        results = self.batch_analyze(bucket_name, "", selected_command, output_dir, verbose)
        
        print(f"✅ Batch analysis completed!")
    
    def _interactive_select_files(self, bucket_name: str, parquet_files: List[Dict[str, Any]]):
        """Interactive select specific files"""
        print(f"\n📋 Select files to analyze (comma-separated numbers, e.g., 1,3,5)")
        print(f"Available files:")
        for i, obj in enumerate(parquet_files):
            print(f"  {i+1}. {obj['name']} ({obj['size_mb']:.2f} MB)")
        
        while True:
            try:
                choice = input(f"\nEnter file numbers (1-{len(parquet_files)}) or 'q' to quit: ").strip()
                if choice.lower() == 'q':
                    return
                
                selected_indices = [int(x.strip()) - 1 for x in choice.split(',')]
                if all(0 <= idx < len(parquet_files) for idx in selected_indices):
                    selected_files = [parquet_files[idx]['name'] for idx in selected_indices]
                    break
                else:
                    print("❌ Invalid selection")
            except ValueError:
                print("❌ Please enter valid numbers")
        
        # Select command
        commands = ["analyze", "metadata", "vector", "export", "data"]
        print(f"\n🔍 Available analysis commands:")
        for i, cmd in enumerate(commands):
            print(f"  {i+1}. {cmd}")
        
        while True:
            try:
                choice = input(f"\nSelect command (1-{len(commands)}) or 'q' to quit: ").strip()
                if choice.lower() == 'q':
                    return
                
                cmd_idx = int(choice) - 1
                if 0 <= cmd_idx < len(commands):
                    selected_command = commands[cmd_idx]
                    break
                else:
                    print("❌ Invalid selection")
            except ValueError:
                print("❌ Please enter a valid number")
        
        # Additional options
        output_dir = input("Enter output directory (or press Enter for current dir): ").strip()
        if not output_dir:
            output_dir = "."
        
        verbose_choice = input("Verbose output? (y/N): ").strip().lower()
        verbose = verbose_choice in ['y', 'yes']
        
        # Run analysis for selected files
        print(f"\n🚀 Starting analysis for {len(selected_files)} selected files...")
        
        results = []
        for i, file_name in enumerate(selected_files, 1):
            print(f"\n📊 Processing file {i}/{len(selected_files)}: {file_name}")
            
            # Determine output file
            output_file = None
            if output_dir:
                base_name = Path(file_name).stem
                output_file = os.path.join(output_dir, f"{base_name}_{selected_command}_result.json")
            
            result = self.download_and_analyze(
                bucket_name, file_name, selected_command, output_file, verbose=verbose
            )
            results.append(result)
        
        # Print summary
        successful = sum(1 for r in results if r['success'])
        print(f"\n✅ Analysis completed! {successful}/{len(results)} files processed successfully.")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="MinIO Parquet Analyzer - Integrated Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode
  python minio_parquet_analyzer.py --endpoint localhost --interactive
  
  # Analyze specific file
  python minio_parquet_analyzer.py --endpoint localhost --bucket mybucket --object data.parquet --command analyze
  
  # Batch analyze all Parquet files
  python minio_parquet_analyzer.py --endpoint localhost --bucket mybucket --batch --command metadata --output-dir results
  
  # List buckets
  python minio_parquet_analyzer.py --endpoint localhost --list-buckets
  
  # List Parquet files in bucket
  python minio_parquet_analyzer.py --endpoint localhost --bucket mybucket --list-files
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
    parser.add_argument("--list-files", "-l", action="store_true", help="List Parquet files in bucket")
    parser.add_argument("--object", "-o", help="Object name in bucket")
    parser.add_argument("--batch", action="store_true", help="Batch analyze all Parquet files in bucket")
    
    # Analysis arguments
    parser.add_argument("--command", "-c", choices=["analyze", "metadata", "vector", "export", "data"],
                       default="analyze", help="Analysis command (default: analyze)")
    parser.add_argument("--output", help="Output file path (for single file analysis)")
    parser.add_argument("--output-dir", help="Output directory (for batch analysis)")
    parser.add_argument("--rows", "-r", type=int, default=10, help="Number of rows to export (for data command)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--keep-local", action="store_true", help="Keep downloaded files locally")
    
    args = parser.parse_args()
    
    # Initialize analyzer
    analyzer = MinioParquetAnalyzer(
        endpoint=args.endpoint,
        port=args.port,
        secure=args.secure,
        access_key=args.access_key,
        secret_key=args.secret_key
    )
    
    # Execute requested operation
    if args.interactive:
        analyzer.interactive_mode()
    elif args.list_buckets:
        buckets = analyzer.list_buckets()
        if buckets:
            print(f"📦 Found {len(buckets)} bucket(s):")
            for bucket in buckets:
                print(f"  - {bucket['name']}")
        else:
            print("❌ No buckets found or access denied")
    elif args.list_files:
        if not args.bucket:
            print("❌ --bucket is required for --list-files")
            sys.exit(1)
        
        files = analyzer.list_parquet_files(args.bucket)
        if files:
            print(f"📊 Found {len(files)} Parquet file(s) in bucket '{args.bucket}':")
            for obj in files:
                print(f"  - {obj['name']} ({obj['size_mb']:.2f} MB)")
        else:
            print("❌ No Parquet files found or access denied")
    elif args.batch:
        if not args.bucket:
            print("❌ --bucket is required for --batch")
            sys.exit(1)
        
        results = analyzer.batch_analyze(
            args.bucket, "", args.command, args.output_dir, args.verbose
        )
        
        if not results:
            sys.exit(1)
    elif args.object:
        if not args.bucket:
            print("❌ --bucket is required when specifying --object")
            sys.exit(1)
        
        result = analyzer.download_and_analyze(
            args.bucket, args.object, args.command, 
            args.output, args.rows, args.verbose, args.keep_local
        )
        
        if not result['success']:
            sys.exit(1)
    else:
        print("❌ No operation specified. Use --interactive, --list-buckets, --list-files, --batch, or specify --object")
        sys.exit(1)


if __name__ == "__main__":
    main() 