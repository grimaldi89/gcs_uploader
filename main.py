#!/usr/bin/env python3
"""
Google Cloud Storage Uploader

A comprehensive tool for uploading files to Google Cloud Storage buckets.
Supports single file uploads, batch uploads, and various configuration options.
"""

import os
import sys
import logging
from pathlib import Path
from typing import List, Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import click
from dotenv import load_dotenv
from tqdm import tqdm

from google.cloud import storage
from google.auth.exceptions import DefaultCredentialsError
from google.cloud.exceptions import NotFound, Forbidden


# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GCSUploader:
    """Google Cloud Storage uploader with batch processing capabilities."""
    
    def __init__(self, project_id: Optional[str] = None, credentials_path: Optional[str] = None):
        """
        Initialize the GCS uploader.
        
        Args:
            project_id: Google Cloud project ID
            credentials_path: Path to service account credentials JSON file
        """
        self.project_id = project_id or os.getenv('GOOGLE_CLOUD_PROJECT')
        self.credentials_path = credentials_path or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        
        try:
            if self.credentials_path:
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials_path
            
            self.client = storage.Client(project=self.project_id)
            logger.info(f"Initialized GCS client for project: {self.project_id}")
            
        except DefaultCredentialsError:
            logger.error("No valid Google Cloud credentials found. Please set up authentication.")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize GCS client: {e}")
            raise
    
    def upload_file(
        self, 
        local_file_path: Union[str, Path], 
        bucket_name: str, 
        blob_name: Optional[str] = None,
        make_public: bool = False
    ) -> bool:
        """
        Upload a single file to GCS.
        
        Args:
            local_file_path: Path to the local file
            bucket_name: Name of the GCS bucket
            blob_name: Name for the blob in GCS (defaults to filename)
            make_public: Whether to make the uploaded file publicly accessible
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        local_file_path = Path(local_file_path)
        
        if not local_file_path.exists():
            logger.error(f"File not found: {local_file_path}")
            return False
        
        if blob_name is None:
            blob_name = local_file_path.name
        
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            logger.info(f"Uploading {local_file_path} to gs://{bucket_name}/{blob_name}")
            
            # Upload the file
            blob.upload_from_filename(str(local_file_path))
            
            if make_public:
                blob.make_public()
                logger.info(f"Made blob public: gs://{bucket_name}/{blob_name}")
            
            logger.info(f"Successfully uploaded: gs://{bucket_name}/{blob_name}")
            return True
            
        except NotFound:
            logger.error(f"Bucket not found: {bucket_name}")
            return False
        except Forbidden:
            logger.error(f"Access denied to bucket: {bucket_name}")
            return False
        except Exception as e:
            logger.error(f"Failed to upload {local_file_path}: {e}")
            return False
    
    def upload_directory(
        self, 
        local_dir_path: Union[str, Path], 
        bucket_name: str, 
        prefix: str = "",
        make_public: bool = False,
        max_workers: int = 4
    ) -> dict:
        """
        Upload all files in a directory to GCS.
        
        Args:
            local_dir_path: Path to the local directory
            bucket_name: Name of the GCS bucket
            prefix: Prefix to add to blob names
            make_public: Whether to make uploaded files publicly accessible
            max_workers: Maximum number of concurrent uploads
            
        Returns:
            dict: Upload results with success/failure counts
        """
        local_dir_path = Path(local_dir_path)
        
        if not local_dir_path.exists() or not local_dir_path.is_dir():
            logger.error(f"Directory not found: {local_dir_path}")
            return {"success": 0, "failed": 0, "errors": []}
        
        # Get all files in directory (recursively)
        files_to_upload = []
        for file_path in local_dir_path.rglob('*'):
            if file_path.is_file():
                relative_path = file_path.relative_to(local_dir_path)
                blob_name = f"{prefix}{relative_path}".replace('\\', '/') if prefix else str(relative_path).replace('\\', '/')
                files_to_upload.append((file_path, blob_name))
        
        if not files_to_upload:
            logger.warning(f"No files found in directory: {local_dir_path}")
            return {"success": 0, "failed": 0, "errors": []}
        
        logger.info(f"Found {len(files_to_upload)} files to upload")
        
        results = {"success": 0, "failed": 0, "errors": []}
        
        # Upload files concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all upload tasks
            future_to_file = {
                executor.submit(self.upload_file, file_path, bucket_name, blob_name, make_public): (file_path, blob_name)
                for file_path, blob_name in files_to_upload
            }
            
            # Process completed uploads with progress bar
            with tqdm(total=len(files_to_upload), desc="Uploading files") as pbar:
                for future in as_completed(future_to_file):
                    file_path, blob_name = future_to_file[future]
                    try:
                        success = future.result()
                        if success:
                            results["success"] += 1
                        else:
                            results["failed"] += 1
                            results["errors"].append(f"Failed to upload: {file_path}")
                    except Exception as e:
                        results["failed"] += 1
                        results["errors"].append(f"Error uploading {file_path}: {e}")
                    
                    pbar.update(1)
        
        logger.info(f"Upload complete: {results['success']} successful, {results['failed']} failed")
        return results
    
    def list_buckets(self) -> List[str]:
        """List all accessible buckets."""
        try:
            buckets = list(self.client.list_buckets())
            bucket_names = [bucket.name for bucket in buckets]
            logger.info(f"Found {len(bucket_names)} accessible buckets")
            return bucket_names
        except Exception as e:
            logger.error(f"Failed to list buckets: {e}")
            return []
    
    def bucket_exists(self, bucket_name: str) -> bool:
        """Check if a bucket exists and is accessible."""
        try:
            bucket = self.client.bucket(bucket_name)
            bucket.reload()
            return True
        except NotFound:
            return False
        except Exception as e:
            logger.error(f"Error checking bucket {bucket_name}: {e}")
            return False


@click.group()
@click.option('--project-id', help='Google Cloud project ID')
@click.option('--credentials', help='Path to service account credentials JSON file')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, project_id, credentials, verbose):
    """Google Cloud Storage Uploader CLI."""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    ctx.ensure_object(dict)
    ctx.obj['project_id'] = project_id
    ctx.obj['credentials'] = credentials


@cli.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.argument('bucket_name')
@click.option('--blob-name', help='Name for the blob in GCS (defaults to filename)')
@click.option('--public', is_flag=True, help='Make the uploaded file publicly accessible')
@click.pass_context
def upload(ctx, file_path, bucket_name, blob_name, public):
    """Upload a single file to GCS."""
    try:
        uploader = GCSUploader(
            project_id=ctx.obj['project_id'],
            credentials_path=ctx.obj['credentials']
        )
        
        if not uploader.bucket_exists(bucket_name):
            click.echo(f"Error: Bucket '{bucket_name}' not found or not accessible", err=True)
            sys.exit(1)
        
        success = uploader.upload_file(file_path, bucket_name, blob_name, public)
        
        if success:
            click.echo(f"Successfully uploaded: {file_path}")
        else:
            click.echo(f"Failed to upload: {file_path}", err=True)
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument('directory_path', type=click.Path(exists=True, file_okay=False))
@click.argument('bucket_name')
@click.option('--prefix', default='', help='Prefix to add to blob names')
@click.option('--public', is_flag=True, help='Make uploaded files publicly accessible')
@click.option('--max-workers', default=4, help='Maximum number of concurrent uploads')
@click.pass_context
def upload_dir(ctx, directory_path, bucket_name, prefix, public, max_workers):
    """Upload all files in a directory to GCS."""
    try:
        uploader = GCSUploader(
            project_id=ctx.obj['project_id'],
            credentials_path=ctx.obj['credentials']
        )
        
        if not uploader.bucket_exists(bucket_name):
            click.echo(f"Error: Bucket '{bucket_name}' not found or not accessible", err=True)
            sys.exit(1)
        
        results = uploader.upload_directory(
            directory_path, bucket_name, prefix, public, max_workers
        )
        
        click.echo(f"Upload complete:")
        click.echo(f"  Successful: {results['success']}")
        click.echo(f"  Failed: {results['failed']}")
        
        if results['errors']:
            click.echo("Errors:")
            for error in results['errors']:
                click.echo(f"  - {error}")
        
        if results['failed'] > 0:
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def list_buckets(ctx):
    """List all accessible GCS buckets."""
    try:
        uploader = GCSUploader(
            project_id=ctx.obj['project_id'],
            credentials_path=ctx.obj['credentials']
        )
        
        buckets = uploader.list_buckets()
        
        if buckets:
            click.echo("Accessible buckets:")
            for bucket in buckets:
                click.echo(f"  - {bucket}")
        else:
            click.echo("No accessible buckets found")
            
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()
