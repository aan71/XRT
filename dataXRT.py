"""
XRT - Exchange Rate Processing Module.

This module handles the processing of exchange rate CSV files:
1. Downloads files from S3 pending folder
2. Processes each record via SOAP web service
3. Uploads results to S3 error/processed folders
4. Cleans up original files from S3 and local storage

Author: Alberto Angelini
Date: December 04, 2025

"""

from __future__ import annotations

import logging
import os
import random
import shutil
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import boto3
import pandas as pd
import requests
import zeep
from botocore.exceptions import ClientError
from bs4 import UnicodeDammit
from dotenv import load_dotenv
from requests import Session
from requests.auth import HTTPDigestAuth
from zeep import Client
from zeep.transports import Transport

# Load environment variables from .env file
# The .env file should be in the same directory as this script or project root
load_dotenv()

# Suppress warnings globally
warnings.filterwarnings("ignore")


# =============================================================================
# Configuration
# =============================================================================

def _get_env(key: str, default: str = None) -> str:
    """
    Get environment variable with optional default.
    
    Args:
        key: Environment variable name.
        default: Default value if not set.
        
    Returns:
        Environment variable value.
        
    Raises:
        EnvironmentError: If required variable is not set and no default provided.
    """
    value = os.getenv(key, default)
    if value is None:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            f"Please check your .env file."
        )
    return value


@dataclass
class AppConfig:
    """
    Application configuration settings.
    
    All sensitive values are loaded from environment variables.
    Non-sensitive defaults are provided for convenience.
    """
    
    # AWS S3 Configuration (from environment)
    aws_region: str = field(default_factory=lambda: _get_env('AWS_REGION_NAME'))
    aws_access_key_id: str = field(default_factory=lambda: _get_env('AWS_ACCESS_KEY_ID'))
    aws_secret_access_key: str = field(default_factory=lambda: _get_env('AWS_SECRET_ACCESS_KEY'))
    
    # S3 Bucket Configuration (from environment with defaults)
    bucket_name: str = field(default_factory=lambda: _get_env('S3_BUCKET_NAME', 'cesce-sics'))
    s3_pending_folder: str = field(default_factory=lambda: _get_env('S3_PENDING_FOLDER', 'noprod/inbound/exchange_rate/pending/'))
    s3_error_folder: str = field(default_factory=lambda: _get_env('S3_ERROR_FOLDER', 'noprod/inbound/exchange_rate/error/'))
    s3_processed_folder: str = field(default_factory=lambda: _get_env('S3_PROCESSED_FOLDER', 'noprod/inbound/exchange_rate/processed/'))
    
    # Local Directory Configuration (from environment with defaults)
    local_pending_dir: Path = field(default_factory=lambda: Path(_get_env('LOCAL_PENDING_DIR', 'C:/Temp/XRT/Pending/')))
    local_error_dir: Path = field(default_factory=lambda: Path(_get_env('LOCAL_ERROR_DIR', 'C:/Temp/XRT/Error/')))
    local_processed_dir: Path = field(default_factory=lambda: Path(_get_env('LOCAL_PROCESSED_DIR', 'C:/Temp/XRT/Processed/')))
    log_dir: Path = field(default_factory=lambda: Path(_get_env('LOG_DIR', 'C:/Temp/XRT/Log/')))
    
    # Web Service Configuration (from environment - SENSITIVE)
    wsdl_url: str = field(default_factory=lambda: _get_env('WSDL_URL'))
    ws_username: str = field(default_factory=lambda: _get_env('WS_USERNAME'))
    ws_password: str = field(default_factory=lambda: _get_env('WS_PASSWORD'))
    
    # Logging Configuration
    log_filename: str = field(default_factory=lambda: _get_env('LOG_FILENAME', 'cesce.log'))
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


# =============================================================================
# Logging Setup
# =============================================================================

def setup_logger(config: AppConfig) -> logging.Logger:
    """
    Configure and return a logger instance.
    
    Args:
        config: Application configuration object.
        
    Returns:
        Configured logger instance with unique identifier.
    """
    config.log_dir.mkdir(parents=True, exist_ok=True)
    log_file = config.log_dir / config.log_filename
    
    logging.basicConfig(
        level=logging.INFO,
        filename=str(log_file),
        format=config.log_format
    )
    
    # Generate unique logger name for this run
    unique_id = str(random.randrange(1, 1000**3)).zfill(10)
    return logging.getLogger(f"XRT_{unique_id}")


# =============================================================================
# S3 Operations Class
# =============================================================================

class S3Handler:
    """
    Handles all S3 operations with a reusable session.
    
    This class encapsulates S3 interactions to avoid creating multiple
    boto3 sessions and improve code maintainability.
    """
    
    def __init__(self, config: AppConfig, logger: logging.Logger):
        """
        Initialize S3 handler with AWS credentials from config.
        
        Args:
            config: Application configuration with AWS credentials.
            logger: Logger instance for operation logging.
        """
        self.logger = logger
        self.session = boto3.Session(
            region_name=config.aws_region,
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key
        )
        self._client = None
        self._resource = None
    
    @property
    def client(self):
        """Lazy-loaded S3 client."""
        if self._client is None:
            self._client = self.session.client('s3')
        return self._client
    
    @property
    def resource(self):
        """Lazy-loaded S3 resource."""
        if self._resource is None:
            self._resource = self.session.resource('s3')
        return self._resource
    
    def list_files(self, bucket_name: str, folder_prefix: str) -> list[str]:
        """
        List all files in an S3 folder.
        
        Args:
            bucket_name: Name of the S3 bucket.
            folder_prefix: S3 folder prefix to list files from.
            
        Returns:
            List of file names (without folder prefix).
            
        Raises:
            S3OperationError: If listing fails.
        """
        try:
            response = self.client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=folder_prefix
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    # Skip folder entries, only include actual files
                    if key != folder_prefix and not key.endswith('/'):
                        filename = key.replace(folder_prefix, '', 1)
                        if filename:
                            files.append(filename)
            
            self._log(f"Found {len(files)} files in S3 folder: {folder_prefix}")
            return files
            
        except ClientError as error:
            raise S3OperationError(f"Failed to list files in {folder_prefix}") from error
    
    def download_file(
        self,
        bucket_name: str,
        s3_folder: str,
        filename: str,
        local_dir: Path
    ) -> Path:
        """
        Download a file from S3 to local directory.
        
        Args:
            bucket_name: Name of the S3 bucket.
            s3_folder: S3 folder prefix.
            filename: Name of the file to download.
            local_dir: Local directory to save the file.
            
        Returns:
            Path to the downloaded local file.
            
        Raises:
            S3OperationError: If download fails.
        """
        try:
            local_dir.mkdir(parents=True, exist_ok=True)
            local_path = local_dir / filename
            s3_key = f"{s3_folder}{filename}"
            
            self._log(f"Downloading: {s3_key} -> {local_path}")
            self.resource.meta.client.download_file(
                Bucket=bucket_name,
                Key=s3_key,
                Filename=str(local_path)
            )
            self._log(f"Successfully downloaded: {filename}")
            return local_path
            
        except ClientError as error:
            raise S3OperationError(f"Failed to download {filename}") from error
    
    def upload_file(
        self,
        local_path: Path,
        bucket_name: str,
        s3_folder: str
    ) -> str:
        """
        Upload a local file to S3.
        
        Args:
            local_path: Path to the local file.
            bucket_name: Name of the S3 bucket.
            s3_folder: S3 folder prefix for the upload.
            
        Returns:
            S3 key of the uploaded file.
            
        Raises:
            S3OperationError: If upload fails.
        """
        try:
            s3_key = f"{s3_folder}{local_path.name}"
            
            self._log(f"Uploading: {local_path} -> s3://{bucket_name}/{s3_key}")
            self.resource.meta.client.upload_file(
                Filename=str(local_path),
                Bucket=bucket_name,
                Key=s3_key
            )
            self._log(f"Successfully uploaded: {local_path.name}")
            return s3_key
            
        except ClientError as error:
            raise S3OperationError(f"Failed to upload {local_path.name}") from error
    
    def delete_file(self, bucket_name: str, s3_folder: str, filename: str) -> None:
        """
        Delete a file from S3.
        
        Args:
            bucket_name: Name of the S3 bucket.
            s3_folder: S3 folder prefix.
            filename: Name of the file to delete.
            
        Raises:
            S3OperationError: If deletion fails.
        """
        try:
            s3_key = f"{s3_folder}{filename}"
            self.client.delete_object(Bucket=bucket_name, Key=s3_key)
            self._log(f"Deleted from S3: {s3_key}")
            
        except ClientError as error:
            raise S3OperationError(f"Failed to delete {filename} from S3") from error
    
    def _log(self, message: str) -> None:
        """Log message to both console and logger."""
        print(message)
        self.logger.info(message)


# =============================================================================
# Custom Exceptions
# =============================================================================

class S3OperationError(Exception):
    """Raised when an S3 operation fails."""
    pass


class WebServiceError(Exception):
    """Raised when a web service call fails."""
    pass


class FileProcessingError(Exception):
    """Raised when file processing fails."""
    pass


# =============================================================================
# File Operations
# =============================================================================

def detect_file_encoding(file_path: Path) -> str:
    """
    Detect the encoding of a file using UnicodeDammit.
    
    Args:
        file_path: Path to the file to analyze.
        
    Returns:
        Detected encoding string (e.g., 'utf-8', 'latin-1').
    """
    with open(file_path, 'rb') as file:
        content = file.read()
    return UnicodeDammit(content).original_encoding


def move_file(source_path: Path, dest_dir: Path, logger: logging.Logger) -> Path:
    """
    Move a file to a destination directory.
    
    Args:
        source_path: Path to the source file.
        dest_dir: Destination directory.
        logger: Logger instance.
        
    Returns:
        Path to the moved file.
        
    Raises:
        FileProcessingError: If move operation fails.
    """
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / source_path.name
        
        if source_path.exists():
            shutil.move(str(source_path), str(dest_path))
            message = f"Moved: {source_path} -> {dest_path}"
            print(message)
            logger.info(message)
            return dest_path
        return None
        
    except Exception as error:
        raise FileProcessingError(f"Failed to move {source_path.name}") from error


def delete_local_file(file_path: Path, logger: logging.Logger) -> None:
    """
    Delete a local file.
    
    Args:
        file_path: Path to the file to delete.
        logger: Logger instance.
        
    Raises:
        FileProcessingError: If deletion fails.
    """
    try:
        if file_path.exists():
            file_path.unlink()
            message = f"Deleted local file: {file_path}"
            print(message)
            logger.info(message)
            
    except Exception as error:
        raise FileProcessingError(f"Failed to delete {file_path}") from error


# =============================================================================
# Web Service Operations
# =============================================================================

def call_exchange_rate_service(
    wsdl_url: str,
    username: str,
    password: str,
    csv_record: str
) -> Optional[str]:
    """
    Call the SICS web service to import a CSV record.
    
    Args:
        wsdl_url: URL of the WSDL service definition.
        username: Authentication username.
        password: Authentication password.
        csv_record: Comma-separated record data.
        
    Returns:
        None if successful, error message string if failed.
    """
    data = {
        'genericInput': {
            'interactiveMessageResponses': {
                'answerYes': 'AC0534'
            }
        },
        'importCSVRecords': {
            'csvRecords': csv_record
        }
    }
    
    try:
        session = Session()
        session.auth = HTTPDigestAuth(username, password)
        client = Client(wsdl_url, transport=Transport(session=session))
        client.service.importCSVRecords(**data)
        return None  # Success
        
    except zeep.exceptions.Fault as fault:
        return client.wsdl.types.deserialize(fault.detail[0])['explanation']
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
        return 'SICS server connection attempt has failed. Please try again.'


# =============================================================================
# Exchange Rate Processing
# =============================================================================

@dataclass
class ProcessingResult:
    """Results from processing a single file."""
    
    success_count: int = 0
    error_count: int = 0
    success_file: Optional[Path] = None
    error_file: Optional[Path] = None


def process_exchange_rate_file(
    file_path: Path,
    config: AppConfig,
    s3_handler: S3Handler,
    logger: logging.Logger
) -> ProcessingResult:
    """
    Process a single exchange rate CSV file.
    
    Reads the CSV file, sends each record to the web service,
    and separates results into success and error files.
    
    Args:
        file_path: Path to the CSV file to process.
        config: Application configuration.
        s3_handler: S3 handler instance.
        logger: Logger instance.
        
    Returns:
        ProcessingResult with counts and output file paths.
        
    Raises:
        FileProcessingError: If processing fails.
    """
    result = ProcessingResult()
    success_records = []
    error_records = []
    
    try:
        # Detect file encoding
        encoding = detect_file_encoding(file_path)
        
        # Read CSV file
        df = pd.read_csv(
            file_path,
            dtype=str,
            header=None,
            sep=',',
            encoding=encoding
        )
        
        total_records = len(df)
        message = f"Processing {total_records} records from {file_path.name}"
        print(message)
        logger.info(message)
        
        # Process each row
        for _, row in df.iterrows():
            # Build CSV record string from all columns
            csv_record = ','.join(str(val) for val in row.values)
            
            # Call web service
            error_message = call_exchange_rate_service(
                config.wsdl_url,
                config.ws_username,
                config.ws_password,
                csv_record
            )
            
            if error_message:
                # Add error message as last column
                error_row = list(row.values) + [error_message.split(". ")[0]]
                error_records.append(error_row)
            else:
                success_records.append(list(row.values))
        
        # Save and upload error records
        if error_records:
            result.error_count = len(error_records)
            result.error_file = _save_and_upload_results(
                records=error_records,
                original_filename=file_path.name,
                suffix='_error',
                local_pending_dir=config.local_pending_dir,
                local_dest_dir=config.local_error_dir,
                s3_folder=config.s3_error_folder,
                bucket_name=config.bucket_name,
                encoding=encoding,
                s3_handler=s3_handler,
                logger=logger
            )
            logger.info(f"Processed with errors: {result.error_count} records")
        
        # Save and upload success records
        if success_records:
            result.success_count = len(success_records)
            result.success_file = _save_and_upload_results(
                records=success_records,
                original_filename=file_path.name,
                suffix='_ok',
                local_pending_dir=config.local_pending_dir,
                local_dest_dir=config.local_processed_dir,
                s3_folder=config.s3_processed_folder,
                bucket_name=config.bucket_name,
                encoding=encoding,
                s3_handler=s3_handler,
                logger=logger
            )
            logger.info(f"Processed successfully: {result.success_count} records")
        
        return result
        
    except Exception as error:
        raise FileProcessingError(f"Failed to process {file_path.name}") from error


def _save_and_upload_results(
    records: list,
    original_filename: str,
    suffix: str,
    local_pending_dir: Path,
    local_dest_dir: Path,
    s3_folder: str,
    bucket_name: str,
    encoding: str,
    s3_handler: S3Handler,
    logger: logging.Logger
) -> Path:
    """
    Save records to CSV, upload to S3, and move to destination folder.
    
    Args:
        records: List of record lists to save.
        original_filename: Original input filename.
        suffix: Suffix to add before file extension (e.g., '_error').
        local_pending_dir: Directory where file is initially saved.
        local_dest_dir: Final local destination directory.
        s3_folder: S3 folder for upload.
        bucket_name: S3 bucket name.
        encoding: File encoding to use.
        s3_handler: S3 handler instance.
        logger: Logger instance.
        
    Returns:
        Path to the final local file.
    """
    # Generate output filename
    name_parts = original_filename.rsplit('.', 1)
    if len(name_parts) == 2:
        output_filename = f"{name_parts[0]}{suffix}.{name_parts[1]}"
    else:
        output_filename = f"{original_filename}{suffix}"
    
    # Save to pending directory first
    output_path = local_pending_dir / output_filename
    df = pd.DataFrame(records)
    df.to_csv(output_path, sep=',', header=False, index=False, encoding=encoding)
    
    message = f"Saved {len(records)} records to {output_filename}"
    print(message)
    logger.info(message)
    
    # Upload to S3
    s3_handler.upload_file(output_path, bucket_name, s3_folder)
    
    # Move to final local destination
    final_path = move_file(output_path, local_dest_dir, logger)
    
    return final_path


# =============================================================================
# Main Processing Pipeline
# =============================================================================

def run_exchange_rate_pipeline(config: AppConfig) -> None:
    """
    Execute the complete exchange rate processing pipeline.
    
    Steps:
    1. Download all files from S3 pending folder
    2. Process each file through the web service
    3. Upload results to S3 error/processed folders
    4. Clean up original files from S3 and local storage
    
    Args:
        config: Application configuration.
        
    Raises:
        Exception: If pipeline execution fails.
    """
    logger = setup_logger(config)
    s3_handler = S3Handler(config, logger)
    
    logger.info("XRT process started")
    print("XRT process started")
    
    try:
        # Step 1: List and download all files from S3
        files_to_process = s3_handler.list_files(
            config.bucket_name,
            config.s3_pending_folder
        )
        
        if not files_to_process:
            message = "No files found to process"
            print(message)
            logger.info(message)
            return
        
        # Step 2: Download all files first
        downloaded_files = []
        for filename in files_to_process:
            local_path = s3_handler.download_file(
                config.bucket_name,
                config.s3_pending_folder,
                filename,
                config.local_pending_dir
            )
            downloaded_files.append((filename, local_path))
        
        # Step 3: Process each downloaded file
        for filename, local_path in downloaded_files:
            logger.info(f"Processing file: {filename}")
            print(f"\nProcessing file: {filename}")
            
            # Process the file
            result = process_exchange_rate_file(
                local_path,
                config,
                s3_handler,
                logger
            )
            
            # Log results
            logger.info(
                f"File {filename} completed - "
                f"Success: {result.success_count}, Errors: {result.error_count}"
            )
            
            # Step 4: Delete original file from S3
            s3_handler.delete_file(
                config.bucket_name,
                config.s3_pending_folder,
                filename
            )
            
            # Step 5: Delete original file from local pending directory
            delete_local_file(local_path, logger)
            
            print(f"File {filename} processed and cleaned up")
        
        logger.info("XRT process completed successfully")
        print("\nXRT process completed successfully!")
        
    except Exception as error:
        logger.error(f"XRT process failed: {error}")
        print(f"\nError: {error}")
        print("XRT process finished with error!")
        raise


# =============================================================================
# Entry Point
# =============================================================================

def main() -> None:
    """Application entry point."""
    config = AppConfig()
    run_exchange_rate_pipeline(config)


if __name__ == "__main__":
    main()
