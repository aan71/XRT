"""
CED - CEDE Interface Module

This module processes CSV files from a pending directory, inserts records into
the CEDE_INTERFACE database table, and manages file lifecycle (pending -> processed/error).

Author: CED Team
"""

import logging
import os
import secrets
import sys
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from bs4 import UnicodeDammit
import numpy as np
import pandas as pd
import pyodbc
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class Config:
    """
    Configuration settings for the CED process.
    
    Database and AWS credentials are loaded from environment variables for security.
    Set these environment variables before running:
        - CED_DB_SERVER
        - CED_DB_DATABASE
        - CED_DB_USERNAME
        - CED_DB_PASSWORD
        - CED_DB_DRIVER
        - AWS_REGION
        - AWS_ACCESS_KEY_ID
        - AWS_SECRET_ACCESS_KEY
        - AWS_S3_BUCKET
    """
    
    # Logging settings
    log_name: str = "ced.log"
    log_path: Path = field(default_factory=lambda: Path("C:/Temp/"))
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_level: str = "info"  # 'info' or 'debug' (debug enables SQL statement logging)
    
    # Directory paths
    local_pending_dir: Path = field(default_factory=lambda: Path("C:/Temp/CED/Pending/"))
    local_error_dir: Path = field(default_factory=lambda: Path("C:/Temp/CED/Error/"))
    local_processed_dir: Path = field(default_factory=lambda: Path("C:/Temp/CED/Processed/"))
    
    # Database settings loaded from environment variables
    server: str = field(default_factory=lambda: os.environ.get("CED_DB_SERVER", ""))
    database: str = field(default_factory=lambda: os.environ.get("CED_DB_DATABASE", ""))
    username: str = field(default_factory=lambda: os.environ.get("CED_DB_USERNAME", ""))
    password: str = field(default_factory=lambda: os.environ.get("CED_DB_PASSWORD", ""))
    driver: str = field(default_factory=lambda: os.environ.get("CED_DB_DRIVER", ""))
    
    # AWS S3 settings loaded from environment variables
    aws_region: str = field(default_factory=lambda: os.environ.get("AWS_REGION", ""))
    aws_access_key_id: str = field(default_factory=lambda: os.environ.get("AWS_ACCESS_KEY_ID", ""))
    aws_secret_access_key: str = field(default_factory=lambda: os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
    aws_s3_bucket: str = field(default_factory=lambda: os.environ.get("AWS_S3_BUCKET", ""))
    
    # S3 folder paths
    s3_pending_folder: str = "prod/inbound/cede_interface/pending/"
    s3_processed_folder: str = "prod/inbound/cede_interface/processed/"
    s3_error_folder: str = "prod/inbound/cede_interface/error/"
    
    def __post_init__(self) -> None:
        """Validate that required configuration is present."""
        missing = []
        if not self.server:
            missing.append("CED_DB_SERVER")
        if not self.database:
            missing.append("CED_DB_DATABASE")
        if not self.username:
            missing.append("CED_DB_USERNAME")
        if not self.password:
            missing.append("CED_DB_PASSWORD")
        if not self.driver:
            missing.append("CED_DB_DRIVER")
        if not self.aws_region:
            missing.append("AWS_REGION")
        if not self.aws_access_key_id:
            missing.append("AWS_ACCESS_KEY_ID")
        if not self.aws_secret_access_key:
            missing.append("AWS_SECRET_ACCESS_KEY")
        if not self.aws_s3_bucket:
            missing.append("AWS_S3_BUCKET")
        
        if missing:
            raise EnvironmentError(
                f"Missing required environment variables: {', '.join(missing)}. "
                "Please set these variables or create a .env file."
            )
    
    @property
    def connection_string(self) -> str:
        """
        Build and return the database connection string with encryption enabled.
        
        Uses TLS encryption for secure data transmission.
        Note: TrustServerCertificate=yes is used for local/development servers
        with self-signed certificates. For production with proper CA-signed
        certificates, set this to 'no'.
        """
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            "Encrypt=yes;TrustServerCertificate=yes"
        )


# Column definitions for the CEDE_INTERFACE table
COLUMNS_TO_USE = [
    'PID', 'STATUS', 'IMPORT_TS', 'BATCH_NAME', 'CREATION_TS', 'RECORD_TYPE',
    'RECORD_UNIQUE_ID', 'PRIM_SYS', 'ACCESS_CODE', 'BASE_COMPANY', 'EC', 'CURRENCY',
    'DTL_AMT', 'AS_AT', 'OCC_YR', 'UW_YEAR', 'AC_YEAR', 'AC_REF_PERIOD',
    'AC_START_DATE', 'AC_END_DATE', 'DATE_OF_BOOKING', 'BOOKING_YEAR', 'BOOKING_YEAR_2',
    'BOOKING_YEAR_3', 'BOOKING_PERIOD', 'BOOKING_PERIOD2', 'BOOKING_PERIOD3',
    'DTL_COMMENT', 'CEDE', 'WS_IDENTIFIER', 'WS_TITLE', 'IS_ESTIMATE', 'UDF_TXT1',
    'UDF_TXT2', 'UDF_TXT3', 'POLICY_ID', 'POLICY_TITLE', 'POLICY_FORMER_ID',
    'IP_START', 'IP_END', 'ORIGINAL_IP_START', 'ORIGINAL_IP_END', 'AUTOMATIC_PROT_ASS',
    'REASON_FOR_MANPROT', 'SECTION_EXT_ID', 'SECTION_NAME', 'ATT_FROM', 'ATT_TO',
    'SEC_CURRENCY', 'MAIN_LIMIT', 'MAIN_LIMIT_TYPE', 'TOTAL_GROSS_P', 'SHARE_PCT',
    'COUNTRY', 'COUNTRY_GRP', 'STATE', 'STATE_GRP', 'MCOB', 'COB', 'SCOB',
    'ADDL_CLASS_1', 'ADDL_CLASS_2', 'ADDL_CLASS_3', 'ADDL_CLASS_4', 'ADDL_CLASS_5',
    'ADDL_CLASS_6', 'ADDL_CLASS_7', 'ADDL_CLASS_8', 'ADDL_CLASS_9', 'ADDL_CLASS_10',
    'ADDL_CLASS_11', 'REP_UNIT_1', 'REP_UNIT_2', 'REP_UNIT_3', 'ORIGIN_OF_BUS',
    'PERIL', 'CLAIM_ID', 'HL_LOSS_ID', 'HL_LOSS_NAME', 'DOL_BEGIN', 'DOL_END',
    'INCL_IN_REC_ORDER', 'ACC_AS_OF_DATE', 'CAUSE_OF_LOSS', 'CONSEQUENCE_OF_LOSS',
    'CLAIM_NAME', 'RISKNAME', 'CLAIM_UDF_TXT1', 'CLAIM_UDF_TXT2', 'CLAIM_UDF_TXT3',
    'PC_LIMIT_INFO1', 'PC_LIMIT_INFO1_TYPE', 'PC_LIMIT_INFO2', 'PC_LIMIT_INFO2_TYPE',
    'PC_DECL_ID', 'PC_DECL_NAME', 'PC_DECL_ATT_FROM', 'PC_DECL_ATT_TO', 'PC_DECL_MAIN_L',
    'PC_DECL_CURR', 'PC_INSURED_ID', 'PC_INSURED_NAME', 'PC_RUG', 'PC_RUG_NAME',
    'PC_RUG_SEQUENT', 'PC_IO_ID', 'PC_IO_NAME', 'PC_IO_TYPE', 'PC_CLAIM_BASIS',
    'LF_TRANS_TYPE', 'LF_SAR', 'LF_XTRA_MORTAL_PCT', 'LF_OTHER_XTRA_PREM',
    'LF_CALC_BASIS', 'LF_AGE', 'LF_RETIREMENT_AGE', 'LF_SMOKER_STATUS',
    'LF_OCCUPATION_CLS', 'LF_RISK_CLASS', 'LF_DISABILITY_CLS', 'LF_ESCALATION',
    'LF_IAB_IDENTIFIER', 'LF_IAB_BEGIN_DATE', 'LF_IO_PERSON_ID', 'LF_IO_PERSON_NAME',
    'LF_IO_ALIAS', 'LF_IO_DT_OF_BIRTH', 'LF_IO_BIRTH_COUNTRY', 'LF_IO_NATIONALITY',
    'LF_IO_PERSON_STATUS', 'LF_IO_GENDER', 'REASON_FOR_CHANGE', 'CLAIM_ADVISED_DT',
    'AUTO_LGT_CL_PT_ASS', 'REA_LGT_CL_MANPT', 'UDF_PCT1', 'UDF_PCT2', 'UDF_PCT3',
    'UDF_PCT4', 'UDF_TXT4', 'FSK_UDF_REF_DATA', 'FRK_UDF_REF_DATA', 'UDF_AMT1_AMT',
    'FK_UDF_AMT1_CY', 'UDF_AMT2_AMT', 'FK_UDF_AMT2_CY', 'UDF_AMT3_AMT', 'FK_UDF_AMT3_CY',
    'UDF_AMT4_AMT', 'FK_UDF_AMT4_CY', 'UDF_AMT5_AMT', 'FK_UDF_AMT5_CY', 'CLMS_TRIG_DT',
    'CLAIMS_TRIG', 'CLAIMANT', 'PC_ASSISTANCE_KEY', 'PC_NUM_OF_INSRD_OBJ',
    'CLAIM_STATUS', 'HL_DOL_BEGIN', 'HL_DOL_END', 'PC_RUG_PROP_GRP',
    'PC_RUG_AUTOMATIC_PROT_ASS', 'PC_RUG_TOP_LOCATION', 'PC_RUG_REASON_FOR_MANPROT',
    'PC_ACCU_EXCL', 'PC_PA_INHERIT_EXCL', 'CLAIM_SHORT_DESC', 'FK_ORDER',
    'FK_LIGHT_DETAIL', 'VERSION'
]

# Column order for INSERT statement (different from CSV column order)
INSERT_COLUMN_ORDER = [
    'IMPORT_TS', 'CREATION_TS', 'RECORD_TYPE', 'DTL_AMT', 'AS_AT', 'OCC_YR',
    'UW_YEAR', 'AC_YEAR', 'AC_START_DATE', 'AC_END_DATE', 'DATE_OF_BOOKING',
    'BOOKING_YEAR', 'BOOKING_YEAR_2', 'BOOKING_YEAR_3', 'CEDE', 'IS_ESTIMATE',
    'IP_START', 'IP_END', 'ORIGINAL_IP_START', 'ORIGINAL_IP_END', 'AUTOMATIC_PROT_ASS',
    'ATT_FROM', 'ATT_TO', 'MAIN_LIMIT', 'TOTAL_GROSS_P', 'SHARE_PCT', 'DOL_BEGIN',
    'DOL_END', 'INCL_IN_REC_ORDER', 'ACC_AS_OF_DATE', 'PC_LIMIT_INFO1', 'PC_LIMIT_INFO2',
    'PC_DECL_ATT_FROM', 'PC_DECL_ATT_TO', 'PC_DECL_MAIN_L', 'PC_RUG_SEQUENT', 'LF_SAR',
    'LF_XTRA_MORTAL_PCT', 'LF_OTHER_XTRA_PREM', 'LF_AGE', 'LF_RETIREMENT_AGE',
    'LF_IAB_BEGIN_DATE', 'LF_IO_DT_OF_BIRTH', 'CLAIM_ADVISED_DT', 'AUTO_LGT_CL_PT_ASS',
    'UDF_PCT1', 'UDF_PCT2', 'UDF_PCT3', 'UDF_PCT4', 'FSK_UDF_REF_DATA', 'UDF_AMT1_AMT',
    'FK_UDF_AMT1_CY', 'UDF_AMT2_AMT', 'FK_UDF_AMT2_CY', 'UDF_AMT3_AMT', 'FK_UDF_AMT3_CY',
    'UDF_AMT4_AMT', 'FK_UDF_AMT4_CY', 'UDF_AMT5_AMT', 'FK_UDF_AMT5_CY', 'CLMS_TRIG_DT',
    'PC_NUM_OF_INSRD_OBJ', 'HL_DOL_BEGIN', 'HL_DOL_END', 'PC_RUG_PROP_GRP',
    'PC_RUG_AUTOMATIC_PROT_ASS', 'PC_RUG_TOP_LOCATION', 'PC_ACCU_EXCL',
    'PC_PA_INHERIT_EXCL', 'VERSION', 'PID', 'STATUS', 'BATCH_NAME', 'RECORD_UNIQUE_ID',
    'PRIM_SYS', 'ACCESS_CODE', 'BASE_COMPANY', 'EC', 'CURRENCY', 'AC_REF_PERIOD',
    'BOOKING_PERIOD', 'BOOKING_PERIOD2', 'BOOKING_PERIOD3', 'DTL_COMMENT',
    'WS_IDENTIFIER', 'WS_TITLE', 'UDF_TXT1', 'UDF_TXT2', 'UDF_TXT3', 'POLICY_ID',
    'POLICY_TITLE', 'POLICY_FORMER_ID', 'REASON_FOR_MANPROT', 'SECTION_EXT_ID',
    'SECTION_NAME', 'SEC_CURRENCY', 'MAIN_LIMIT_TYPE', 'COUNTRY', 'COUNTRY_GRP',
    'STATE', 'STATE_GRP', 'MCOB', 'COB', 'SCOB', 'ADDL_CLASS_1', 'ADDL_CLASS_2',
    'ADDL_CLASS_3', 'ADDL_CLASS_4', 'ADDL_CLASS_5', 'ADDL_CLASS_6', 'ADDL_CLASS_7',
    'ADDL_CLASS_8', 'ADDL_CLASS_9', 'ADDL_CLASS_10', 'ADDL_CLASS_11', 'REP_UNIT_1',
    'REP_UNIT_2', 'REP_UNIT_3', 'ORIGIN_OF_BUS', 'PERIL', 'CLAIM_ID', 'HL_LOSS_ID',
    'HL_LOSS_NAME', 'CAUSE_OF_LOSS', 'CONSEQUENCE_OF_LOSS', 'CLAIM_NAME', 'RISKNAME',
    'CLAIM_UDF_TXT1', 'CLAIM_UDF_TXT2', 'CLAIM_UDF_TXT3', 'PC_LIMIT_INFO1_TYPE',
    'PC_LIMIT_INFO2_TYPE', 'PC_DECL_ID', 'PC_DECL_NAME', 'PC_DECL_CURR',
    'PC_INSURED_ID', 'PC_INSURED_NAME', 'PC_RUG', 'PC_RUG_NAME', 'PC_IO_ID',
    'PC_IO_NAME', 'PC_IO_TYPE', 'PC_CLAIM_BASIS', 'LF_TRANS_TYPE', 'LF_CALC_BASIS',
    'LF_SMOKER_STATUS', 'LF_OCCUPATION_CLS', 'LF_RISK_CLASS', 'LF_DISABILITY_CLS',
    'LF_ESCALATION', 'LF_IAB_IDENTIFIER', 'LF_IO_PERSON_ID', 'LF_IO_PERSON_NAME',
    'LF_IO_ALIAS', 'LF_IO_BIRTH_COUNTRY', 'LF_IO_NATIONALITY', 'LF_IO_PERSON_STATUS',
    'LF_IO_GENDER', 'REASON_FOR_CHANGE', 'REA_LGT_CL_MANPT', 'UDF_TXT4',
    'FRK_UDF_REF_DATA', 'CLAIMS_TRIG', 'CLAIMANT', 'PC_ASSISTANCE_KEY', 'CLAIM_STATUS',
    'PC_RUG_REASON_FOR_MANPROT', 'CLAIM_SHORT_DESC', 'FK_ORDER', 'FK_LIGHT_DETAIL'
]

# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logger(config: Config) -> logging.Logger:
    """
    Configure and return a logger instance with cryptographically secure unique identifier.
    
    Args:
        config: Configuration object containing logging settings.
        
    Returns:
        Configured logger instance.
    """
    logging.basicConfig(
        level=logging.INFO,
        filename=str(config.log_path / config.log_name),
        format=config.log_format
    )
    # Use cryptographically secure random identifier
    unique_id = secrets.token_hex(8)
    return logging.getLogger(f'CED_{unique_id}')


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def format_sql_for_debug(sql: str, values: tuple) -> str:
    """
    Format SQL statement with actual values for debugging purposes.
    
    Args:
        sql: SQL statement with ? placeholders.
        values: Tuple of values to substitute.
        
    Returns:
        SQL string with values substituted for placeholders.
    """
    placeholder = "%PARAMETER%"
    formatted_sql = sql.replace("?", placeholder)
    for value in values:
        formatted_sql = formatted_sql.replace(placeholder, repr(value), 1)
    return formatted_sql


def detect_file_encoding(file_path: Path) -> str:
    """
    Detect the encoding of a file using UnicodeDammit.
    
    Args:
        file_path: Path to the file to analyze.
        
    Returns:
        Detected encoding string.
    """
    with open(file_path, 'rb') as file:
        content = file.read()
    return UnicodeDammit(content).original_encoding


def build_insert_statement() -> str:
    """
    Build the INSERT SQL statement dynamically from column definitions.
    
    Returns:
        Complete INSERT INTO statement with placeholders.
    """
    columns = ', '.join(INSERT_COLUMN_ORDER)
    placeholders = ', '.join(['?'] * len(INSERT_COLUMN_ORDER))
    return f"INSERT INTO CEDE_INTERFACE ({columns}) VALUES ({placeholders})"


def get_pending_files(directory: Path) -> list[Path]:
    """
    Get list of files in the pending directory with path traversal protection.
    
    Args:
        directory: Path to the pending directory.
        
    Returns:
        List of file paths that are safely within the directory.
    """
    base_path = directory.resolve()
    safe_files = []
    
    for file_path in directory.iterdir():
        resolved_path = file_path.resolve()
        # Ensure file is within the expected directory (prevent path traversal)
        try:
            resolved_path.relative_to(base_path)
            if resolved_path.is_file():
                safe_files.append(resolved_path)
        except ValueError:
            # Path is outside base directory - skip it
            logging.warning(f"Skipping file outside base directory: {file_path}")
            continue
    
    return safe_files


def sanitize_error_message(error: Exception) -> str:
    """
    Sanitize error message to remove sensitive internal details.
    
    Args:
        error: The exception to sanitize.
        
    Returns:
        Sanitized error message safe for logging/output.
    """
    error_str = str(error)
    # Remove file paths
    error_str = error_str.replace("\\", "/")
    # Remove common sensitive patterns
    sensitive_patterns = [
        (r"C:/[^\s;'\"]+", "[PATH]"),
        (r"password[=:][^\s;'\"]+", "password=[REDACTED]"),
        (r"pwd[=:][^\s;'\"]+", "pwd=[REDACTED]"),
    ]
    import re
    for pattern, replacement in sensitive_patterns:
        error_str = re.sub(pattern, replacement, error_str, flags=re.IGNORECASE)
    return error_str


def validate_csv_columns(dataframe: pd.DataFrame, required_columns: list[str]) -> None:
    """
    Validate that the CSV file contains all required columns.
    
    Args:
        dataframe: The loaded DataFrame to validate.
        required_columns: List of column names that must be present.
        
    Raises:
        ValueError: If required columns are missing.
    """
    missing_columns = set(required_columns) - set(dataframe.columns)
    if missing_columns:
        raise ValueError(f"CSV file is missing required columns: {sorted(missing_columns)}")


# =============================================================================
# S3 OPERATIONS
# =============================================================================

class S3Manager:
    """Handles Amazon S3 operations for file transfer."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        """
        Initialize the S3 manager.
        
        Args:
            config: Configuration object with AWS credentials.
            logger: Logger instance.
        """
        self.config = config
        self.logger = logger
        self.s3_client = boto3.client(
            's3',
            region_name=config.aws_region,
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key
        )
    
    def list_pending_files(self) -> list[str]:
        """
        List all files in the S3 pending folder.
        
        Returns:
            List of S3 object keys (file paths) in the pending folder.
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.config.aws_s3_bucket,
                Prefix=self.config.s3_pending_folder
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    # Skip the folder itself (ends with /)
                    if not key.endswith('/'):
                        files.append(key)
            
            return files
            
        except ClientError as e:
            self.logger.error(f"Error listing S3 files: {e}")
            raise
    
    def download_file(self, s3_key: str, local_path: Path) -> None:
        """
        Download a file from S3 to local directory.
        
        Args:
            s3_key: The S3 object key (path) to download.
            local_path: Local file path to save the downloaded file.
        """
        try:
            self.s3_client.download_file(
                self.config.aws_s3_bucket,
                s3_key,
                str(local_path)
            )
            self._log_and_print(f"Downloaded: {s3_key} -> {local_path}")
            
        except ClientError as e:
            self.logger.error(f"Error downloading {s3_key}: {e}")
            raise
    
    def upload_file(self, local_path: Path, s3_key: str) -> None:
        """
        Upload a file from local directory to S3.
        
        Args:
            local_path: Local file path to upload.
            s3_key: The S3 object key (path) to upload to.
        """
        try:
            self.s3_client.upload_file(
                str(local_path),
                self.config.aws_s3_bucket,
                s3_key
            )
            self._log_and_print(f"Uploaded: {local_path} -> s3://{self.config.aws_s3_bucket}/{s3_key}")
            
        except ClientError as e:
            self.logger.error(f"Error uploading {local_path}: {e}")
            raise
    
    def delete_file(self, s3_key: str) -> None:
        """
        Delete a file from S3.
        
        Args:
            s3_key: The S3 object key (path) to delete.
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.config.aws_s3_bucket,
                Key=s3_key
            )
            self._log_and_print(f"Deleted from S3: {s3_key}")
            
        except ClientError as e:
            self.logger.error(f"Error deleting {s3_key}: {e}")
            raise
    
    def download_all_pending_files(self) -> list[tuple[Path, str]]:
        """
        Download all files from S3 pending folder to local pending directory.
        
        Returns:
            List of tuples containing (local_path, s3_key) for each downloaded file.
        """
        pending_files = self.list_pending_files()
        downloaded = []
        
        if not pending_files:
            self._log_and_print("No files found in S3 pending folder.")
            return downloaded
        
        self._log_and_print(f"Found {len(pending_files)} file(s) in S3 pending folder.")
        
        # Ensure local pending directory exists
        self.config.local_pending_dir.mkdir(parents=True, exist_ok=True)
        
        for s3_key in pending_files:
            # Extract filename from S3 key
            filename = Path(s3_key).name
            local_path = self.config.local_pending_dir / filename
            
            self.download_file(s3_key, local_path)
            downloaded.append((local_path, s3_key))
        
        return downloaded
    
    def upload_result_file(self, local_path: Path, is_error: bool) -> None:
        """
        Upload a result file (_ok or _error) to the appropriate S3 folder.
        
        Args:
            local_path: Local path to the result file.
            is_error: True if this is an error file, False if success file.
        """
        filename = local_path.name
        
        if is_error:
            s3_key = self.config.s3_error_folder + filename
        else:
            s3_key = self.config.s3_processed_folder + filename
        
        self.upload_file(local_path, s3_key)
    
    def _log_and_print(self, message: str) -> None:
        """Log message and print to stdout."""
        print(message)
        self.logger.info(message)


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

class DatabaseProcessor:
    """Handles database operations for the CED process."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        """
        Initialize the database processor.
        
        Args:
            config: Configuration object.
            logger: Logger instance.
        """
        self.config = config
        self.logger = logger
        self.insert_statement = build_insert_statement()
    
    def execute_insert(self, cursor: pyodbc.Cursor, data: tuple) -> Optional[str]:
        """
        Execute a single INSERT statement.
        
        Args:
            cursor: Database cursor.
            data: Tuple of values to insert.
            
        Returns:
            None on success, sanitized error message on failure.
        """
        try:
            cursor.execute(self.insert_statement, data)
            return None
        except pyodbc.Error as error:
            # Return sanitized error message
            return sanitize_error_message(error)
    
    def process_file(self, file_path: Path) -> bool:
        """
        Process a single CSV file and insert records into the database.
        
        Args:
            file_path: Path to the CSV file to process.
            
        Returns:
            True if all records processed successfully, False if any errors occurred.
            
        Raises:
            ValueError: If CSV file is missing required columns.
            pyodbc.Error: If database connection fails.
        """
        file_name = file_path.name
        encoding = detect_file_encoding(file_path)
        
        # Read CSV file
        dataframe = pd.read_csv(
            file_path,
            sep=';',
            encoding=encoding,
            dtype=str
        )
        
        # Validate CSV has required columns
        validate_csv_columns(dataframe, INSERT_COLUMN_ORDER)
        
        dataframe = dataframe.replace({np.nan: None})
        
        total_records = len(dataframe)
        self._log_and_print(f'Total record(s) to process: {total_records}')
        sys.stdout.flush()
        
        # Track successful and failed records
        successful_records: list[list] = []
        failed_records: list[list] = []
        
        # Process records
        try:
            with pyodbc.connect(self.config.connection_string) as connection:
                cursor = connection.cursor()
                cursor.fast_executemany = True
                
                for _, row in dataframe.iterrows():
                    # Build data tuple in the correct column order
                    data = tuple(row[col] for col in INSERT_COLUMN_ORDER)
                    
                    # Log SQL in debug mode (WARNING: may contain sensitive data)
                    # Only enable in development environments
                    if self.config.log_level == 'debug':
                        self.logger.debug("Executing INSERT statement (values redacted for security)")
                    
                    # Execute insert
                    error = self.execute_insert(cursor, data)
                    
                    if error is not None:
                        # Record failed - add to error list with sanitized error message
                        failed_records.append(list(row.values) + [error])
                    else:
                        # Record succeeded
                        successful_records.append(list(row.values))
                
                connection.commit()
                
        except pyodbc.Error as db_error:
            self.logger.error(f"Database connection error: {sanitize_error_message(db_error)}")
            raise
        
        # Write output files
        self._write_error_file(file_name, failed_records, encoding)
        self._write_success_file(file_name, successful_records, encoding)
        
        # Return True only if all records succeeded
        return len(failed_records) == 0
    
    def _write_error_file(
        self,
        original_filename: str,
        records: list[list],
        encoding: str
    ) -> None:
        """Write failed records to error file."""
        if not records:
            return
        
        error_filename = original_filename.replace('.', '_error.')
        error_path = self.config.local_error_dir / error_filename
        
        # Add header row
        records.insert(0, COLUMNS_TO_USE + ['ERROR'])
        
        error_df = pd.DataFrame(records)
        error_df.to_csv(
            error_path,
            sep=';',
            mode='w',
            header=False,
            index=False,
            encoding=encoding
        )
        
        record_count = len(records) - 1  # Exclude header
        self._log_and_print(f"Processed with errors: {record_count} records")
    
    def _write_success_file(
        self,
        original_filename: str,
        records: list[list],
        encoding: str
    ) -> None:
        """Write successful records to processed file."""
        if not records:
            return
        
        success_filename = original_filename.replace('.', '_ok.')
        success_path = self.config.local_processed_dir / success_filename
        
        # Add header row
        records.insert(0, COLUMNS_TO_USE)
        
        success_df = pd.DataFrame(records)
        success_df.to_csv(
            success_path,
            sep=';',
            mode='w',
            header=False,
            index=False,
            encoding=encoding
        )
        
        record_count = len(records) - 1  # Exclude header
        self._log_and_print(f"Processed successfully: {record_count} records")
    
    def _log_and_print(self, message: str) -> None:
        """Log message and print to stdout."""
        print(message)
        self.logger.info(message)


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main() -> None:
    """Main entry point for the CED process."""
    # Suppress warnings
    warnings.filterwarnings("ignore")
    
    # Initialize configuration and logger
    config = Config()
    logger = setup_logger(config)
    
    logger.info('CED process has been created.')
    print('CED process has been created.')
    
    try:
        # Initialize S3 manager
        s3_manager = S3Manager(config, logger)
        
        # Step 1: Download all files from S3 pending folder to local pending directory
        print("\n--- Step 1: Downloading files from S3 ---")
        logger.info("Step 1: Downloading files from S3")
        downloaded_files = s3_manager.download_all_pending_files()
        
        if not downloaded_files:
            print('CED process has finished - no files to process.')
            logger.info('CED process has finished - no files to process.')
            return
        
        # Step 2: Process downloaded files
        print(f"\n--- Step 2: Processing {len(downloaded_files)} file(s) ---")
        logger.info(f"Step 2: Processing {len(downloaded_files)} file(s)")
        
        # Initialize database processor
        processor = DatabaseProcessor(config, logger)
        
        # Process each file
        for local_path, s3_key in downloaded_files:
            print(f'\nProcessing file: {local_path.name}')
            logger.info(f'Processing file: {local_path.name}')
            
            try:
                # Process the file and insert records into database
                all_records_succeeded = processor.process_file(local_path)
                
                # Step 3: Upload result files to S3
                print(f"\n--- Step 3: Uploading results for {local_path.name} ---")
                logger.info(f"Step 3: Uploading results for {local_path.name}")
                
                # Upload success file if exists
                success_filename = local_path.name.replace('.', '_ok.')
                success_path = config.local_processed_dir / success_filename
                if success_path.exists():
                    s3_manager.upload_result_file(success_path, is_error=False)
                
                # Upload error file if exists
                error_filename = local_path.name.replace('.', '_error.')
                error_path = config.local_error_dir / error_filename
                if error_path.exists():
                    s3_manager.upload_result_file(error_path, is_error=True)
                
                # Step 4: Delete original file from S3 pending folder
                print(f"\n--- Step 4: Cleaning up {local_path.name} ---")
                logger.info(f"Step 4: Cleaning up {local_path.name}")
                s3_manager.delete_file(s3_key)
                
                # Delete local pending file
                local_path.unlink()
                
                if all_records_succeeded:
                    print(f'File {local_path.name} processed successfully.')
                    logger.info(f'File {local_path.name} processed successfully.')
                else:
                    print(f'File {local_path.name} processed with some errors.')
                    logger.warning(f'File {local_path.name} processed with some errors.')
                
            except ValueError as validation_error:
                # CSV validation errors
                print(f'Validation error in file {local_path.name}: {validation_error}')
                logger.error(f'Validation error in file {local_path.name}: {validation_error}')
                continue
                
            except pyodbc.Error as db_error:
                # Database errors
                sanitized_msg = sanitize_error_message(db_error)
                print(f'Database error processing file {local_path.name}: {sanitized_msg}')
                logger.error(f'Database error processing file {local_path.name}: {sanitized_msg}')
                continue
                
            except ClientError as s3_error:
                # S3 errors
                print(f'S3 error with file {local_path.name}: {s3_error}')
                logger.error(f'S3 error with file {local_path.name}: {s3_error}')
                continue
                
            except OSError as file_error:
                # File system errors
                print(f'File system error with {local_path.name}: {file_error}')
                logger.error(f'File system error with {local_path.name}: {file_error}')
                continue
        
        print('\n=== CED process has finished successfully! ===')
        logger.info('CED process has finished successfully!')
        
    except Exception as error:
        print(f'Error: {error}')
        print('CED process has finished with error!')
        logger.error(error)
        logger.info('CED process has finished with error!')


if __name__ == '__main__':
    main()
