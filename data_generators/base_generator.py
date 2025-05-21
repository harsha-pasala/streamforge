from abc import ABC, abstractmethod
import pandas as pd
import os
from datetime import datetime
import tempfile
from flask import request
import logging
import random

logger = logging.getLogger(__name__)

class BaseGenerator(ABC):
    def __init__(self, schema_path, output_base_path):
        self.schema_path = schema_path
        self.output_base_path = output_base_path
        self.schema = self._load_schema()
        
    def _is_local_env(self):
        """Check if running in local environment by checking request headers."""
        try:
            # Get the actual URL from request headers
            host = request.headers.get('X-Forwarded-Host', request.host)
            referer = request.headers.get('Referer', '')
            
            logger.info(f"Detected host: {host}")
            logger.info(f"Detected referer: {referer}")
            
            # Check if we're running on Databricks domain
            is_databricks = any([
                'databricks' in host,
                'databricks' in referer
            ])
            
            logger.info(f"Environment detection result: {'Databricks' if is_databricks else 'Local'}")
            return not is_databricks
        except Exception as e:
            logger.info(f"Error during environment detection - defaulting to local. Error: {str(e)}")
            return True
        
    def _load_schema(self):
        """Load schema from YAML file."""
        import yaml
        logger.info(f"Loading schema from: {self.schema_path}")
        
        try:
            with open(self.schema_path) as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading schema from file: {str(e)}")
            raise
    
    def _get_output_path(self, table_name):
        """Generate output path for the generated data."""
        if self._is_local_env():
            # Local environment: use standard path joining
            table_dir = os.path.join(self.output_base_path, os.path.basename(os.path.dirname(self.schema_path)), table_name)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            return os.path.join(table_dir, f"data_{timestamp}.csv")
        else:
            # Databricks environment: ensure path starts with /Volumes/
            if not self.output_base_path.startswith('/Volumes/'):
                logger.warning("Databricks path should start with /Volumes/. Adding prefix.")
                self.output_base_path = f"/Volumes/{self.output_base_path.lstrip('/')}"
            
            # Use forward slashes for Databricks paths
            table_dir = f"{self.output_base_path}/{os.path.basename(os.path.dirname(self.schema_path))}/{table_name}"
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            return f"{table_dir}/data_{timestamp}.csv"
    
    def _check_directory_empty(self, directory):
        """Check if directory is empty in both local and Databricks environments."""
        if self._is_local_env():
            # Local environment check
            if os.path.exists(directory):
                if os.listdir(directory):
                    raise ValueError(f"Directory {directory} is not empty. Please clear the directory before generating new data.")
        else:
            # Databricks environment check
            from databricks.sdk import WorkspaceClient
            workspace = WorkspaceClient()
            try:
                # List files in the directory
                files = workspace.files.list(directory)
                if files and len(files) > 0:
                    raise ValueError(f"Directory {directory} is not empty. Please clear the directory before generating new data.")
            except Exception as e:
                # If directory doesn't exist, that's fine - it will be created
                if "does not exist" not in str(e):
                    raise e
    
    def _save_to_databricks(self, df, output_path):
        """Save data to Databricks UC volume using SDK."""
        from databricks.sdk import WorkspaceClient
        workspace = WorkspaceClient()
        
        print(f"DEBUG - Full output path being used: {output_path}")
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            df.to_csv(temp_file.name, index=False)
            
            # Use Databricks SDK to write to UC volume
            with open(temp_file.name, 'rb') as f:
                workspace.files.upload(
                    file_path=output_path,
                    contents=f.read(),
                    overwrite=True
                )
            
            # Clean up the temporary file
            os.unlink(temp_file.name)
    
    def save_data(self, df, table_name):
        """Save generated data to CSV file."""
        output_path = self._get_output_path(table_name)
        output_dir = os.path.dirname(output_path)
        
        logger.info(f"Saving data for table {table_name}")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Full output path: {output_path}")
        logger.info(f"Environment: {'Local' if self._is_local_env() else 'Databricks'}")
        
        if self._is_local_env():
            # Local development: create directory and save directly
            logger.info("Local environment detected - using direct file operations")
            try:
                logger.info(f"Creating directory: {output_dir}")
                os.makedirs(output_dir, exist_ok=True)
                logger.info(f"Writing data to: {output_path}")
                df.to_csv(output_path, index=False)
                logger.info("Data saved successfully")
            except Exception as e:
                logger.error(f"Error saving data locally: {str(e)}")
                raise
        else:
            # Databricks deployment: use SDK to write to UC volume
            logger.info("Databricks environment detected - using SDK")
            try:
                self._save_to_databricks(df, output_path)
                logger.info("Data saved successfully via Databricks SDK")
            except Exception as e:
                logger.error(f"Error saving data via Databricks SDK: {str(e)}")
                raise
            
        logger.info(f"Generated file: {output_path}")
        return output_path
    
    def _generate_value(self, col, col_def):
        """Base method for generating values based on data type."""
        if isinstance(col_def, str):
            dtype = col_def
            format_spec = None
        else:
            dtype = col_def.get('type', 'string')
            format_spec = col_def.get('format')
            
        col_lower = col.lower()
        
        # Handle basic data types
        if dtype == 'int':
            return random.randint(1, 9999)
        elif dtype == 'float':
            return round(random.uniform(0, 1000), 2)
        elif dtype == 'bool':
            return random.choice([True, False])
        elif dtype == 'string':
            if format_spec:
                if '|' in format_spec:
                    # Handle pipe-separated formats (e.g., "RES|COM|IND")
                    return random.choice(format_spec.split('|'))
                elif '#' in format_spec:
                    # Handle formats with hash symbols for random digits
                    result = format_spec
                    while '#' in result:
                        result = result.replace('#', str(random.randint(0, 9)), 1)
                    return result
            elif 'name' in col_lower:
                return self.fake.name()
            elif 'address' in col_lower:
                return self.fake.address()
            elif 'city' in col_lower:
                return self.fake.city()
            elif 'state' in col_lower:
                return self.fake.state()
            elif 'zip' in col_lower:
                return self.fake.zipcode()
            elif 'contact' in col_lower and 'number' in col_lower:
                # Generate a standard 10-digit phone number
                return f"({self.fake.random_number(digits=3)}) {self.fake.random_number(digits=3)}-{self.fake.random_number(digits=4)}"
            elif 'email' in col_lower:
                return self.fake.email()
            else:
                return self.fake.word().title()
        elif dtype == 'datetime':
            return self.fake.date_time().isoformat()
            
        raise ValueError(f"Unsupported data type: {dtype}")
    
    @abstractmethod
    def generate_data(self):
        """Generate data based on schema. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement generate_data()") 