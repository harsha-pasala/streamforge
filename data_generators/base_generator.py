from abc import ABC, abstractmethod
import pandas as pd
import os
from datetime import datetime
import tempfile
import logging
import random

logger = logging.getLogger(__name__)

class BaseGenerator(ABC):
    def __init__(self, schema_path, output_base_path, is_local=True):
        self.schema_path = schema_path
        self.output_base_path = output_base_path
        self.is_local = is_local
        self.schema = self._load_schema()
        
    def _is_local_env(self):
        """Check if running in local environment."""
        return self.is_local
        
    def _load_schema(self):
        """Load schema from YAML file."""
        import yaml
        logger.info(f"Loading schema from: {self.schema_path}")
        
        # If schema_path is None, return an empty schema (used for cleanup operations)
        if self.schema_path is None:
            return {"table": "temp", "columns": []}
            
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
        """Check if directory is empty and clean it up if needed."""
        if self._is_local_env():
            # Local environment check and cleanup
            if os.path.exists(directory):
                if os.listdir(directory):
                    logger.info(f"Directory {directory} is not empty. Cleaning up...")
                    try:
                        import shutil
                        shutil.rmtree(directory)
                        logger.info(f"Successfully cleaned up directory: {directory}")
                    except Exception as e:
                        logger.error(f"Error cleaning up directory {directory}: {str(e)}")
                        raise
            else:
                logger.info(f"Directory {directory} does not exist - will be created when needed")
        else:
            # Databricks environment check and cleanup
            from databricks.sdk import WorkspaceClient
            workspace = WorkspaceClient()
            
            # Remove trailing slash for directory operations
            directory = directory.rstrip('/')
            
            def delete_directory_recursive(dir_path):
                """Recursively delete a directory and its contents."""
                try:
                    # List contents of the directory
                    contents = list(workspace.files.list_directory_contents(dir_path))
                    if contents:
                        for item in contents:
                            if item.is_directory:
                                # Recursively delete subdirectories
                                delete_directory_recursive(item.path)
                            else:
                                # Delete files
                                workspace.files.delete(item.path)
                        # Delete the empty directory
                        workspace.files.delete_directory(dir_path)
                        logger.info(f"Successfully cleaned up directory: {dir_path}")
                except Exception as e:
                    if "not found" not in str(e).lower():
                        logger.error(f"Error cleaning up directory {dir_path}: {str(e)}")
                        raise
            
            try:
                # Check if directory exists and has contents
                contents = list(workspace.files.list_directory_contents(directory))
                if contents:
                    logger.info(f"Directory {directory} is not empty. Cleaning up...")
                    delete_directory_recursive(directory)
            except Exception as e:
                # If directory doesn't exist, that's fine - it will be created
                if "not found" in str(e).lower():
                    logger.info(f"Directory {directory} does not exist - will be created when needed")
                else:
                    logger.error(f"Unexpected error checking directory {directory}: {str(e)}")
                    raise
    
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
        # Check for null probability first
        if isinstance(col_def, dict):
            null_prob = col_def.get('null_probability', 0.0)
            if random.random() < null_prob:
                return None
            
            dtype = col_def.get('type', 'string')
            format_spec = col_def.get('format')
        else:
            # Handle simple type definitions (e.g., "string", "int", etc.)
            dtype = str(col_def).lower()  # Convert to lowercase string
            format_spec = None
            
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
                elif '?' in format_spec:
                    # Handle formats with question marks for random letters
                    result = format_spec
                    while '?' in result:
                        result = result.replace('?', random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 1)
                    return result
                else:
                    # Simple catch-all: return format as-is
                    return format_spec
            elif 'name' in col_lower:
                return self.fake.name()
            elif 'email' in col_lower:
                return self.fake.email()
            elif 'address' in col_lower:
                return self.fake.address()
            elif 'city' in col_lower:
                return self.fake.city()
            elif 'state' in col_lower:
                return self.fake.state()
            elif 'zip' in col_lower:
                return self.fake.zipcode()
            elif ('contact' in col_lower or 'phone' in col_lower) and 'number' in col_lower:
                return f"({self.fake.random_number(digits=3)}) {self.fake.random_number(digits=3)}-{self.fake.random_number(digits=4)}"
            else:
                return self.fake.word().title()
        elif dtype == 'datetime':
            return self.fake.date_time().isoformat()
            
        error_msg = f"Unsupported data type: {dtype}"
        if format_spec:
            error_msg += f" with format: {format_spec}"
        raise ValueError(error_msg)
    
    @abstractmethod
    def generate_data(self):
        """Generate data based on schema. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement generate_data()") 