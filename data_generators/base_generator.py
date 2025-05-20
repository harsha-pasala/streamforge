from abc import ABC, abstractmethod
import pandas as pd
import os
from datetime import datetime
import tempfile
from flask import request

class BaseGenerator(ABC):
    def __init__(self, schema_path, output_base_path):
        self.schema_path = schema_path
        self.output_base_path = output_base_path
        self.schema = self._load_schema()
        self.workspace = None
        
    def _is_local_env(self):
        """Check if running in local environment by checking request URL."""
        try:
            host = request.host
            return '127.0.0.1' in host or 'localhost' in host
        except Exception:
            # If we can't determine host, assume we're local
            return True
        
    def _load_schema(self):
        """Load schema from YAML file."""
        import yaml
        with open(self.schema_path) as f:
            return yaml.safe_load(f)
    
    def _get_output_path(self, table_name):
        """Generate output path for the generated data."""
        table_dir = os.path.join(self.output_base_path, os.path.basename(os.path.dirname(self.schema_path)), table_name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(table_dir, f"data_{timestamp}.csv")
    
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
        
        if self._is_local_env():
            # Local development: create directory and save directly
            os.makedirs(output_dir, exist_ok=True)
            df.to_csv(output_path, index=False)
        else:
            # Databricks deployment: use SDK to write to UC volume
            self._save_to_databricks(df, output_path)
            
        print(f"Generated file: {output_path}")
        return output_path
    
    @abstractmethod
    def generate_data(self):
        """Generate data based on schema. Must be implemented by subclasses."""
        pass 