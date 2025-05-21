from .base_generator import BaseGenerator
import pandas as pd
from faker import Faker
import random

class DimensionGenerator(BaseGenerator):
    def __init__(self, schema_path, output_base_path):
        super().__init__(schema_path, output_base_path)
        self.fake = Faker()
        
    def _generate_value(self, col, col_def):
        """Generate a value based on column definition."""
        return super()._generate_value(col, col_def)
        
    def generate_data(self):
        """Generate dimension table data."""
        rows = []
        num_rows = self.schema.get('num_rows', 10)
        
        for i in range(1, num_rows + 1):
            row = {}
            for col, col_def in self.schema['columns'].items():
                if col.endswith('_id'):
                    row[col] = i
                else:
                    row[col] = self._generate_value(col, col_def)
            rows.append(row)
            
        return pd.DataFrame(rows) 