from .base_generator import BaseGenerator
import pandas as pd
from faker import Faker
import random

class FactGenerator(BaseGenerator):
    def __init__(self, schema_path, output_base_path, dimension_key_ranges):
        super().__init__(schema_path, output_base_path)
        self.fake = Faker()
        self.dimension_key_ranges = dimension_key_ranges
        
    def _generate_value(self, col, col_def):
        """Generate a value based on column definition."""
        if isinstance(col_def, str):
            dtype = col_def
        else:
            dtype = col_def.get('type', 'string')
            
        col_lower = col.lower()
        
        if dtype == 'int':
            return self.fake.random_int(min=1, max=9999)
        elif dtype == 'float':
            return round(random.uniform(0, 1000), 2)
        elif dtype == 'string':
            if 'name' in col_lower:
                return self.fake.name()
            elif 'address' in col_lower:
                return self.fake.address()
            elif 'city' in col_lower:
                return self.fake.city()
            elif 'state' in col_lower:
                return self.fake.state()
            elif 'zip' in col_lower:
                return self.fake.zipcode()
            else:
                return self.fake.word().title()
        elif dtype == 'datetime':
            return self.fake.date_time().isoformat()
        return None
        
    def generate_data(self):
        """Generate fact table data."""
        rows = []
        num_rows = self.schema.get('num_rows', 10)
        
        for _ in range(num_rows):
            row = {}
            for col, col_def in self.schema['columns'].items():
                if col in self.dimension_key_ranges:
                    # Use dimension key ranges for foreign keys
                    row[col] = self.fake.random_int(min=1, max=self.dimension_key_ranges[col])
                else:
                    row[col] = self._generate_value(col, col_def)
            rows.append(row)
            
        return pd.DataFrame(rows) 