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
        if isinstance(col_def, str):
            dtype = col_def
            format_spec = None
        else:
            dtype = col_def.get('type', 'string')
            format_spec = col_def.get('format')
            
        col_lower = col.lower()
        
        if dtype == 'int':
            return self.fake.random_int(min=1, max=9999)
        elif dtype == 'float':
            return round(random.uniform(0, 1000), 2)
        elif dtype == 'string':
            if format_spec:
                if '|' in format_spec:
                    # Handle pipe-separated formats (e.g., "RES|COM|IND")
                    return random.choice(format_spec.split('|'))
                elif '#' in format_spec:
                    # Handle formats with hash symbols for random digits (e.g., "RATE-##", "MTR-####-####")
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
            else:
                return self.fake.word().title()
        elif dtype == 'datetime':
            return self.fake.date_time().isoformat()
        return None
        
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