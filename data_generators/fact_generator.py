from .base_generator import BaseGenerator
import pandas as pd
from faker import Faker
import random

class FactGenerator(BaseGenerator):
    def __init__(self, schema_path, output_base_path, dimension_key_ranges, is_local=True):
        super().__init__(schema_path, output_base_path, is_local=is_local)
        self.fake = Faker()
        self.dimension_key_ranges = dimension_key_ranges
        
    def _generate_value(self, col, col_def):
        """Generate a value based on column definition."""
        # Check if there are data quality rules for this column
        if 'data_quality_rules' in self.schema and col in self.schema['data_quality_rules']:
            rules = self.schema['data_quality_rules'][col]
            min_value = rules.get('min_value')
            max_value = rules.get('max_value')
            anomaly_percentage = rules.get('anomaly_percentage', 0)
            
            # Randomly decide if this value should be an anomaly
            if random.random() < anomaly_percentage:
                # Generate an anomalous value outside the normal range
                if random.random() < 0.5:  # 50% chance of being below min
                    value = min_value - random.uniform(0.1, 0.3)  # 10-30% below min
                else:  # 50% chance of being above max
                    value = max_value + random.uniform(0.1, 0.3)  # 10-30% above max
            else:
                # Generate a normal value within the range
                value = random.uniform(min_value, max_value)
            
            # Round to 2 decimal places for float values
            if isinstance(value, float):
                value = round(value, 2)
                
            return value
            
        # Use base implementation if no quality rules
        return super()._generate_value(col, col_def)

    def _generate_value_with_quality_rules(self, col, col_def):
        """Generate a value considering data quality rules if they exist."""
        value = self._generate_value(col, col_def)
        
        # Check if there are data quality rules for this column
        if 'data_quality_rules' in self.schema and col in self.schema['data_quality_rules']:
            rules = self.schema['data_quality_rules'][col]
            min_value = rules.get('min_value')
            max_value = rules.get('max_value')
            anomaly_percentage = rules.get('anomaly_percentage', 0)
            
            # Randomly decide if this value should be an anomaly
            if random.random() < anomaly_percentage:
                # Generate an anomalous value outside the normal range
                if random.random() < 0.5:  # 50% chance of being below min
                    value = min_value - random.uniform(0.1, 0.3)  # 10-30% below min
                else:  # 50% chance of being above max
                    value = max_value + random.uniform(0.1, 0.3)  # 10-30% above max
            else:
                # Generate a normal value within the range
                value = random.uniform(min_value, max_value)
            
            # Round to 2 decimal places for float values
            if isinstance(value, float):
                value = round(value, 2)
        
        return value
        
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
                elif 'data_quality_rules' in self.schema and col in self.schema['data_quality_rules']:
                    # Use quality rules if they exist for this column
                    row[col] = self._generate_value_with_quality_rules(col, col_def)
                else:
                    # Use standard value generation if no quality rules
                    row[col] = self._generate_value(col, col_def)
            rows.append(row)
            
        return pd.DataFrame(rows) 