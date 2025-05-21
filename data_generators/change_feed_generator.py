from .base_generator import BaseGenerator
import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

class ChangeFeedGenerator(BaseGenerator):
    def __init__(self, schema_path, output_base_path):
        super().__init__(schema_path, output_base_path)
        self.fake = Faker()
        self.rules = self.schema['change_feed_rules']
        
    def _generate_initial_row(self, customer_id):
        """Generate the initial INSERT row for a customer."""
        row = {'customer_id': customer_id, 'operation': 'INSERT'}
        
        # Generate values for all columns except operation and customer_id
        for col, col_def in self.schema['columns'].items():
            if col not in ['operation', 'customer_id', 'change_timestamp']:
                row[col] = self._generate_value(col, col_def)
        
        return row

    def _generate_value(self, col, col_def):
        """Generate a value based on column name and data type."""
        if isinstance(col_def, str):
            dtype = col_def
        else:
            dtype = col_def.get('type', 'string')
            
        # Special handling for datetime in change feeds
        if dtype == 'datetime':
            return self.fake.date_time_between(
                start_date=self.rules['time_range']['start_date'],
                end_date=self.rules['time_range']['end_date']
            ).isoformat()
            
        # Use base implementation for all other types
        return super()._generate_value(col, col_def)

    def _generate_update_row(self, base_row):
        """Generate an UPDATE row based on the previous row."""
        row = base_row.copy()
        row['operation'] = 'UPDATE'
        
        # Update only the allowed fields
        for field in self.rules['updatable_fields']:
            if field in self.schema['columns']:
                col_def = self.schema['columns'][field]
                row[field] = self._generate_value(field, col_def)
        
        return row

    def _generate_delete_row(self, base_row):
        """Generate a DELETE row based on the previous row."""
        row = base_row.copy()
        row['operation'] = 'DELETE'
        
        # Null out the specified fields
        for field in self.rules['delete_null_fields']:
            row[field] = None
            
        return row

    def _generate_timestamps(self, num_changes):
        """Generate ordered timestamps for changes."""
        start_date = datetime.strptime(self.rules['time_range']['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(self.rules['time_range']['end_date'], '%Y-%m-%d')
        
        # Generate random timestamps within the range
        timestamps = []
        current_date = start_date
        for _ in range(num_changes):
            # Add random days between min and max
            days_to_add = random.randint(
                self.rules['time_between_changes']['min'],
                self.rules['time_between_changes']['max']
            )
            current_date += timedelta(days=days_to_add)
            if current_date > end_date:
                break
            timestamps.append(current_date)
            
        return sorted(timestamps)

    def generate_data(self):
        """Generate all change feed data."""
        all_rows = []
        num_customers = self.schema['num_rows']
        
        for customer_id in range(1, num_customers + 1):
            # Generate initial INSERT
            base_row = self._generate_initial_row(customer_id)
            
            # Determine number of changes for this customer
            num_updates = random.randint(0, self.rules['operation_distribution']['UPDATE'])
            will_delete = random.random() < self.rules['operation_distribution']['DELETE']
            
            # Generate timestamps for all changes
            num_changes = 1 + num_updates + (1 if will_delete else 0)  # INSERT + UPDATEs + (DELETE if any)
            timestamps = self._generate_timestamps(num_changes)
            
            # Add INSERT with first timestamp
            base_row['change_timestamp'] = timestamps[0].isoformat()
            all_rows.append(base_row)
            
            # Add UPDATEs
            current_row = base_row
            for i in range(num_updates):
                current_row = self._generate_update_row(current_row)
                current_row['change_timestamp'] = timestamps[i + 1].isoformat()
                all_rows.append(current_row)
            
            # Add DELETE if needed
            if will_delete:
                delete_row = self._generate_delete_row(current_row)
                delete_row['change_timestamp'] = timestamps[-1].isoformat()
                all_rows.append(delete_row)
        
        return pd.DataFrame(all_rows) 