# DLT StreamForge

A Python-based data generation project for Databricks Apps that creates realistic, referentially intact test data from YAML schema definitions. The platform also generates Delta Live Tables (DLT) pipeline code to help you get started with data processing.

## Overview

DLT StreamForge provides a powerful framework for generating realistic data across various sectors (Energy, Retail, Healthcare, etc.) by defining your data models in YAML schemas. It's designed to be deployed as a Databricks App, making it easy to generate and manage test data in your Databricks environment. The platform automatically handles relationships between tables, ensuring that foreign keys in fact tables reference valid dimension keys, and maintains data consistency across your entire dataset. Additionally, it generates ready-to-use DLT pipeline code in Python or SQL to help you build your data processing workflows.

## Features

- **Sector-Agnostic**: Generate data for any sector by adding new schema definitions
- **Schema-Driven**: Data generation based on YAML schema definitions
- **Realistic Data Patterns**: Generates meaningful categorical values and realistic data distributions
- **Referential Integrity**: Maintains relationships between dimension and fact tables
- **DLT Pipeline Generation**: Creates Python or SQL code for DLT pipeline
- **Continuous Data Generation**: Fact and change feed data are generated every 30 seconds, while dimension data is generated once
- **Multiple Data Types**: Supports various data types including:
  - Numerical (int, float)
  - Temporal (datetime)
  - Categorical (string with predefined formats)
  - Boolean
  - Composite types (arrays, maps)

## Project Structure

```
.
├── app.py                    # Main Databricks App entry point
├── app.yaml                  # Databricks App configuration
├── requirements.txt          # Project dependencies
├── data_generators/          # Core data generation logic
│   ├── base_generator.py     # Base class for all generators
│   ├── dimension_generator.py # Handles dimension table generation
│   ├── fact_generator.py     # Handles fact table generation
│   └── change_feed_generator.py # Handles change feed generation
└── schema/                   # YAML schema definitions
    ├── Energy/              # Energy sector schemas
    ├── Retail/              # Retail sector schemas
    └── [Your Sector]/       # Add your own sector schemas
```

## Schema Categories

The project supports three types of schemas, each serving a specific purpose in data generation:

### 1. Dimension Schemas
Dimension tables store descriptive attributes and reference data. Example:
```yaml
# schema/Energy/equipment.yml
table: equipment
type: dimension
num_rows: 500
columns:
  equipment_id: int
  equipment_name: 
    type: string
    format: "e######"
  equipment_type: 
    type: string
    format: "GAS_TURBINE|STEAM_TURBINE|WIND_TURBINE|SOLAR_PANEL"
  status: 
    type: string
    format: "OPERATIONAL|MAINTENANCE|OFFLINE|DECOMMISSIONED"
```

### 2. Fact Schemas
Fact tables store measurable events and metrics. These tables are continuously generated every 30 seconds and placed in the Unity Catalog volume path under corresponding subdirectories. Example:
```yaml
# schema/Energy/electricity_production.yml
table: electricity_production
type: fact
num_rows: 1000
columns:
  production_id: int
  plant_id: int
  equipment_id: int
  timestamp: datetime
  generation_mwh: float
  weather_conditions: 
    type: string
    format: "CLEAR|CLOUDY|RAINY|STORMY|SNOWY"
```

### 3. Change Feed Schemas
Change feed tables track modifications to dimension tables. Like fact tables, these are also generated every 30 seconds and placed in the Unity Catalog volume path under corresponding subdirectories. Example:
```yaml
# schema/Energy/customer_changes.yml
table: customer_changes
type: change_feed
num_rows: 100
columns:
  change_id: int
  customer_id: int
  change_type:
    type: string
    format: "INSERT|UPDATE|DELETE"
  change_timestamp: datetime
  changed_fields: 
    type: array
    items: string
```

## Deployment

### Local Development

1. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the application:
   ```bash
   python app.py
   ```
   This will start the application locally and generate data in the specified output directory.

### Databricks Apps Deployment

1. Import the project into your Databricks workspace
2. Deploy as a Databricks App following the [official Databricks Apps documentation](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/get-started)
3. Configure your schema files in the `schema/` directory
4. Ensure you have Unity Catalog Volume read and write permissions for data generation
5. Use the app to generate data directly in your Databricks environment

The project includes:
- `app.py`: Main application entry point
- `app.yaml`: Databricks App configuration
- `requirements.txt`: Required Python dependencies

## Schema Definition Features

### Table Types
- `dimension`: For reference data and attributes
- `fact`: For measurable events and metrics
- `change_feed`: For tracking dimension table changes

### Format Specifications

The platform supports various data types and formats for generating realistic data:

#### Data Types
- `int`: Random integers between 1 and 9999
- `float`: Random floats between 0 and 1000 (rounded to 2 decimal places)
- `string`: Various string formats and smart field detection
- `datetime`: ISO format timestamps
- `bool`: Boolean values

#### String Format Types
1. **Pipe-separated Values**
   ```yaml
   format: "RES|COM|IND"  # Randomly selects one value
   ```

2. **Pattern-based Formats**
   ```yaml
   format: "RATE-##"     # Replaces # with random digits
   format: "MTR-####-####"  # Multiple digit groups
   ```

3. **Smart Field Detection**
   The generator automatically detects common field names and generates appropriate values:
   - `name`: Generates random names
   - `address`: Generates random addresses
   - `city`: Generates random cities
   - `state`: Generates random states
   - `zip`: Generates random zip codes
   - Other fields: Generates random words with title case

#### Special Handling
- Fields ending with `_id` are automatically assigned sequential values
- Foreign keys in fact tables use dimension key ranges for referential integrity:
  - Primary key ranges from dimension tables are stored (e.g., equipment_id: 1-500)
  - Fact table foreign keys are generated within these ranges (e.g., equipment_id in production table will only use values 1-500)
  - This ensures all foreign keys reference valid dimension records
- Change feed tables support operation types (INSERT|UPDATE|DELETE) and timestamp generation
