# Lakeflow StreamForge

A Python-based data generation tool for creating realistic streaming data pipelines with Databricks Lakeflow Declarative Pipelines. This tool generates dimension, fact, and change feed tables with configurable schemas and data quality rules.

## Features

- **Multiple Table Types**:
  - Dimension tables with configurable key ranges
  - Fact tables with foreign key relationships
  - Change feed tables with SCD Type 2 support
  - Support for multiple industries (Retail, Energy, etc.)

- **Data Quality Rules**:
  - Configurable min/max values for numeric columns
  - Anomaly percentage to generate out-of-range values
  - Automatic data quality rule application during generation
  - Support for both normal and anomalous data patterns

- **Lakeflow Declarative Pipeline Generation**:
  - Automatic pipeline code generation in SQL and Python
  - Support for streaming tables and views
  - SCD Type 2 implementation for change feeds
  - Exportable as Jupyter notebooks
  - Flexible medallion architecture options (Bronze only or Bronze + Silver)
  - Two modes: Full Code and Workshop Mode

- **Continuous Data Generation**:
  - Simulates real-time data streaming by continuously generating files
  - Configurable duration (1-24 hours) with countdown timer
  - Maintains referential integrity across dimension and fact tables
  - Generates new data every 15 seconds
  - Automatic cleanup when duration expires
  - Persistent UI state during page reloads

## Prerequisites

- Python 3.8+
- Databricks CLI configured (for Databricks deployment)
- Required Python packages (see requirements.txt)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/dlt-streamforge.git
cd dlt-streamforge
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

1. **Configure Schema**:
   - Place your schema YAML files in the `schema/{industry}` directory
   - Define tables, columns, and data quality rules
   - Example schema structure:
   ```yaml
   table: energy_consumption
   type: fact
   num_rows: 1000
   columns:
     plant_id:
       type: int
     consumption_value:
       type: float
   data_quality_rules:
     consumption_value:
       min_value: 0
       max_value: 1000
       anomaly_percentage: 0.05  # 5% of values will be outside range
   ```

2. **Run the Application**:
   ```bash
   python app.py
   ```

3. **Generate Data**:
   - Enter a volume or directory path to stream the generated data model files
   - Select your industry
   - Choose output language (SQL/Python)
   - Select medallion layers (Bronze only or Bronze + Silver)
   - Choose pipeline mode (Full Code or Workshop Mode)
   - Set duration in hours (1-24, default: 4)
   - Click "Start" to begin generation

   The application will:
   - Generate initial dimension tables and pipeline code
   - Continuously generate new fact and change feed data every 15 seconds
   - Maintain referential integrity with dimension tables
   - Show countdown timer with remaining time
   - Continue until either:
     * You click the "Stop" button
     * The configured duration expires
   - Automatically stop and clean up resources when finished
   - Maintain UI state consistency during page reloads

### Pipeline Generation Options

1. **Medallion Layers**:
   - **Bronze Only**: Generates pipeline code for raw data ingestion only
   - **Bronze + Silver**: Generates pipeline code for both raw data ingestion and quality-enriched tables

2. **Pipeline Modes**:
   - **Full Code**: Generates complete, production-ready pipeline code
   - **Workshop Mode**: Generates code with placeholders for educational purposes

3. **Duration Control**:
   - Set generation duration between 1 and 24 hours
   - Real-time countdown timer shows remaining time
   - Automatic cleanup when duration expires
   - UI state persists during page reloads

## Schema Configuration

### Table Types

1. **Dimension Tables**:
   ```yaml
   table: equipment
   type: dimension
   num_rows: 500
   columns:
     equipment_id:
       type: int
     equipment_name:
       type: string
   ```

2. **Fact Tables**:
   ```yaml
   table: energy_consumption
   type: fact
   num_rows: 1000
   columns:
     plant_id:
       type: int
     consumption_value:
       type: float
   data_quality_rules:
     consumption_value:
       min_value: 0
       max_value: 1000
       anomaly_percentage: 0.05
   ```

3. **Change Feed Tables**:
   ```yaml
   table: customer_changes
   type: change_feed
   num_rows: 100
   columns:
     key:
       type: string
     change_timestamp:
       type: datetime
   change_feed_rules:
     dlt_config:
       keys: ["key"]
       sequence_by: "change_timestamp"
   ```

### Data Quality Rules

Data quality rules can be defined for any column in your schema:

```yaml
data_quality_rules:
  column_name:
    min_value: 0        # Minimum allowed value
    max_value: 1000     # Maximum allowed value
    anomaly_percentage: 0.05  # Percentage of values that will be outside range
```

## Output

The tool generates:
1. CSV files for each table
2. Pipeline code in SQL and Python
3. Jupyter notebook with complete pipeline code
   - Provides guidance on replacing placeholders in Workshop Mode
   - Contains links to relevant Databricks documentation

## Deployment

### Local Development
- Run `python app.py`
- Access the UI at `http://localhost:8050`

### Databricks Deployment
1. Configure Databricks CLI
2. Deploy to Databricks workspace
3. Access through Databricks URL

## Contributing

1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
