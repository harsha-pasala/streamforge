import dash
from dash import dcc, html, Output, Input, State
import os
import yaml
import time
import json
import logging
from data_generators import DimensionGenerator, FactGenerator, ChangeFeedGenerator, BaseGenerator
from dash.dependencies import ClientsideFunction
from threading import Thread
import threading
from flask import jsonify

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
APP_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_BASE_PATH = os.path.join(APP_DIR, "schema")

# Theme configuration
DB_COLORS = {
    'primary': '#FF3621',
    'secondary': '#1E88E5',
    'success': '#00A86B',
    'background': '#FFFFFF',
    'text': '#333333',
    'border': '#E0E0E0'
}

# Common styles
STYLES = {
    'container': {
        'maxWidth': '800px',
        'margin': '0 auto',
        'padding': '40px',
        'backgroundColor': DB_COLORS['background'],
        'borderRadius': '8px',
        'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
    },
    'input_container': {
        'maxWidth': '600px',
        'margin': '0 auto 40px auto',
        'padding': '40px 40px 40px 0px',
        'backgroundColor': DB_COLORS['background'],
        'borderRadius': '8px',
        'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
    },
    'button': {
        'color': 'white',
        'border': 'none',
        'padding': '10px 24px',
        'borderRadius': '4px',
        'cursor': 'pointer',
        'fontWeight': '500',
        'fontSize': '14px',
        'transition': 'background-color 0.3s'
    },
    'code_block': {
        'backgroundColor': '#2D2D2D',
        'color': '#FFFFFF',
        'padding': '15px',
        'borderRadius': '4px',
        'overflowX': 'auto',
        'fontFamily': 'monospace',
        'fontSize': '14px'
    }
}

# Global state
dimension_key_ranges = {}
status = {
    "running": False,
    "industry": None,
    "iteration_count": 0,
    "start_time": None,
    "dlt_code": None,
    "output_path": None,
    "thread": None,
    "lock": threading.Lock(),
    "selected_language": None,
    "selected_industry": None,
    "path_input": None,
    "selected_dlt_output": None,
    "selected_dlt_mode": None,
    "duration_hours": 4  # Default to 4 hours
}

def generation_service():
    """Background service that runs file generation."""
    while status["running"]:
        try:
            with status["lock"]:
                if not status["running"]:
                    break
                generate_files_for_industry(status["industry"])
        except Exception as e:
            logger.error(f"Error in generation service: {str(e)}")
            with status["lock"]:
                status["running"] = False
                status["thread"] = None
            break
        time.sleep(15)  # Wait 15 seconds between iterations

def start_generation_thread():
    """Start the generation thread if it's not already running."""
    with status["lock"]:
        if status["thread"] is None:
            status["running"] = True  # Set running state before starting thread
            status["thread"] = Thread(target=generation_service)
            status["thread"].start()
            print("Started generation thread, running state:", status["running"])  # Debug log

def stop_generation_thread():
    """Stop the generation thread if it's running."""
    with status["lock"]:
        if status["thread"]:
            print("Stopping background thread...")
            status["running"] = False
            status["thread"].join(timeout=5)  # Wait up to 5 seconds for thread to finish
            status["thread"] = None
            # Reset all state
            status["industry"] = None
            status["iteration_count"] = 0
            status["start_time"] = None
            status["dlt_code"] = None
            status["output_path"] = None
            # Don't reset selected_language, selected_industry, path_input, and selected_dlt_output
            # as they are UI state that should persist
            print("Background thread stopped and state reset")

# Initialize Dash app
app = dash.Dash(__name__)
server = app.server

# Add state endpoint
@app.server.route('/api/state')
def get_state():
    """Endpoint to check the current state."""
    with status["lock"]:
        state = {
            "running": status["running"],
            "industry": status["industry"],
            "iteration_count": status["iteration_count"],
            "dlt_code": status["dlt_code"],
            "selected_language": status["selected_language"],
            "selected_industry": status["selected_industry"],
            "path_input": status["path_input"],
            "selected_dlt_output": status["selected_dlt_output"],
            "selected_dlt_mode": status["selected_dlt_mode"],
            "duration_hours": status["duration_hours"]
        }
        print("Returning state:", state)  # Add debug logging
        return jsonify(state)

# Add custom CSS for Inter font and Font Awesome
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
        <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
        <style>
            * {
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# Helper functions
def list_industries():
    """List all available industries from schema directory."""
    return [
        d for d in os.listdir(SCHEMA_BASE_PATH)
        if os.path.isdir(os.path.join(SCHEMA_BASE_PATH, d))
    ]

def load_all_schemas(industry):
    """Load all schema files for an industry."""
    industry_path = os.path.join(SCHEMA_BASE_PATH, industry)
    schemas = []
    for file in os.listdir(industry_path):
        if file.endswith((".yml", ".yaml")):
            with open(os.path.join(industry_path, file)) as f:
                schema = yaml.safe_load(f)
                schemas.append(schema)
    return schemas

def generate_dlt_references(schema, output_path, table_type):
    """Generate DLT reference code for a table in both SQL and Python."""
    table_name = schema["table"]
    
    # Generate quality constraints only for fact and dimension tables
    quality_constraints = []
    if table_type in ['fact', 'dimension'] and 'data_quality_rules' in schema:
        for column, rules in schema['data_quality_rules'].items():
            # Handle NOT NULL constraints
            if rules.get('not_null'):
                constraint_name = f"not_null_{column}"
                constraint_condition = f"{column} IS NOT NULL"
                
                # Validate action is one of the allowed values
                action = rules.get('action', 'warn').lower()
                if action not in ['warn', 'drop', 'fail']:
                    logger.warning(f"Invalid action '{action}' for column {column}. Defaulting to 'warn'.")
                    action = 'warn'
                
                quality_constraints.append({
                    'name': constraint_name,
                    'condition': constraint_condition,
                    'description': rules.get('description', f'{column} should not be null'),
                    'action': action
                })
            
            # Handle min/max value constraints
            if 'min_value' in rules and 'max_value' in rules:
                constraint_name = f"valid_{column}"
                constraint_condition = f"{column} BETWEEN {rules['min_value']} AND {rules['max_value']}"
                
                # Validate action is one of the allowed values
                action = rules.get('action', 'warn').lower()
                if action not in ['warn', 'drop', 'fail']:
                    logger.warning(f"Invalid action '{action}' for column {column}. Defaulting to 'warn'.")
                    action = 'warn'
                
                quality_constraints.append({
                    'name': constraint_name,
                    'condition': constraint_condition,
                    'description': rules.get('description', f'Valid range for {column}'),
                    'action': action
                })
    
    # Helper function to get table comment based on mode
    def get_table_comment(layer, table_name, table_type):
        if status["selected_dlt_mode"] == "full_code":
            return f"{layer} Streaming Table for {table_name} ({table_type})"
        return "<CHANGE_HERE: enter_table_comment>"
    
    if table_type == "change_feed":
        # Get DLT configuration from schema
        dlt_config = schema.get("change_feed_rules", {}).get("dlt_config", {})
        keys = dlt_config.get("keys", ["key"])  # Default to ["key"] if not specified
        sequence_by = dlt_config.get("sequence_by", "change_timestamp")  # Default to change_timestamp if not specified
        except_columns = dlt_config.get("except_columns", None)  # Optional except_columns
        
        # Build the COLUMNS clause for SQL
        columns_clause = ""
        if except_columns:
            columns_clause = f"\nCOLUMNS * EXCEPT ({', '.join(except_columns)})"
        
        # SQL DLT code for change feed - using full catalog.schema format for change feeds
        sql_code = f'''
-- Create streaming table for raw data
CREATE OR REFRESH STREAMING TABLE bronze.{table_name}
COMMENT '{get_table_comment("Bronze", table_name, "change feed")}'
AS SELECT * FROM STREAM read_files("{output_path}/", format => "csv", inferColumnTypes => "true", multiLine => "true");

-- Create streaming table
CREATE OR REFRESH STREAMING TABLE silver.{table_name}
COMMENT '{get_table_comment("Silver", table_name, "change feed")}';

-- AUTO CDC FLOW
CREATE FLOW auto_cdc_{table_name} AS AUTO CDC INTO silver.{table_name}
FROM STREAM(bronze.{table_name})
KEYS ({', '.join(keys)})
SEQUENCE BY {sequence_by}{columns_clause}
STORED AS SCD TYPE {2 if status["selected_dlt_mode"] == "full_code" else "<CHANGE_HERE: 1/2>"};
'''
        
        # Build the auto_cdc_flow parameters for Python
        scd_type_value = f'"{2 if status["selected_dlt_mode"] == "full_code" else "<CHANGE_HERE: 1/2>"}"'
        
        # Add except_column_list parameter if present
        extra_params = ""
        if except_columns:
            extra_params = f',\n    except_column_list={except_columns}'
        
        # Python DLT code for change feed - using full catalog.schema format for change feeds
        python_code = f'''@dlt.table(name="bronze.{table_name}")
def source():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load("{output_path}/")
    )

dlt.create_streaming_table(
    name="silver.{table_name}",
    comment="{get_table_comment("Silver", table_name, "change feed")}"
)

dlt.create_auto_cdc_flow(
    target="silver.{table_name}",
    source="bronze.{table_name}",
    keys={keys},
    sequence_by="{sequence_by}",
    stored_as_scd_type={scd_type_value}{extra_params}
)
'''
    else:  # fact or dimension tables
        # Generate SQL code based on DLT Output selection
        if status["selected_dlt_output"] == "bronze":
            # Only bronze table without constraints
            sql_code = f'''
CREATE OR REFRESH STREAMING TABLE bronze.{table_name}
COMMENT '{get_table_comment("Bronze", table_name, table_type)}'
AS SELECT * FROM STREAM read_files("{output_path}/", format => "csv", inferColumnTypes => "true", multiLine => "true")
'''
            # Python code for bronze only
            python_code = f'''@dlt.table(name="bronze.{table_name}")
def {table_name}():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load("{output_path}/")
    )
'''
        else:  # bronze and silver
            # Generate constraint lines for silver table
            constraint_lines = []
            for c in quality_constraints:
                if c['action'] == 'warn':
                    constraint_lines.append(f"CONSTRAINT {c['name']} EXPECT ({c['condition']})")
                elif c['action'] == 'drop':
                    constraint_lines.append(f"CONSTRAINT {c['name']} EXPECT ({c['condition']}) ON VIOLATION DROP ROW")
                elif c['action'] == 'fail':
                    constraint_lines.append(f"CONSTRAINT {c['name']} EXPECT ({c['condition']}) ON VIOLATION FAIL UPDATE")
            constraints_sql = f"(\n{', '.join(constraint_lines)}\n)" if quality_constraints else ""

            # SQL code for both bronze and silver
            sql_code = f'''
-- Create bronze table
CREATE OR REFRESH STREAMING TABLE bronze.{table_name}
COMMENT '{get_table_comment("Bronze", table_name, table_type)}'
AS SELECT * FROM STREAM read_files("{output_path}/", format => "csv", inferColumnTypes => "true", multiLine => "true");

-- Create silver table with constraints
CREATE OR REFRESH STREAMING TABLE silver.{table_name}{constraints_sql}
COMMENT '{get_table_comment("Silver", table_name, table_type)}'
AS SELECT * FROM STREAM(bronze.{table_name})
'''

            # Python code for both bronze and silver
            python_constraints = []
            for c in quality_constraints:
                if c['action'] == 'warn':
                    python_constraints.append(f'@dlt.expect("{c["name"]}", "{c["condition"]}")')
                elif c['action'] == 'drop':
                    python_constraints.append(f'@dlt.expect_or_drop("{c["name"]}", "{c["condition"]}")')
                elif c['action'] == 'fail':
                    python_constraints.append(f'@dlt.expect_or_fail("{c["name"]}", "{c["condition"]}")')

            python_code = f'''@dlt.table(name="bronze.{table_name}")
def {table_name}_bronze():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load("{output_path}/")
    )

@dlt.table(name="silver.{table_name}")
{chr(10).join(python_constraints)}
def {table_name}_silver():
    return spark.readStream.table("bronze.{table_name}")
'''
    
    return {
        'sql': sql_code,
        'python': python_code
    }

def generate_files_for_industry(industry):
    """Generate all data files for an industry."""
    global dimension_key_ranges, status

    current_iteration = status['iteration_count']
    status['iteration_count'] += 1

    logger.info(f"\nIteration {current_iteration} for industry {industry}")
    logger.debug(f"Current dimension_key_ranges: {dimension_key_ranges}")

    schemas = load_all_schemas(industry)
    dlt_references = []

    # Check and clean up output directory before starting
    if current_iteration == 0:
        output_dir = os.path.join(status['output_path'], industry)
        # Create a temporary generator instance to handle directory cleanup
        is_local = not status['output_path'].strip().lower().startswith('/volumes/')
        
        # Debug logging for cleanup
        logger.info(f"DEBUG CLEANUP - Output path: '{status['output_path']}'")
        logger.info(f"DEBUG CLEANUP - Output path (stripped): '{status['output_path'].strip()}'")
        logger.info(f"DEBUG CLEANUP - Starts with /volumes/ (case-insensitive): {status['output_path'].strip().lower().startswith('/volumes/')}")
        logger.info(f"DEBUG CLEANUP - is_local determined as: {is_local}")
        logger.info(f"DEBUG CLEANUP - output_dir: {output_dir}")
        
        # Use DimensionGenerator since it's the simplest concrete implementation
        temp_generator = DimensionGenerator(None, status['output_path'], is_local=is_local)
        temp_generator._check_directory_empty(output_dir)

    # Store dimension key ranges in first iteration
    if current_iteration == 0:
        for schema in schemas:
            if schema.get("type", "fact") == "dimension":
                for col in schema["columns"]:
                    if col.endswith("_id"):
                        dimension_key_ranges[col] = schema.get("num_rows", 10)
                        logger.debug(f"Storing dimension key range for {col}: {dimension_key_ranges[col]}")

    # Process all tables
    for schema in schemas:
        table = schema["table"]
        table_type = schema.get("type", "fact")

        logger.info(f"\nProcessing table: {table} (type: {table_type})")

        # Skip dimension tables after first iteration
        if table_type == "dimension" and current_iteration > 0:
            logger.info(f"Skipping dimension table {table} as iteration_count > 0")
            continue

        try:
            schema_path = os.path.join(SCHEMA_BASE_PATH, industry, f"{table}.yml")
            logger.info(f"Loading schema from: {schema_path}")
            
            # Determine if we're in a local environment based on the output path
            is_local = not status['output_path'].strip().lower().startswith('/volumes/')
            
            # Debug logging
            logger.info(f"DEBUG - Output path: '{status['output_path']}'")
            logger.info(f"DEBUG - Output path (stripped): '{status['output_path'].strip()}'")
            logger.info(f"DEBUG - Starts with /volumes/ (case-insensitive): {status['output_path'].strip().lower().startswith('/volumes/')}")
            logger.info(f"DEBUG - is_local determined as: {is_local}")
            
            # Select appropriate generator based on table type
            if table_type == "dimension":
                generator = DimensionGenerator(schema_path, status['output_path'], is_local=is_local)
            elif table_type == "fact":
                generator = FactGenerator(schema_path, status['output_path'], dimension_key_ranges, is_local=is_local)
            elif table_type == "change_feed":
                generator = ChangeFeedGenerator(schema_path, status['output_path'], is_local=is_local)
            else:
                logger.warning(f"Unknown table type: {table_type}")
                continue

            # Generate and save data
            logger.info(f"Generating data for table: {table}")
            df = generator.generate_data()
            logger.info(f"Saving data for table: {table}")
            output_path = generator.save_data(df, table)
            logger.info(f"Data saved to: {output_path}")
            
            # Generate DLT references for first iteration
            if current_iteration == 0:
                logger.info(f"Generating DLT references for table: {table}")
                dlt_refs = generate_dlt_references(schema, output_path, table_type)
                dlt_references.append({
                    "table": table,
                    "type": table_type,
                    "references": dlt_refs
                })

        except Exception as e:
            logger.error(f"Error processing table {table}: {str(e)}")
            raise

    # Print DLT references after first iteration
    if current_iteration == 0 and dlt_references:
        logger.info("\n=== DLT Reference Code ===")
        for ref in dlt_references:
            logger.info(f"\nTable: {ref['table']} ({ref['type']})")
            logger.debug("\nSQL DLT Code:")
            logger.debug(ref['references']['sql'])
            logger.debug("\nPython DLT Code:")
            logger.debug(ref['references']['python'])
            logger.debug("\n" + "="*50)

    logger.info(f"\nCompleted iteration {current_iteration}")

def create_dlt_code_display(dlt_codes, language):
    """Create the DLT code display component."""
    if not language:
        return html.Div("Please select a language to view the code.", style={
            'textAlign': 'center',
            'color': DB_COLORS['text'],
            'fontSize': '14px',
            'fontStyle': 'italic'
        })
    
    return html.Div([
        html.Div([
            html.H4(f"Table: {code['table']}", style={'marginBottom': '10px'}),
            html.Div([
                html.Pre(code['code'][language], style=STYLES['code_block'])
            ])
        ]) for code in dlt_codes
    ])

def create_notebook_content(dlt_codes, selected_language):
    """Create Jupyter notebook content with DLT code."""
    cells = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [f"""### Databricks DLT Pipeline Code

This notebook contains the DLT pipeline code for creating bronze tables in {selected_language.upper()}.

Look for `<CHANGE_HERE: ...>` placeholders in the code and replace them with your values. Detailed instructions follow below.

#### Table Naming Instructions
Before running the code, you need to specify where your tables will be stored. You can use any of these three formats:

1. Three level catalog.schema.table format:
   - Replace `<CHANGE_HERE: catalog>.<CHANGE_HERE: schema>` with your Unity Catalog and schema names
   - Example: `unity_catalog.my_schema.table_name`

2. Two level schema.table format:
   - Replace `<CHANGE_HERE: schema>` with your schema name
   - The default catalog will be used
   - Example: `my_schema.table_name`

3. Simple table name format:
   - Use just the table name
   - Both default catalog and schema will be used
   - Example: `table_name`

#### Table Documentation and Configuration
For each table in the code:

1. Table Comments:
   - Replace `<CHANGE_HERE: enter_table_comment>` with a descriptive comment about the table's purpose and contents
   - Example: "Bronze table containing raw customer transaction data"

2. For Change Feed Tables:
   - Replace `<CHANGE_HERE: 1/2>` with either 1 or 2 to specify the SCD (Slowly Changing Dimension) type:
     - Type 1: Overwrites the old value with the new value
     - Type 2: Maintains history by creating new records for each change

#### Learn More
- [Streaming Tables Documentation](https://docs.databricks.com/aws/en/dlt/streaming-tables) - Learn about streaming tables and their use cases for data ingestion and low-latency streaming transformations.
- [Materialized Views Documentation](https://docs.databricks.com/aws/en/dlt/materialized-views) - Understand how materialized views work and their benefits for incremental data processing."""]
        }
    ]
    
    for code in dlt_codes:
        cells.extend([
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [f"#### Table: {code['table']}"]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "source": [code['code'][selected_language]],
                "outputs": []
            }
        ])
    
    # Set kernel based on selected language
    kernel_spec = {
        "python": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3"
        },
        "sql": {
            "display_name": "SQL",
            "language": "sql",
            "name": "sql"
        }
    }
    
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": kernel_spec[selected_language],
            "language_info": {
                "codemirror_mode": {
                    "name": "ipython" if selected_language == "python" else "sql",
                    "version": 3
                },
                "file_extension": ".py" if selected_language == "python" else ".sql",
                "mimetype": "text/x-python" if selected_language == "python" else "text/x-sql",
                "name": selected_language,
                "nbconvert_exporter": "python" if selected_language == "python" else "sql",
                "pygments_lexer": "ipython3" if selected_language == "python" else "sql",
                "version": "3.8.0"
            },
            "default_language": selected_language
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    return json.dumps(notebook)

# UI Component functions
def create_header():
    """Create the app header with logo and title."""
    return html.Div([
        html.Div([
            html.I(
                className="fas fa-stream",
                style={
                    'fontSize': '32px',
                    'color': DB_COLORS['primary'],
                    'marginRight': '12px',
                    'verticalAlign': 'middle'
                }
            ),
            html.H2("Lakeflow StreamForge", 
                    style={
                        'color': DB_COLORS['text'],
                        'fontWeight': '600',
                        'fontSize': '24px',
                        'display': 'inline-block',
                        'verticalAlign': 'middle',
                        'margin': '0'
                    }),
        ], style={'textAlign': 'center', 'marginBottom': '40px'}),
    ], style={'textAlign': 'center', 'marginBottom': '40px'})

def create_input_section():
    """Create the input section with path, dropdowns, and control button."""
    return html.Div([
        html.Div([
            html.Div([
                dcc.Input(
                    id='path-input',
                    type='text',
                    value='',
                    placeholder='Path to volume E.g./Volumes/path/to/stream/',
                    style={
                        'width': '412px',
                        'padding': '8px 12px',
                        'border': f'1px solid {DB_COLORS["border"]}',
                        'borderRadius': '4px',
                        'fontSize': '14px',
                        'marginBottom': '15px'
                    }
                ),
            ], style={'textAlign': 'center', 'marginBottom': '15px'}),
            html.Div([
                dcc.Dropdown(
                    id='industry-dropdown',
                    options=[{"label": i, "value": i} for i in list_industries()],
                    value=None,
                    placeholder="Choose industry",
                    style={
                        'border': f'1px solid {DB_COLORS["border"]}',
                        'borderRadius': '4px',
                        'fontSize': '14px',
                        'width': '250px',
                        'display': 'inline-block',
                        'verticalAlign': 'middle',
                        'marginRight': '12px'
                    }
                ),
                dcc.Dropdown(
                    id='language-dropdown',
                    options=[
                        {"label": "SQL", "value": "sql"},
                        {"label": "Python", "value": "python"}
                    ],
                    value=None,
                    placeholder="Choose language",
                    style={
                        'border': f'1px solid {DB_COLORS["border"]}',
                        'borderRadius': '4px',
                        'fontSize': '14px',
                        'width': '150px',
                        'display': 'inline-block',
                        'verticalAlign': 'middle',
                        'marginRight': '12px'
                    }
                ),
            ], style={'marginBottom': '20px', 'textAlign': 'center'}),
            html.Div([
                dcc.Dropdown(
                    id='dlt-output-dropdown',
                    options=[
                        {"label": "Bronze", "value": "bronze"},
                        {"label": "Bronze and Silver", "value": "bronze_silver"}
                    ],
                    value=None,
                    placeholder="Choose Medallion Layers",
                    style={
                        'border': f'1px solid {DB_COLORS["border"]}',
                        'borderRadius': '4px',
                        'fontSize': '14px',
                        'width': '200px',
                        'display': 'inline-block',
                        'verticalAlign': 'middle',
                        'marginRight': '12px'
                    }
                ),
                dcc.Dropdown(
                    id='dlt-mode-dropdown',
                    options=[
                        {"label": "Full Code", "value": "full_code"},
                        {"label": "Workshop Mode", "value": "workshop_mode"}
                    ],
                    value=None,
                    placeholder="Choose DLT Mode",
                    style={
                        'border': f'1px solid {DB_COLORS["border"]}',
                        'borderRadius': '4px',
                        'fontSize': '14px',
                        'width': '200px',
                        'display': 'inline-block',
                        'verticalAlign': 'middle',
                        'marginRight': '12px'
                    }
                ),
            ], style={'marginBottom': '20px', 'textAlign': 'center'}),
            html.Div([
                html.Div([
                    html.Label(
                        "Duration (hours):",
                        style={
                            'display': 'inline-block',
                            'marginRight': '10px',
                            'fontSize': '14px',
                            'fontWeight': '500',
                            'color': '#666666'  # Darker gray for better readability
                        }
                    ),
                    dcc.Input(
                        id='duration-input',
                        type='number',
                        value=1,
                        min=1,
                        max=24,
                        placeholder='Enter number of hours (1-24)',
                        style={
                            'width': '120px',  # Reduced width since it's side by side
                            'padding': '8px 12px',
                            'border': f'1px solid {DB_COLORS["border"]}',
                            'borderRadius': '4px',
                            'fontSize': '14px',
                            'display': 'inline-block',
                            'verticalAlign': 'middle',
                            'color': '#333333'  # Darker text color for better readability
                        }
                    ),
                ], style={
                    'display': 'inline-block',
                    'verticalAlign': 'middle',
                    'textAlign': 'left'
                }),
            ], style={'marginBottom': '20px', 'textAlign': 'center'}),
        ], style={'marginBottom': '20px'}),

        html.Div([
            html.Button(
                "Start",
                id='control-button',
                n_clicks=0,
                style={**STYLES['button'], 'backgroundColor': DB_COLORS['success']}
            ),
        ], style={'display': 'flex', 'justifyContent': 'center', 'marginBottom': '15px'}),

        html.Div(
            id='status-display',
            style={
                'padding': '12px',
                'borderRadius': '4px',
                'color': DB_COLORS['text'],
                'fontSize': '14px',
                'fontWeight': '400'
            }
        ),
    ], style={**STYLES['input_container'], 'paddingBottom': '30px'})

def create_code_section():
    """Create the code display section with export button."""
    return html.Div([
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Button([
                            html.I(className="fas fa-cloud-download-alt", style={'marginRight': '8px'}),
                            "Export Notebook"
                        ],
                            id='export-button',
                            n_clicks=0,
                            style={**STYLES['button'], 'backgroundColor': DB_COLORS['secondary']},
                            disabled=True
                        ),
                        dcc.Download(id='download-notebook'),
                    ], style={
                        'display': 'flex',
                        'justifyContent': 'flex-end',
                        'paddingBottom': '5px',
                        'paddingRight': '20px'
                    }),
                ], style={
                    'position': 'absolute',
                    'top': '20px',
                    'right': '20px',
                    'width': '100%',
                    'display': 'none'
                }, id='export-button-container'),
                html.Div(
                    id='dlt-code-display',
                    children=html.Div("Select an industry and click Start to generate DLT code.", style={
                        'textAlign': 'center',
                        'color': DB_COLORS['text'],
                        'fontSize': '14px',
                        'fontStyle': 'italic'
                    }),
                    style={
                        'padding': '20px',
                        'backgroundColor': '#F8F9FA',
                        'borderRadius': '4px',
                        'border': f'1px solid {DB_COLORS["border"]}'
                    }
                ),
            ], style={'position': 'relative'}),
        ]),
    ], id='dlt-code-section', style={**STYLES['container'], 'display': 'none'})

# App layout
app.layout = html.Div([
    create_header(),
    create_input_section(),
    create_code_section(),
    dcc.Interval(id='interval-timer', interval=15000, n_intervals=0, disabled=True),  # 15 seconds for data generation
    dcc.Interval(id='countdown-timer', interval=1000, n_intervals=0, disabled=True),   # 1 second for countdown
    # Add hidden div for initial state check
    html.Div(id='initial-state-trigger', style={'display': 'none'})
], style={
    'backgroundColor': '#F8F9FA',
    'minHeight': '100vh',
    'padding': '40px 20px'
})

# Add callback for countdown timer
@app.callback(
    [Output('status-display', 'children', allow_duplicate=True),
     Output('countdown-timer', 'disabled')],
    Input('countdown-timer', 'n_intervals'),
    prevent_initial_call=True
)
def update_countdown(n_intervals):
    with status["lock"]:
        if not status["running"] or not status["start_time"]:
            return dash.no_update, True
        
        # Calculate remaining time
        duration_seconds = status["duration_hours"] * 3600
        elapsed_time = time.time() - status['start_time']
        remaining_seconds = duration_seconds - elapsed_time
        
        if remaining_seconds <= 0:
            stop_generation_thread()
            return f"Generation stopped after {status['duration_hours']} hours.", True
        
        # Calculate remaining time in hours, minutes, and seconds
        remaining_hours = int(remaining_seconds // 3600)
        remaining_minutes = int((remaining_seconds % 3600) // 60)
        remaining_secs = int(remaining_seconds % 60)
        
        # Format the remaining time message
        time_message = f"Generating files for '{status['industry']}'... (Time remaining: "
        if remaining_hours > 0:
            time_message += f"{remaining_hours}h "
        if remaining_minutes > 0 or remaining_hours > 0:
            time_message += f"{remaining_minutes}m "
        time_message += f"{remaining_secs}s)"
        
        return time_message, False

# Update the control generation callback to handle interval timer
@app.callback(
    [Output('interval-timer', 'disabled'),
     Output('status-display', 'children'),
     Output('control-button', 'children'),
     Output('control-button', 'style'),
     Output('control-button', 'disabled'),
     Output('dlt-code-display', 'children'),
     Output('dlt-code-section', 'style'),
     Output('export-button-container', 'style'),
     Output('countdown-timer', 'disabled', allow_duplicate=True)],
    [Input('control-button', 'n_clicks'),
     Input('interval-timer', 'n_intervals')],
    [State('language-dropdown', 'value'),
     State('industry-dropdown', 'value'),
     State('path-input', 'value'),
     State('dlt-output-dropdown', 'value'),
     State('dlt-mode-dropdown', 'value'),
     State('duration-input', 'value'),
     State('dlt-code-section', 'style'),
     State('dlt-code-display', 'children')],
    prevent_initial_call=True
)
def control_generation(button_clicks, n_intervals, selected_language, selected_industry, path_input, selected_dlt_output, selected_dlt_mode, duration_hours, current_section_style, current_display):
    global dimension_key_ranges, status
    
    ctx = dash.callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    trigger = ctx.triggered[0]['prop_id'].split('.')[0]
    print(f"\nTrigger: {trigger}")
    print(f"Current iteration: {status['iteration_count']}")
    print(f"Current DLT code: {status['dlt_code'] is not None}")

    # Update UI state
    with status["lock"]:
        if selected_language:
            status["selected_language"] = selected_language
        if selected_industry:
            status["selected_industry"] = selected_industry
        if path_input:
            status["path_input"] = path_input.strip()  # Trim whitespace
        if selected_dlt_output:
            status["selected_dlt_output"] = selected_dlt_output
        if selected_dlt_mode:
            status["selected_dlt_mode"] = selected_dlt_mode
        if duration_hours:
            status["duration_hours"] = duration_hours

    # Default section style
    section_style = current_section_style if current_section_style else {**STYLES['container'], 'display': 'none'}
    export_button_style = {'textAlign': 'center', 'display': 'none'}

    # Loading message component
    loading_message = html.Div("Generating DLT code...", style={
        'textAlign': 'center',
        'color': DB_COLORS['text'],
        'fontSize': '14px',
        'fontStyle': 'italic'
    })

    # Button styles
    start_style = {**STYLES['button'], 'backgroundColor': DB_COLORS['success']}
    stop_style = {**STYLES['button'], 'backgroundColor': DB_COLORS['primary']}

    # Handle control button and interval timer
    if trigger == 'control-button':
        if not status["running"]:  # Start button was clicked
            if not path_input:
                return True, html.Div([
                    html.Span("⚠️ Please enter a path to the volume.", 
                             style={'color': '#FF3621'})
                ], style={'padding': '12px'}), "Start", start_style, False, loading_message, section_style, export_button_style, True
            
            if not selected_industry:
                return True, html.Div([
                    html.Span("⚠️ Please select an industry.", 
                             style={'color': '#FF3621'})
                ], style={'padding': '12px'}), "Start", start_style, False, loading_message, section_style, export_button_style, True
            
            if not selected_language:
                return True, html.Div([
                    html.Span("⚠️ Please select a language.", 
                             style={'color': '#FF3621'})
                ], style={'padding': '12px'}), "Start", start_style, False, loading_message, section_style, export_button_style, True

            if not selected_dlt_output:
                return True, html.Div([
                    html.Span("⚠️ Please select medallion layers for DLT Output.", 
                             style={'color': '#FF3621'})
                ], style={'padding': '12px'}), "Start", start_style, False, loading_message, section_style, export_button_style, True

            if not selected_dlt_mode:
                return True, html.Div([
                    html.Span("⚠️ Please select a DLT Mode.", 
                             style={'color': '#FF3621'})
                ], style={'padding': '12px'}), "Start", start_style, False, loading_message, section_style, export_button_style, True

            if not duration_hours or duration_hours < 1 or duration_hours > 24:
                return True, html.Div([
                    html.Span("⚠️ Please enter a valid duration between 1 and 24 hours.", 
                             style={'color': '#FF3621'})
                ], style={'padding': '12px'}), "Start", start_style, False, loading_message, section_style, export_button_style, True

            try:
                print("\nStarting generation...")
                with status["lock"]:
                    status['iteration_count'] = 0
                    status['start_time'] = time.time()
                    status['dlt_code'] = None
                    status['output_path'] = path_input
                    dimension_key_ranges = {}
                    status["running"] = True
                    status["industry"] = selected_industry
                    
                    # Debug logging for path setting
                    logger.info(f"DEBUG START - path_input received: '{path_input}'")
                    logger.info(f"DEBUG START - status['output_path'] set to: '{status['output_path']}'")
                    logger.info(f"DEBUG START - status['output_path'] (stripped): '{status['output_path'].strip()}'")
                    logger.info(f"DEBUG START - path starts with /volumes/ (case-insensitive): {status['output_path'].strip().lower().startswith('/volumes/')}")
                
                # Start the generation thread
                start_generation_thread()
                
                section_style['display'] = 'block'
                return False, f"Generating files for '{selected_industry}'...", "Stop", stop_style, False, loading_message, section_style, export_button_style, False
            except Exception as e:
                return True, html.Div([
                    html.Span(f"⚠️ {str(e)}", 
                             style={'color': '#FF3621'})
                ], style={'padding': '12px'}), "Start", start_style, False, None, section_style, export_button_style, True
        else:  # Stop button was clicked
            print("\nStopping generation...")
            # Disable button immediately and update UI
            with status["lock"]:
                status["running"] = False
                status["thread"] = None
                status["industry"] = None
                status["iteration_count"] = 0
                status["start_time"] = None
                status["dlt_code"] = None
                status["output_path"] = None
            
            # Stop the background thread
            print("Stopping background thread...")
            stop_generation_thread()
            print("Background thread stopped and state reset")
            
            # Reset UI state
            section_style['display'] = 'none'
            export_button_style['display'] = 'none'
            
            return True, "Stopped.", "Start", start_style, False, None, section_style, export_button_style, True

    elif trigger == 'interval-timer':
        with status["lock"]:
            if not status["running"]:
                # If generation is not running, disable the interval timer and reset state
                stop_generation_thread()  # Ensure thread is stopped
                return True, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, True
            
            # Check if DLT code needs to be generated
            if status['dlt_code'] is None:
                print("\nGenerating DLT code...")
                try:
                    schemas = load_all_schemas(status["industry"])
                    dlt_codes = []
                    for schema in schemas:
                        table_type = schema.get("type", "fact")
                        if table_type in ["dimension", "fact", "change_feed"]:
                            table = schema["table"]
                            output_path = os.path.join(status['output_path'], status["industry"], table)
                            code = generate_dlt_references(schema, output_path, table_type)
                            print(f"Generated code for table: {table} (type: {table_type})")
                            dlt_codes.append({
                                "table": table,
                                "code": code
                            })
                    
                    status['dlt_code'] = dlt_codes
                    print(f"Stored DLT code: {status['dlt_code'] is not None}")
                    section_style['display'] = 'block'
                    export_button_style['display'] = 'block'
                    return False, dash.no_update, "Stop", stop_style, False, create_dlt_code_display(dlt_codes, selected_language), section_style, export_button_style, False
                except Exception as e:
                    print(f"Error generating DLT code: {str(e)}")
                    section_style['display'] = 'block'
                    export_button_style['display'] = 'none'
                    return False, dash.no_update, "Stop", stop_style, False, loading_message, section_style, export_button_style, False
            
            print("\nSubsequent iteration - using stored code")
            section_style['display'] = 'block'
            export_button_style['display'] = 'block'
            if status['dlt_code'] is None:
                return False, dash.no_update, "Stop", stop_style, False, loading_message, section_style, export_button_style, False
            return False, dash.no_update, "Stop", stop_style, False, create_dlt_code_display(status['dlt_code'], selected_language), section_style, export_button_style, False

    raise dash.exceptions.PreventUpdate

@app.callback(
    Output('download-notebook', 'data'),
    Input('export-button', 'n_clicks'),
    State('language-dropdown', 'value'),
    prevent_initial_call=True
)
def export_notebook(n_clicks, selected_language):
    if not status['dlt_code'] or not status['industry']:
        raise dash.exceptions.PreventUpdate
    
    # Create notebook content with the current language
    notebook_content = create_notebook_content(status['dlt_code'], selected_language)
    
    # Return the notebook file for download
    return dcc.send_bytes(
        notebook_content.encode('utf-8'),
        filename=f"dlt_pipeline_{status['industry']}.ipynb",
        type='application/x-ipynb+json'
    )

@app.callback(
    Output('export-button', 'disabled'),
    Input('dlt-code-display', 'children'),
    prevent_initial_call=True
)
def update_export_button(code_display):
    if not code_display or isinstance(code_display, str):
        return True
    return False

# Add callback for initial state check
@app.callback(
    [Output('initial-state-trigger', 'children'),
     Output('language-dropdown', 'value'),
     Output('industry-dropdown', 'value'),
     Output('path-input', 'value'),
     Output('dlt-output-dropdown', 'value'),
     Output('dlt-mode-dropdown', 'value'),
     Output('duration-input', 'value')],
    Input('initial-state-trigger', 'children'),
    prevent_initial_call=False  # Allow initial call
)
def trigger_initial_state_check(_):
    """Return current state values on page load."""
    with status["lock"]:
        if status["running"]:
            return [
                'triggered',
                status["selected_language"],
                status["selected_industry"],
                status["path_input"],
                status["selected_dlt_output"],
                status["selected_dlt_mode"],
                status["duration_hours"]
            ]
        return ['triggered', '', '', '', '', '', 4]  # Default duration to 4 hours

# Add UI state sync callback
@app.callback(
    [Output('control-button', 'children', allow_duplicate=True),
     Output('control-button', 'style', allow_duplicate=True),
     Output('dlt-code-section', 'style', allow_duplicate=True),
     Output('export-button-container', 'style', allow_duplicate=True)],
    [Input('initial-state-trigger', 'children')],
    prevent_initial_call='initial_duplicate'
)
def sync_ui_state(_):
    """Sync UI elements with server state on page load."""
    # Button styles
    start_style = {**STYLES['button'], 'backgroundColor': DB_COLORS['success']}
    stop_style = {**STYLES['button'], 'backgroundColor': DB_COLORS['primary']}
    
    # Default section style
    section_style = {**STYLES['container'], 'display': 'none'}
    export_button_style = {'textAlign': 'center', 'display': 'none'}
    
    with status["lock"]:
        if status["running"]:
            section_style['display'] = 'block'
            export_button_style['display'] = 'block'
            return "Stop", stop_style, section_style, export_button_style
        else:
            return "Start", start_style, section_style, export_button_style

# Add separate callback for language dropdown
@app.callback(
    Output('dlt-code-display', 'children', allow_duplicate=True),
    Input('language-dropdown', 'value'),
    State('dlt-code-display', 'children'),
    prevent_initial_call=True
)
def update_code_display(language, current_display):
    """Update code display when language changes."""
    if not language or not status['dlt_code']:
        return dash.no_update
    
    try:
        return create_dlt_code_display(status['dlt_code'], language)
    except Exception as e:
        logger.error(f"Error updating code display: {str(e)}")
        return current_display

if __name__ == "__main__":
    app.run(debug=True)
