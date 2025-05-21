import dash
from dash import dcc, html, Output, Input, State
import os
import yaml
import time
import json
import logging
from data_generators import DimensionGenerator, FactGenerator, ChangeFeedGenerator, BaseGenerator
from dash.dependencies import ClientsideFunction

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
    "output_path": None
}

# Initialize Dash app
app = dash.Dash(__name__)
server = app.server

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
    """Generate DLT reference code for a table."""
    table_name = schema["table"]
    
    if table_type == "change_feed":
        # Get DLT configuration from schema
        dlt_config = schema.get("change_feed_rules", {}).get("dlt_config", {})
        keys = dlt_config.get("keys", ["key"])  # Default to ["key"] if not specified
        sequence_by = dlt_config.get("sequence_by", "change_timestamp")  # Default to change_timestamp if not specified
        
        # SQL DLT code for change feed
        sql_code = f'''
-- Create streaming view for change feed
CREATE OR REFRESH STREAMING VIEW {table_name}_view
AS SELECT * FROM STREAM read_files("{output_path}/", format => "csv");

-- Create streaming table
CREATE OR REFRESH STREAMING TABLE {table_name};

-- Apply changes using SCD Type 2
APPLY CHANGES INTO {table_name}
FROM {table_name}_view
KEYS ({', '.join(keys)})
SEQUENCE BY {sequence_by}
STORED AS SCD TYPE 2;
'''
        
        # Python DLT code for change feed
        python_code = f'''@dlt.view(name="{table_name}_view")
def source():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFile.format", "csv")
        .load("{output_path}/")
    )

dlt.create_streaming_table("{table_name}")

dlt.apply_changes_into(
    target="{table_name}",
    source="{table_name}_view",
    keys={keys},
    sequence_by="{sequence_by}",
    stored_as_scd_type=2
)
'''
    else:
        # SQL DLT code for regular tables
        sql_code = f'''
CREATE OR REFRESH STREAMING TABLE {table_name}_bronze
AS SELECT * FROM STREAM read_files("{output_path}/", format => "csv")
'''
        
        # Python DLT code for regular tables
        python_code = f'''@dlt.table(name="{table_name}_bronze")
def {table_name}_bronze():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFile.format", "csv")
        .load("{output_path}/")
    )
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
            
            # Select appropriate generator based on table type
            if table_type == "dimension":
                generator = DimensionGenerator(schema_path, status['output_path'])
            elif table_type == "fact":
                generator = FactGenerator(schema_path, status['output_path'], dimension_key_ranges)
            elif table_type == "change_feed":
                generator = ChangeFeedGenerator(schema_path, status['output_path'])
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
                html.Pre(code['references'][language], style=STYLES['code_block'])
            ])
        ]) for code in dlt_codes
    ])

def create_notebook_content(dlt_codes, selected_language):
    """Create Jupyter notebook content with DLT code."""
    cells = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [f"# Databricks DLT Pipeline Code\n\nThis notebook contains the DLT pipeline code for creating bronze tables in {selected_language.upper()}."]
        }
    ]
    
    for code in dlt_codes:
        cells.extend([
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [f"## Table: {code['table']}"]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "source": [code['references'][selected_language]],
                "outputs": []
            }
        ])
    
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {
                    "name": "ipython",
                    "version": 3
                },
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.8.0"
            }
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
            html.H2("DLT StreamForge", 
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
                    placeholder="Choose language",
                    style={
                        'border': f'1px solid {DB_COLORS["border"]}',
                        'borderRadius': '4px',
                        'fontSize': '14px',
                        'width': '150px',
                        'display': 'inline-block',
                        'verticalAlign': 'middle'
                    }
                ),
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
    dcc.Interval(id='interval-timer', interval=15000, n_intervals=0, disabled=True)
], style={
    'backgroundColor': '#F8F9FA',
    'minHeight': '100vh',
    'padding': '40px 20px'
})

# Callbacks
@app.callback(
    Output('interval-timer', 'disabled'),
    Output('status-display', 'children'),
    Output('control-button', 'children'),
    Output('control-button', 'style'),
    Output('dlt-code-display', 'children'),
    Output('dlt-code-section', 'style'),
    Output('industry-dropdown', 'value'),
    Output('language-dropdown', 'value'),
    Output('export-button-container', 'style'),
    Input('control-button', 'n_clicks'),
    Input('interval-timer', 'n_intervals'),
    State('industry-dropdown', 'value'),
    State('language-dropdown', 'value'),
    State('path-input', 'value'),
    prevent_initial_call=True
)
def control_generation(button_clicks, n_intervals, selected_industry, selected_language, path_input):
    global dimension_key_ranges, status
    
    ctx = dash.callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    trigger = ctx.triggered[0]['prop_id'].split('.')[0]
    print(f"\nTrigger: {trigger}")
    print(f"Current iteration: {status['iteration_count']}")
    print(f"Current DLT code: {status['dlt_code'] is not None}")

    # Default section style
    section_style = {**STYLES['container'], 'display': 'none'}
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

    if trigger == 'control-button':
        if not status["running"]:  # Start button was clicked
            if not path_input:
                return True, html.Div([
                    html.Span("⚠️ Please enter a path to the volume.", 
                             style={'color': '#FF3621'})
                ], style={'padding': '12px'}), "Start", start_style, loading_message, section_style, None, None, export_button_style
            
            if not selected_industry:
                return True, html.Div([
                    html.Span("⚠️ Please select an industry.", 
                             style={'color': '#FF3621'})
                ], style={'padding': '12px'}), "Start", start_style, loading_message, section_style, None, None, export_button_style
            
            if not selected_language:
                return True, html.Div([
                    html.Span("⚠️ Please select a language.", 
                             style={'color': '#FF3621'})
                ], style={'padding': '12px'}), "Start", start_style, loading_message, section_style, None, None, export_button_style
            
            try:
                print("\nStarting generation...")
                status['iteration_count'] = 0
                status['start_time'] = time.time()
                status['dlt_code'] = None
                status['output_path'] = path_input
                dimension_key_ranges = {}
                status["running"] = True
                status["industry"] = selected_industry
                
                section_style['display'] = 'block'
                return False, f"Generating files for '{selected_industry}'...", "Stop", stop_style, loading_message, section_style, selected_industry, selected_language, export_button_style
            except Exception as e:
                return True, html.Div([
                    html.Span(f"⚠️ {str(e)}", 
                             style={'color': '#FF3621'})
                ], style={'padding': '12px'}), "Start", start_style, None, section_style, None, None, export_button_style
        else:  # Stop button was clicked
            print("\nStopping generation...")
            status["running"] = False
            status["industry"] = None
            status['start_time'] = None
            section_style['display'] = 'none'
            export_button_style['display'] = 'none'
            return True, "Stopped.", "Start", start_style, None, section_style, None, None, export_button_style

    elif trigger == 'interval-timer':
        if not status["running"] or not status["industry"]:
            raise dash.exceptions.PreventUpdate
        
        if status['start_time'] and (time.time() - status['start_time']) > 10800:
            print("\nTime limit reached...")
            status["running"] = False
            status["industry"] = None
            status['start_time'] = None
            section_style['display'] = 'none'
            export_button_style['display'] = 'none'
            return True, "Generation stopped after 3 hours.", "Start", start_style, None, section_style, None, None, export_button_style
        
        try:
            generate_files_for_industry(status["industry"])
        except Exception as e:
            # Handle any error by stopping the generation and displaying the error message
            status["running"] = False
            status["industry"] = None
            status['start_time'] = None
            section_style['display'] = 'none'
            export_button_style['display'] = 'none'
            return True, html.Div([
                html.Span(f"⚠️ {str(e)}", 
                         style={'color': '#FF3621'})
            ], style={'padding': '12px'}), "Start", start_style, None, section_style, None, None, export_button_style
        
        if status['iteration_count'] == 1 and status['dlt_code'] is None:
            print("\nGenerating DLT code...")
            try:
                schemas = load_all_schemas(status["industry"])
                dlt_codes = []
                for schema in schemas:
                    table_type = schema.get("type", "fact")
                    if table_type in ["dimension", "fact", "change_feed"]:  # Added change_feed here
                        table = schema["table"]
                        output_path = os.path.join(status['output_path'], status["industry"], table)
                        dlt_refs = generate_dlt_references(schema, output_path, table_type)
                        print(f"Generated code for table: {table} (type: {table_type})")
                        dlt_codes.append({
                            "table": table,
                            "references": dlt_refs
                        })
                
                status['dlt_code'] = create_dlt_code_display(dlt_codes, selected_language)
                print(f"Stored DLT code: {status['dlt_code'] is not None}")
                section_style['display'] = 'block'
                export_button_style['display'] = 'block'
                return False, f"Generating files for '{status['industry']}'...", "Stop", stop_style, status['dlt_code'], section_style, status["industry"], selected_language, export_button_style
            except Exception as e:
                print(f"Error generating DLT code: {str(e)}")
                section_style['display'] = 'block'
                export_button_style['display'] = 'none'
                return False, f"Generating files for '{status['industry']}'...", "Stop", stop_style, loading_message, section_style, status["industry"], selected_language, export_button_style
        
        print("\nSubsequent iteration - using stored code")
        section_style['display'] = 'block'
        export_button_style['display'] = 'block' if status['dlt_code'] else 'none'
        return False, f"Generating files for '{status['industry']}'...", "Stop", stop_style, status['dlt_code'], section_style, status["industry"], selected_language, export_button_style

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
    
    # Extract DLT codes from the stored code
    dlt_codes = []
    for child in status['dlt_code'].children:
        if isinstance(child, html.Div):
            table_name = child.children[0].children.split(': ')[1]
            # Get the code from the pre element
            code = child.children[1].children[0].children
            # Generate code for the table in the selected language
            output_path = os.path.join(status['output_path'], status['industry'], table_name)
            dlt_refs = generate_dlt_references({'table': table_name}, output_path, 'fact')
            dlt_codes.append({
                'table': table_name,
                'references': dlt_refs
            })
    
    # Create notebook content with only the selected language
    notebook_content = create_notebook_content(dlt_codes, selected_language)
    
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

@app.callback(
    Output('dlt-code-display', 'children', allow_duplicate=True),
    Input('language-dropdown', 'value'),
    prevent_initial_call=True
)
def update_language(selected_language):
    if not status['dlt_code']:
        raise dash.exceptions.PreventUpdate
    
    # Extract DLT codes from the stored code
    dlt_codes = []
    for child in status['dlt_code'].children:
        if isinstance(child, html.Div):
            table_name = child.children[0].children.split(': ')[1]
            code = child.children[1].children[0].children
            dlt_codes.append({
                'table': table_name,
                'references': {
                    'sql': code if selected_language == 'sql' else None,
                    'python': code if selected_language == 'python' else None
                }
            })
    
    return create_dlt_code_display(dlt_codes, selected_language)

if __name__ == "__main__":
    app.run(debug=True)
