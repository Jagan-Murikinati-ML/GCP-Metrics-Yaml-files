# import os
# import json
# import pandas as pd
# from ruamel.yaml import YAML
# from ruamel.yaml.scalarstring import DoubleQuotedScalarString

# def convert_to_single_yaml(config_file):
#     # Load config
#     with open(config_file) as f:
#         config = json.load(f)

#     excel_file = config['excel_input_file']
#     output_file = config['yaml_output_file']
#     excluded_namespaces = set(config.get('excluded_namespaces', []))

#     # Read Excel
#     df = pd.read_excel(excel_file)
#     yaml_data = {'namespaces': {}}

#     for _, row in df.iterrows():
#         # Extract only the base namespace (e.g., 'storage.googleapis.com')
#         full_namespace = row['Namespace']
#         namespace = full_namespace.split('/')[0]  # Flatten deeper namespaces

#         if namespace in excluded_namespaces:
#             continue

#         # Clean up metric fields
#         metric_name = row['Metric Type'].strip()
#         metric_name_quoted = DoubleQuotedScalarString(metric_name)
#         unit_quoted = DoubleQuotedScalarString(str(row['Mapping Metric Unit'])) if pd.notna(row['Mapping Metric Unit']) else None
#         description_quoted = DoubleQuotedScalarString(row['Short Description']) if isinstance(row['Short Description'], str) else ""

#         sampling_rate = row['Sampling Rate (seconds)']
#         if pd.isna(sampling_rate):
#             sampling_rate = None
#         elif isinstance(sampling_rate, float) and sampling_rate.is_integer():
#             sampling_rate = int(sampling_rate)

#         data_delay = row['Latency (seconds)']
#         if pd.isna(data_delay):
#             data_delay = None
#         elif isinstance(data_delay, float) and data_delay.is_integer():
#             data_delay = int(data_delay)

#         # Initialize namespace if it doesn't exist
#         if namespace not in yaml_data['namespaces']:
#             yaml_data['namespaces'][namespace] = {
#                 'namespace': namespace,
#                 'metrics': {}
#             }

#         # Add metric data
#         yaml_data['namespaces'][namespace]['metrics'][metric_name_quoted] = {
#             'name': metric_name_quoted,
#             'type': row['Monitored Resource'],
#             'unit': unit_quoted,
#             'datatype': row['Metric Data Type'],
#             'regionFetcher': row['Region Fetcher'],
#             'samplingRate': sampling_rate,
#             'dataDelay': data_delay,
#             'description': description_quoted
#         }

#     # Create output folder if needed
#     os.makedirs(os.path.dirname(output_file), exist_ok=True)

#     # Dump YAML
#     yaml = YAML()
#     yaml.default_flow_style = False
#     with open(output_file, 'w') as file:
#         yaml.dump(yaml_data, file)

#     print(f"✅ Metrics YAML file generated: {output_file}")

# # Example usage
# convert_to_single_yaml("single_file_yaml_config.json")


import os
import json
import pandas as pd
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import DoubleQuotedScalarString

def convert_to_single_yaml(config_file):
    # Load config
    with open(config_file) as f:
        config = json.load(f)

    excel_file = config['excel_input_file']
    output_file = config['yaml_output_file']
    excluded_namespaces = set(config.get('excluded_namespaces', []))

    # Read Excel
    df = pd.read_excel(excel_file)
    yaml_data = {'namespaces': {}}

    for _, row in df.iterrows():
        # Extract only the base namespace (e.g., 'storage.googleapis.com')
        full_namespace = row['Namespace']
        namespace = full_namespace.split('/')[0]  # Flatten deeper namespaces

        if namespace in excluded_namespaces:
            continue

        # Clean up metric fields
        metric_name = row['Metric Type'].strip()
        metric_name_quoted = DoubleQuotedScalarString(metric_name)
        unit_quoted = DoubleQuotedScalarString(str(row['Mapping Metric Unit'])) if pd.notna(row['Mapping Metric Unit']) else None
        description_quoted = DoubleQuotedScalarString(row['Short Description']) if isinstance(row['Short Description'], str) else ""

        sampling_rate = row['Sampling Rate (seconds)']
        if pd.isna(sampling_rate):
            sampling_rate = None
        elif isinstance(sampling_rate, float) and sampling_rate.is_integer():
            sampling_rate = int(sampling_rate)

        data_delay = row['Latency (seconds)']
        if pd.isna(data_delay):
            data_delay = None
        elif isinstance(data_delay, float) and data_delay.is_integer():
            data_delay = int(data_delay)

        # 🆕 Add dataConvertor logic based on Metric Kind
        metric_kind = str(row.get('Kind', '')).strip().lower()
        if metric_kind == "delta":
            data_converter = DoubleQuotedScalarString("{data}/60")
        else:
            data_converter = DoubleQuotedScalarString("")

        # Initialize namespace if it doesn't exist
        if namespace not in yaml_data['namespaces']:
            yaml_data['namespaces'][namespace] = {
                'namespace': namespace,
                'metrics': {}
            }

        # Add metric data
        yaml_data['namespaces'][namespace]['metrics'][metric_name_quoted] = {
            'name': metric_name_quoted,
            'type': row['Monitored Resource'],
            'unit': unit_quoted,
            'dataConvertor': data_converter,   # ✅ Added here
            'datatype': row['Metric Data Type'],
            'regionFetcher': row['Region Fetcher'],
            'samplingRate': sampling_rate,
            'dataDelay': data_delay,
            'description': description_quoted
        }

    # Create output folder if needed
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Dump YAML
    yaml = YAML()
    yaml.default_flow_style = False
    with open(output_file, 'w') as file:
        yaml.dump(yaml_data, file)

    print(f"✅ Metrics YAML file generated: {output_file}")

# Example usage
convert_to_single_yaml("single_file_yaml_config.json")


