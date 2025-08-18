import os
import json
import pandas as pd
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import DoubleQuotedScalarString

def sanitize_filename(name):
    return name.lower().replace("/", "_").replace(" ", "_")

def convert_to_yaml_per_resource(config):
    excel_file = config['excel_input_file']
    output_dir = config['yaml_output_dir']
    excluded_namespaces = set(config.get('excluded_namespaces', []))

    df = pd.read_excel(excel_file)

    os.makedirs(output_dir, exist_ok=True)

    for monitored_resource in df['Monitored Resource'].dropna().unique():
        subset_df = df[df['Monitored Resource'] == monitored_resource]
        yaml_data = {'namespaces': {}}

        for _, row in subset_df.iterrows():
            namespace = row['Namespace']
            if namespace in excluded_namespaces:
                continue

            metric_name = '/'.join(row['Metric Type'].split('/')[1:]).strip()
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
            metric_kind = row.get('Kind', '').strip().lower()
            if metric_kind == "delta":
                data_converter = DoubleQuotedScalarString("{data}/60")
            else:
                data_converter = DoubleQuotedScalarString("")

            if namespace not in yaml_data['namespaces']:
                yaml_data['namespaces'][namespace] = {
                    'namespace': namespace,
                    'metrics': {}
                }

            yaml_data['namespaces'][namespace]['metrics'][metric_name_quoted] = {
                'name': metric_name_quoted,
                'type': row['Monitored Resource'],
                'unit': unit_quoted,
                'dataConvertor': data_converter,
                'datatype': row['Metric Data Type'],
                'regionFetcher': row['Region Fetcher'],
                'samplingRate': sampling_rate,
                'dataDelay': data_delay,
                'description': description_quoted
                  
            }

        filename = f"{sanitize_filename(monitored_resource)}.yaml"
        output_path = os.path.join(output_dir, filename)

        yaml = YAML()
        yaml.default_flow_style = False
        with open(output_path, 'w') as file:
            yaml.dump(yaml_data, file)

        print(f"✅ Generated: {output_path}")

if __name__ == "__main__":
    with open("updated_yaml_config.json", "r") as f:
        config = json.load(f)
    convert_to_yaml_per_resource(config)
