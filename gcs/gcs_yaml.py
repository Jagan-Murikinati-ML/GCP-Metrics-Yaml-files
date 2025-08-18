

import os
import pandas as pd
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import DoubleQuotedScalarString

def convert_to_single_yaml(excel_file, output_file):
    df = pd.read_excel(excel_file)

    excluded_namespaces = {
        'firewallinsights.googleapis.com/subnet',
        'firewallinsights.googleapis.com/vm',
        'agent.googleapis.com/processes',
        'agent.googleapis.com/gpu',
        'networking.googleapis.com/vm_flow',
        'agent.googleapis.com/disk',
        'agent.googleapis.com/memory',
        'agent.googleapis.com/cpu',
        'compute.googleapis.com/guest'
    }

    yaml_data = {'namespaces': {}}

    for _, row in df.iterrows():
        namespace = "storage.googleapis.com"
        if namespace in excluded_namespaces:
            continue

        metric_name = '/'.join(row['Metric Type'].split('/')[0:]).strip()
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

        if namespace not in yaml_data['namespaces']:
            yaml_data['namespaces'][namespace] = {
                'namespace': namespace,
                'metrics': {}
            }

        yaml_data['namespaces'][namespace]['metrics'][metric_name_quoted] = {
            'name': metric_name_quoted,
            'type': row['Monitored Resource'],
            'unit': unit_quoted,
            'datatype': row['Metric Data Type'],
            'regionFetcher': row['Region Fetcher'],
            'samplingRate': sampling_rate,
            'dataDelay': data_delay,
            'description': description_quoted
        }

    yaml = YAML()
    yaml.default_flow_style = False
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, 'w') as file:
        yaml.dump(yaml_data, file)

    print(f"✅ Single YAML file generated: {output_file}")

# Example usage
convert_to_single_yaml("gcs_metrics.xlsx", "gcs_output_yaml_files/gcs_metrics.yaml")

