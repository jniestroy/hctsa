import os
import json
import pandas as pd

def parse_matlab_file(file_path):
    param_names = []
    with open(file_path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            if line.startswith('function'):
                # Extract the part after the function keyword
                func_def = line.split('=')[-1].strip()
                # Extract the part within the parentheses
                param_str = func_def.split('(')[1].split(')')[0]
                param_names = [param.strip() for param in param_str.split(',')]
                break
    return param_names

def convert_table_to_config(df):
    config = {}

    for _, row in df.iterrows():
        formula = row['formula']
        
        # Extract the function name from the formula before the first parenthesis
        function_name = formula.split('(')[0]
        
        # Initialize an empty list for each function if not already present
        if function_name not in config:
            config[function_name] = []

        # Extract parameter values from the formula
        param_values = {}
        if '(' in formula and ')' in formula:
            param_str = '('.join(formula.split('(')[1:])[:-1]
            param_values = [param.strip() for param in param_str.split(',')]
        if function_name == 'DN_HistogramMode':
            print('next')
            print(param_values)
        
        # Load specific MATLAB file for parameter names if it exists
        matlab_file_path = f'../Operations/{function_name}.m'
        if os.path.exists(matlab_file_path):
            param_names = parse_matlab_file(matlab_file_path)
            params = {param_name: param_value for param_name, param_value in zip(param_names, param_values)}
        else:
            params = {f'param{i+1}': param for i, param in enumerate(param_values)}

        # Append the params dictionary to the list of the function
        config[function_name].append(params)

    return config

# Read the CSV file into a pandas DataFrame
df = pd.read_csv('MasterOperations.csv', names=['index', 'function_name', 'formula'])

# Convert the table to the config format
config = convert_table_to_config(df)

# Save the config dictionary to a JSON file
with open('MasterOperations.json', 'w') as f:
    json.dump(config, f, indent=2)

print("Config file created successfully.")
