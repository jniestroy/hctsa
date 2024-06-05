import json

def convert_values(data):
    if isinstance(data, dict):
        for key, value in data.items():
            data[key] = convert_values(value)
    elif isinstance(data, list):
        for i in range(len(data)):
            data[i] = convert_values(data[i])
    elif isinstance(data, str):
        if data.isdigit():
            return int(data)
        try:
            float_value = float(data)
            if '.' in data:
                return float_value
        except ValueError:
            pass
        if data.lower() == "true":
            return True
        elif data.lower() == "false":
            return False
    return data

def main():
    # Load the JSON file
    with open('MasterOperations.json', 'r') as file:
        data = json.load(file)

    # Convert strings to appropriate types
    cleaned_data = convert_values(data)

    # Write the cleaned data to a new file
    with open('cleanedMasterOperations.json', 'w') as file:
        json.dump(cleaned_data, file, indent=2)

if __name__ == '__main__':
    main()
