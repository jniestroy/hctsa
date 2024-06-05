% Load JSON config
configData = fileread('test_config.json');
config = jsondecode(configData);

% Read parquet file
input_file = 'initial_tests.parquet';
data = parquetread(input_file);

% Separate unprocessed and processed columns
unprocessed_columns = data(:, 1:3);
processed_columns = data(:, 4:303);

% Initialize results storage
all_results = [];

% Process each series in processed_columns
for i = 1:height(processed_columns)
    series = table2array(processed_columns(i, :));
    results = run_analysis(series, config, OPERATIONS_MAP);
    results.index = i;
    flattened_results = flatten_struct(results);
    all_results = [all_results; flattened_results];
end

% Convert results to table
results_table = struct2table(all_results);

% Merge unprocessed columns with results
final_table = [unprocessed_columns, results_table];

% Write final table to a CSV file
output_csv_file = 'analysis_results.csv';
writetable(final_table, output_csv_file);

% Function to flatten a nested struct
function flat_struct = flatten_struct(s)
    fields = fieldnames(s);
    flat_struct = struct();
    for i = 1:numel(fields)
        field = fields{i};
        value = s.(field);
        if isstruct(value)
            sub_flat_struct = flatten_struct(value);
            sub_fields = fieldnames(sub_flat_struct);
            for j = 1:numel(sub_fields)
                sub_field = sub_fields{j};
                flat_struct.(sprintf('%s_%s', field, sub_field)) = sub_flat_struct.(sub_field);
            end
        else
            flat_struct.(field) = value;
        end
    end
end
