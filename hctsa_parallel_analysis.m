% Load JSON config
configData = fileread('./Convert/filteredMasterOperations.json');
config = jsondecode(configData);

input_file = 'all_samples.parquet';
data = parquetread(input_file);

% Identify rows with missing data in processed columns
rows_with_missing = any(ismissing(data(:, 5:304)), 2);

% Remove rows with missing data in processed columns
data_cleaned = data(~rows_with_missing, :);

% Separate unprocessed and processed columns
unprocessed_columns = data_cleaned(:, 1:4);
processed_columns = data_cleaned(:, 5:304);


% Initialize results storage
all_results = cell(height(processed_columns), 1);
all_fieldnames = {};

% Initialize compute time storage
compute_times = struct();

% Set up parallel pool
if isempty(gcp('nocreate'))
    parpool(8);
end

% Ensure parallel environment is set up correctly
parfevalOnAll(@() fprintf('Parallel environment set up.\n'), 0);

% Suppress warnings and prints
warning('off', 'all');
parfevalOnAll(@() warning('off', 'all'), 0);
parfevalOnAll(@() evalc('disp(''Suppressing prints'')'), 0);

% Process each series in processed_columns in parallel
parfor i = 1:height(processed_columns)
    series = table2array(processed_columns(i, :));
    [results, times] = run_analysis(series, config, OPERATIONS_MAP);
    results.index = i;
    flattened_results = flatten_struct(results);
    
    % Store flattened results in cell array
    all_results{i} = flattened_results;
    
    % Store compute times
    compute_times(i).times = times;
    
    % Display progress every 100 rows
    if mod(i, 100) == 0
        fprintf('Processed %d out of %d rows.\n', i, height(processed_columns));
    end
end

% Collect all field names
for i = 1:numel(all_results)
    all_fieldnames = union(all_fieldnames, fieldnames(all_results{i}));
end

% Ensure all results have consistent fields
for i = 1:numel(all_results)
    missing_fields = setdiff(all_fieldnames, fieldnames(all_results{i}));
    for j = 1:numel(missing_fields)
        all_results{i}.(missing_fields{j}) = NaN;
    end
end

% Convert results to table with consistent fields
results_table = struct2table([all_results{:}]);

% Merge unprocessed columns with results
final_table = [unprocessed_columns, results_table];

% Save final table to a .mat file
output_mat_file = 'analysis_results.mat';
save(output_mat_file, 'final_table');

% Save compute times to a .mat file
output_times_file = 'compute_times.mat';
save(output_times_file, 'compute_times');

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

function [results, times] = run_analysis(series, config, operations_map)
    results = struct();
    times = struct();
    config_fields = fieldnames(config);
    for i = 1:numel(config_fields)
        func_name = config_fields{i};
        param_sets = config.(func_name);
        if isKey(operations_map, func_name)
            func = operations_map(func_name);
            if iscell(param_sets) % Handle cell array of parameter sets
                for j = 1:numel(param_sets)
                    params = param_sets{j};
                    % Check for transform
                    if isfield(params, 'transform')
                        transform = params.transform;
                        params = rmfield(params, 'transform'); % Remove the transform field
                        transformed_series = apply_transform(series, transform);
                    else
                        transformed_series = series;
                    end
                    
                    param_str = '';
                    param_fields = fieldnames(params);
                    for k = 1:numel(param_fields)
                        field = param_fields{k};
                        value = params.(field);
                        param_str = [param_str, sprintf('%s_%s_', field, num2str(value))];
                    end
                    param_str = param_str(1:end-1); % Remove trailing underscore
                    param_str = regexprep(param_str, '[^a-zA-Z0-9_]', '_'); % Replace invalid characters
                    result_key = sprintf('%s_%s', func_name, param_str);
                    
                    % Track computation time
                    tic;
                    try
                        results.(result_key) = call_function_with_params(func, transformed_series, params);
                    catch
                        results.(result_key) = NaN;
                    end
                    times.(result_key) = toc;
                end
            elseif numel(param_sets) > 1 % Handle struct arrays
                for j = 1:numel(param_sets)
                    params = param_sets(j);
                    % Check for transform
                    if isfield(params, 'transform')
                        transform = params.transform;
                        params = rmfield(params, 'transform'); % Remove the transform field
                        transformed_series = apply_transform(series, transform);
                    else
                        transformed_series = series;
                    end
                    
                    param_str = strjoin(cellfun(@(k, v) sprintf('%s_%s', k, num2str(v)), ...
                        fieldnames(params), struct2cell(params), 'UniformOutput', false), '_');
                    param_str = regexprep(param_str, '[^a-zA-Z0-9_]', '_'); % Replace invalid characters
                    result_key = sprintf('%s_%s', func_name, param_str);
                    
                    % Track computation time
                    tic;
                    try
                        results.(result_key) = call_function_with_params(func, transformed_series, params);
                    catch
                        results.(result_key) = NaN;
                    end
                    times.(result_key) = toc;
                end
            else % Handle single struct
                params = param_sets;
                % Check for transform
                if isfield(params, 'transform')
                    transform = params.transform;
                    params = rmfield(params, 'transform'); % Remove the transform field
                    transformed_series = apply_transform(series, transform);
                else
                    transformed_series = series;
                end
                
                param_str = '';
                param_fields = fieldnames(params);
                for k = 1:numel(param_fields)
                    field = param_fields{k};
                    value = params.(field);
                    param_str = [param_str, sprintf('%s_%s_', field, num2str(value))];
                end
                param_str = param_str(1:end-1); % Remove trailing underscore
                param_str = regexprep(param_str, '[^a-zA-Z0-9_]', '_'); % Replace invalid characters
                result_key = sprintf('%s_%s', func_name, param_str);
                
                % Track computation time
                tic;
                try
                    results.(result_key) = call_function_with_params(func, transformed_series, params);
                catch
                    results.(result_key) = NaN;
                end
                times.(result_key) = toc;
            end
        else
            results.(func_name) = "Missing Operation";
        end
    end
end

function transformed_series = apply_transform(series, transform)
    switch transform
        case 'x_z'
            transformed_series = (series - mean(series)) / std(series);
        case 'abs(x_z)'
            transformed_series = abs((series - mean(series)) / std(series));
        case 'zscore(abs(x_z))'
            abs_series = abs((series - mean(series)) / std(series));
            transformed_series = (abs_series - mean(abs_series)) / std(abs_series);
        case 'diff(x_z)'
            z_scored_series = (series - mean(series)) / std(series);
            transformed_series = diff(z_scored_series);
        case 'zscore(sign(x_z))'
            z_scored_series = (series - mean(series)) / std(series);
            sign_series = sign(z_scored_series);
            transformed_series = (sign_series - mean(sign_series)) / std(sign_series);
        otherwise
            error('Unknown transform type ''%s''', transform);
    end
end

function result = call_function_with_params(func, series, params)
    % Convert struct fields and values to cell arrays
    param_values = struct2cell(params);
    
    % Create a cell array for the input arguments to the function
    input_args = [{series}, param_values'];
    
    % Call the function with the unpacked parameters
    result = feval(func, input_args{:});
end
