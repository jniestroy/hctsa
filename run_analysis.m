function results = run_analysis(series, config, operations_map)
    results = struct();
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
                    try
                        results.(result_key) = call_function_with_params(func, transformed_series, params);
                    catch
                        results.(result_key) = NaN;
                    end
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
                    try
                        results.(result_key) = call_function_with_params(func, transformed_series, params);
                    catch
                        results.(result_key) = NaN;
                    end
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
                try
                    results.(result_key) = call_function_with_params(func, transformed_series, params);
                catch
                    results.(result_key) = NaN;
                end
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


