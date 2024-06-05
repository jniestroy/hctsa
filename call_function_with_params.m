function result = call_function_with_params(func, series, params)
    % Convert struct fields and values to cell arrays
    param_values = struct2cell(params);
    
    % Create a cell array for the input arguments to the function
    input_args = [{series}, param_values'];
    
    % Call the function with the unpacked parameters
    result = feval(func, input_args{:});
end
