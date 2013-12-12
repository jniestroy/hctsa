% SQL_getids
% 
% Takes as input a set of constraints on the time series and operations to
% include then runs the appropriate mySQL commands and outputs the relevant
% ts_ids / op_ids
% 
%---INPUTS:
% TsorOps: specifies 'ops' for metrics or 'ts' for time series
% LengthRange (ts): contrain included time series by length [minimum_length maximum_length] (1x2 vector)
% LengthRange (mets): takes roll of masterpull: whether (1) or not (0) to pull
%                in other master-linked operations
% KeywordInclude: constrain included time series by keyword -- nx2 cell
% 		(i) keyword
% 		(ii) how many of that keyword (0==all available)
% KeywordRemove: keywords NOT to include (cell of strings)
% idr: range of possible ids to restrict the search to (empty = don't
%                    restrict -- default)
% howtolimit: can select one of:
%                'rand' (limit cases by random)
%                'pcmax' (limit cases by maximum percentage calculated)
%                'pcmin' (limit cases by minimum percentage calculated)
% dbc: can specify database; otherwise opens the default specified in
%         SQL_opendatabase
% 
%---OUTPUTS
% ids: a vector or either ts_ids or op_ids that match the input constraints
% 
% ------------------------------------------------------------------------------
% Copyright (C) 2013,  Ben D. Fulcher <ben.d.fulcher@gmail.com>,
% <http://www.benfulcher.com>
% 
% If you use this code for your research, please cite:
% B. D. Fulcher, M. A. Little, N. S. Jones., "Highly comparative time-series
% analysis: the empirical structure of time series and their methods",
% J. Roy. Soc. Interface 10(83) 20130048 (2010). DOI: 10.1098/rsif.2013.0048
% 
% This work is licensed under the Creative Commons
% Attribution-NonCommercial-ShareAlike 3.0 Unported License. To view a copy of
% this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/ or send
% a letter to Creative Commons, 444 Castro Street, Suite 900, Mountain View,
% California, 94041, USA.
% ------------------------------------------------------------------------------

function ids = SQL_getids(TsorOps,LengthRange,KeywordInclude,KeywordRemove,idr,howtolimit,dbname)
%%% FOREPLAY
%% Check inputs -- set defaults
% if nargin < 5; disp('You must provide 5 inputs! And no, I''m not asking nicely!!'); return; end

if strcmp(TsorOps,'ts')
	if isempty(LengthRange)
		LengthRange = [200, 20000];
		fprintf(1,'Setting default length constraints: %u--%u\n',LengthRange(1),LengthRange(2))
	end
elseif strcmp(TsorOps,'ops')
	masterpull = LengthRange;
	if isempty(masterpull)
		masterpull = 1; % retrieves other outputs of all master functions implicated in the range
		fprintf(1,'Pulling in all pointers by default\n');
	end
else
	error('First input must be either ''ts'' or ''ops''.');
end

if nargin < 4
    KeywordRemove = {};
end

if nargin < 5
	idr = [];
end
if nargin < 6 || isempty(howtolimit)
    howtolimit = 'pcmax';
end
if nargin < 7
	dbname = ''; % Use default database by default
end

% seperate into keywords to include: kyes
% and the number of that keyword to include: kyesn
if ~isempty(KeywordInclude)
    kyes = KeywordInclude(:,1); kyesn = vertcat(KeywordInclude{:,2});
else
    kyes = {}; kyesn = [];
end

%% Open connection to mySQL database
dbc = SQL_opendatabase(dbname);

%%% TIME SERIES
if strcmp(TsorOps,'ts')

	%% Filter time series by keyword
	% keywords as a cell: kyes; and how many of each to include: kyesn
	s = {'','','',''};
	
	% Extra qualifier to only look in a certain range
	% s{1} -- IDR
	if ~isempty(idr)
		s{1} = sprintf('ts_id BETWEEN %u AND %u',idr(1),idr(2));
	end

	% Extra qualifier to not include certain keywords -- this is performed seperately in the query for each keyword *to* include
	% s{2} -- KeywordRemove
	if ~isempty(KeywordRemove)
		s{2} = ['ts_id NOT IN (SELECT ts_id FROM TsKeywordsRelate WHERE tskw_id IN ' ...
							'(SELECT tskw_id FROM TimeSeriesKeywords WHERE Keyword IN (' BF_cat(KeywordRemove,',','''') '))) '];
	end
	
    % % Extra qualifier pcalcr to have PercentageCalculated only in a certain range
    % % s{3} -- pcalcr
    % if ~isempty(pcalcr)
    %     s{3} = sprintf('PercentageCalculated BETWEEN %f AND %f',pcalcr(1),pcalcr(2));
    % end
	
	% Length constraint in words
	s{3} = sprintf('TimeSeries.Length BETWEEN %u AND %u',LengthRange(1),LengthRange(2));
	
	% Combine these results into part of a mySQL query string
	conditions = '';
	for j = 1:length(s)
		if ~isempty(s{j})
			conditions = [conditions ' AND ' s{j}];
		end
	end

	%% kyes, keywords to include
	if ~isempty(kyes)
		C_tskwyes = cell(length(kyes),1);
		for i = 1:length(kyes)
			ncut = kyesn(i);

			% Now do the rest of the query at once: do the keyword matches and length constraints		
			if ncut == 0 % Get all keyword matches
				SelectString = ['SELECT ts_id FROM TimeSeries WHERE ' ...
								'ts_id IN (SELECT ts_id FROM TsKeywordsRelate WHERE tskw_id = '  ...
								'(SELECT tskw_id FROM TimeSeriesKeywords WHERE Keyword = ''' kyes{i} '''))' ...
								conditions];
			else % constrain to some number (using the LIMIT command in mySQL)
                switch howtolimit
                    case 'pcmax'
                        limitme = sprintf(' ORDER BY PercentageCalculated DESC LIMIT %u',ncut);
                    case 'pcmin'
                        limitme = sprintf(' ORDER BY PercentageCalculated LIMIT %u',ncut);
                    case 'rand'
                        limitme = sprintf(' ORDER BY RAND() LIMIT %u',ncut);
                        % I know this is a slow implementation -- probably better
                        % to create the random numbers here to constrain
                end
                
				if isempty(kyes{i})
					SelectString = ['SELECT ts_id FROM TimeSeries WHERE ' ...
									conditions(6:end) limitme];

                else		
					% constrain by PercentageCalculated
					SelectString = ['SELECT ts_id FROM TimeSeries WHERE ' ...
									'ts_id IN (SELECT ts_id FROM TsKeywordsRelate WHERE tskw_id = '  ...
									'(SELECT tskw_id FROM TimeSeriesKeywords WHERE Keyword = ''' kyes{i} ''')) ' ...
									conditions limitme];
				end
			end

			% execute the select statement
			[ts_ids,~,~,emsg] = mysql_dbquery(dbc,SelectString);
			if isempty(ts_ids) && isempty(emsg)
				fprintf(1,'No time series matched the given constraints for the keyword ''%s''\n',kyes{i});
			elseif ~isempty(emsg)
				fprintf(1,'Error retrieving time series for %s\n',kyes{i});
				disp(emsg)
				keyboard
			else
				C_tskwyes{i} = vertcat(ts_ids{:});
				ngot = length(C_tskwyes{i});
			
				if isempty(kyes{i})
					fprintf(1,'Found %u time series selected by %s\n',ngot,howtolimit);
				else
					fprintf(1,'Found %u time series with keyword ''%s''\n',ngot,kyes{i});
				end
			end
		end
	
		% Finally, now keep all that are left
		ts_ids_keep = vertcat(C_tskwyes{:});
		lbefore = length(ts_ids_keep);
		ts_ids_keep = unique(ts_ids_keep);
		lafter = length(ts_ids_keep);
		if lafter<lbefore
			fprintf(1,'We''ve lost %u time series to keyword overlap...\n',lbefore-lafter);
		end
	
	else % just use the other constraints
		% if isempty(KeywordRemove)
		SelectString = ['SELECT ts_id FROM TimeSeries WHERE ' conditions(6:end)];
		% else
		% 	SelectString = ['SELECT ts_id FROM TimeSeries WHERE ' ss_tsl ' ' KeywordRemoveextrastring];
		% end
		[ts_ids,~,~,emsg] = mysql_dbquery(dbc,SelectString);
		if ~isempty(emsg)
			fprintf(1,'Database call failed\n%s\n',SelectString); disp(emsg); keyboard
		else
			ts_ids_keep = unique(vertcat(ts_ids{:}));
		end
	end

	% We now have ts_ids_keep -- the keyword and length-constrained time series
	nts = length(ts_ids_keep); % number of time series
	if nts == 0
		fprintf(1,'No Time Series found.\n');
		ids = [];
	else
		fprintf(1,'Time Series Filtered: %u\n',nts);
		ids = ts_ids_keep;
	end
	
else
	%%% Operations

	s = {'','',''};
	
	% Extra qualifier to only look in a certain range
	% s{1} -- IDR
	if ~isempty(idr)
		s{1} = ['op_id BETWEEN ' num2str(idr(1)) ' AND ' num2str(idr(2))];
	end

	% Extra qualifier to not include certain keywords -- this is performed seperately in the query for each keyword *to* include
	% s{2} -- KeywordRemove
	if ~isempty(KeywordRemove)
		s{2} = ['op_id NOT IN (SELECT op_id FROM OpKeywordsRelate WHERE opkw_id IN ' ...
							'(SELECT opkw_id FROM OperationKeywords WHERE Keyword IN (' BF_cat(KeywordRemove,',','''') '))) '];
	end
	
    % % Extra qualifier pcalcr to have PercentageCalculated only in a certain range
    % % s{3} -- pcalcr
    % if ~isempty(pcalcr)
    %     s{3} = ['PercentageCalculated BETWEEN ' num2str(pcalcr(1)) ' AND ' num2str(pcalcr(2))];
    % end
	
	% Combine these results into part of a mySQL query string
	conditions='';
	for j = 1:length(s)
		if ~isempty(s{j})
			conditions = [conditions ' AND ' s{j}];
		end
	end
	
	% if ~isempty(KeywordRemove)
	% 	mnoextrastring = ['AND op_id NOT IN (SELECT op_id FROM OpKeywordsRelate WHERE opkw_id IN ' ...
	% 						'(SELECT opkw_id FROM OperationKeywords WHERE Keyword IN (' BF_cat(mno,',','''') '))) '];
	% else
	% 	mnoextrastring = '';
	% end

	if ~isempty(kyes)
		C_kyes = cell(length(kyes),1);
		for i=1:length(kyes)
			ncut = kyesn(i);

			% Now do the rest of the query at once: do the keyword matches and length constraints
		
			if ncut==0 % Get all matches			
				SelectString = ['SELECT op_id FROM Operations WHERE ' ...
								'op_id IN (SELECT op_id FROM OpKeywordsRelate WHERE opkw_id = '  ...
								'(SELECT opkw_id FROM OperationKeywords WHERE Keyword = ''' kyes{i} '''))' ...
								conditions];
			else % constrain to some number (using the LIMIT command in mySQL)
                switch howtolimit
                    case 'pcmax'
                        limitme = [' ORDER BY PercentageCalculated DESC LIMIT ' num2str(ncut)];
                    case 'pcmin'
                        limitme = [' ORDER BY PercentageCalculated LIMIT ' num2str(ncut)];
                    case 'rand'
                        limitme = [' ORDER BY RAND() LIMIT ' num2str(ncut)];
                        % I know this is a slow implementation -- probably better
                        % to create the random numbers here to constrain
                end
                
                
				if isempty(kyes{i}) % empty keyword -- just constrain by number
					if isempty(conditions)
						SelectString = ['SELECT op_id FROM Operations' limitme];
					else
						SelectString = ['SELECT op_id FROM Operations WHERE ' conditions(6:end) ...
											limitme];
					end
				else
					SelectString = ['SELECT op_id FROM Operations WHERE ' ...
									'op_id IN (SELECT op_id FROM OpKeywordsRelate WHERE opkw_id = '  ...
									 '(SELECT opkw_id FROM OperationKeywords WHERE Keyword = ''' kyes{i} '''))' ...
									   conditions limitme];
				end
			end
		
			% Execute the query
			[op_ids,qrf,rs,emsg] = mysql_dbquery(dbc,SelectString);
		
			if ~isempty(emsg)
				fprintf(1,'Error finding %s\n',kyes{i}); disp(emsg); keyboard
			end
			C_kyes{i} = vertcat(op_ids{:});
		
			ngot = length(C_kyes{i});
			if isempty(kyes{i})
				fprintf(1,'Found %u operations chosen by %s\n',ngot,howtolimit);
			else
				fprintf(1,'Found %u operations with keyword ''%s''\n',ngot,kyes{i});
			end
		end
	
		% Agglomerate all the bits
		op_ids_keep = vertcat(C_kyes{:});
		lbefore = length(op_ids_keep);
		op_ids_keep = unique(op_ids_keep);
		lafter = length(op_ids_keep);
		if lafter < lbefore
			fprintf(1,'We lost %u  to overlapping keywords!\n',lbefore-lafter);
		end
	
	else % just use the length/KeywordRemove constraint
		if isempty(conditions)
			SelectString = 'SELECT op_id FROM Operations'; % include all operations -- exclude nothing
		else
			SelectString = ['SELECT op_id FROM Operations WHERE ' conditions(6:end)]; % (remove "AND ")
		end
		[op_ids,~,~,emsg] = mysql_dbquery(dbc,SelectString);
		if ~isempty(emsg)
			fprintf(1,'Database call failed\n%s\n',SelectString); disp(emsg); keyboard
		else
			op_ids_keep = unique(vertcat(op_ids{:}));
		end
	end

	%% Get other metrics which point to master functions which will be called anyway
	if masterpull && ~isempty(op_ids_keep)
		% Find implicated Master functions (a super inefficient way of doing it:)
		SelectString = ['SELECT op_id FROM Operations WHERE mop_id IN (SELECT DISTINCT mop_id FROM Operations WHERE op_id IN (' BF_cat(op_ids_keep,',') '))'];
		[newmids,~,~,emsg] = mysql_dbquery(dbc,SelectString);
		if isempty(emsg)
			if ~isempty(newmids) % there are some master functions implicated
				newmids = vertcat(newmids{:});
				nm = length(op_ids_keep); % number of metrics
				op_ids_keep = union(op_ids_keep,newmids); % include the new ones
				if length(op_ids_keep) > nm
					fprintf(1,'%u additional operations were included as implicated by existing master functions\n',length(op_ids_keep)-nm);
				end
			end
		else % an error
			fprintf(1,'Error retrieving the operations from implicated master functions\n%s',emsg); keyboard
		end
	end

	nm = length(op_ids_keep); % number of metrics
	if nm == 0
		fprintf(1,'No matching operations found.\n');
		ids = [];
	else
		fprintf(1,'Operations filtered: %u\n',nm);
		ids = op_ids_keep;
	end
end

% if tolog, fclose(flog), end
SQL_closedatabase(dbc)

end