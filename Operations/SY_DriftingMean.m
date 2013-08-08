% SY_DriftingMean
% 
% This function implements an idea found in the Matlab Central forum:
% http://www.mathworks.de/matlabcentral/newsreader/view_thread/136539
% 
% >> It seems to me that you are looking for a measure for a drifting mean.
% >> If so, this is what I would try:
% >> 
% >> - Decide on a frame length N
% >> - Split your signal in a number of frames of length N
% >> - Compute the means of each frame
% >> - Compute the variance for each frame
% >> - Compare the ratio of maximum and minimum mean
% >>   with the mean variance of the frames.
% >> 
% >> Rune
% 
% This operation splits the time series into segments, computes the mean and
% variance in each segment and compares the maximum and minimum mean to the mean
% variance.
% 
% INPUTS:
% y, the input time series
% 
% howl, (i) 'fix': fixed-length segments (of length l)
%       (ii) 'num': a given number, l, of segments
%       
% l, either the length ('fix') or number of segments ('num')
% 

function out = SY_DriftingMean(y,howl,l)
% Ben Fulcher, 2009

N = length(y); % length of the input time series

% Check inputs
if nargin < 2 || isempty(howl)
    howl = 'num'; % a specified number of segments
end
if strcmp(howl,'num')
    l = floor(N/l);
elseif ~strcmp(howl,'fix')
    error('Unknown input setting ''%s''',howl)
end

if nargin < 3 || isempty(l)
    switch howl
    case 'num'
        l = 5; % 5 segments
    case 'fix'
        l = 200; % 200-sample segments
    end
end

% ++BF 19/3/2010
if N < l % doesn't make sense to split into more windows than there are data points
    fprintf(1,'Time Series (N = %u < l = %u) is too short for this operation\n',N,l);
    out = NaN; return
end

% Get going
nfits = floor(N/l);
z = zeros(l,nfits);
for i = 1:nfits; % number of times l fits completely into N
    z(:,i) = y((i-1)*l+1:i*l);
end
zm = mean(z);
zv = var(z);
meanvar = mean(zv);
maxmean = max(zm);
minmean = min(zm);
meanmean = mean(zm);

out.max = maxmean/meanvar;
out.min = minmean/meanvar;
out.mean = meanmean/meanvar;
out.meanmaxmin = (out.max+out.min)/2;
out.meanabsmaxmin = (abs(out.max)+abs(out.min))/2;

end