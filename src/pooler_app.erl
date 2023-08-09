-module(pooler_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, config_change/3]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    pooler_sup:start_link().

stop(_State) ->
    ok.

config_change(_Changed, _New, _Removed) ->
    %% TODO: implement.
    %% Only 3 keys are in use right now:
    %% * pools (would require a custom diff function)
    %% * metrics_module
    %% * metrics_api
    ok.
