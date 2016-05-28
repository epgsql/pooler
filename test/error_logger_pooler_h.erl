%%% report handler to add to error_logger for calling error_logger_mon
%%% during test
-module(error_logger_pooler_h).

-export([init/1,
	 handle_event/2,
         handle_call/2,
         handle_info/2,
	 terminate/2]).

init(T) ->
    {ok, T}.

handle_event(_Event, Type) ->
    error_logger_mon:report(),
    {ok, Type}.

handle_info(_, Type) ->
    {ok, Type}.

handle_call(_Query, _Type) ->
    {error, bad_query}.

terminate(_Reason, _Type) ->
    [].
