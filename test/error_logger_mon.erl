%%% A gen_server to check if we get any error_logger messages during test to see if
%%% any messages gets generated when they shouldn't

-module(error_logger_mon).

-behaviour(gen_server).
-define(SERVER, ?MODULE).

-record(state, {count = 0 :: integer()}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
%% gen_server
-export([start_link/0,
         report/0,
         get_msg_count/0,
         stop/0
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

report() ->
    gen_server:call(?SERVER, report).

get_msg_count() ->
    gen_server:call(?SERVER, get_count).

stop() ->
    gen_server:call(?SERVER, stop).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([]) ->
    {ok, #state{}}.

handle_call(get_count, _From, #state{count = C} = State) ->
    {reply, C, State};
handle_call(report, _From, #state{count = C} = State) ->
    {reply, ok, State#state{count = C+1}};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, error, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
