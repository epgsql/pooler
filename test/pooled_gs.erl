%% @doc A gen_server module that can be pooled by pooler.  This module
%% is used to test and demo pooler and plays the role of the pooled
%% member.
%%
%% A pooled_gs is started with a `Type' which is intended to help
%% identify the members for testing multi-pool scenarios. Each
%% pooled_gs also sets a unique id. The type and id are retrieved
%% using `get_id/1'.
-module(pooled_gs).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1,
         get_id/1,
         ping/1,
         ping_count/1,
         crash/1,
         do_work/2,
         stop/1
        ]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Args ={_Type}) ->
    % not registered
    gen_server:start_link(?MODULE, Args, []);

start_link(Args ={_Type, _InitFun}) ->
    % not registered
    gen_server:start_link(?MODULE, Args, []).

%% @doc return the type argument passed to this worker at start time
%% along with this worker's unique id.
get_id(S) ->
    gen_server:call(S, get_id).

%% @doc In processing this request, the server will sleep for a time
%% determined by a call to `random:uniform(T)'. Can be useful in
%% stress-testing the pool to simulate consumers taking a member out
%% of the pool for a varied request time.
do_work(S, T) ->
    gen_server:call(S, {do_work, T}).

ping(S) ->
    gen_server:call(S, ping).

ping_count(S) ->
    gen_server:call(S, ping_count).

crash(S) ->
    gen_server:cast(S, crash),
    sent_crash_request.

stop(S) ->
    gen_server:call(S, stop).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
-record(state, {
          type = "",
          id,
          ping_count = 0
         }).

init({Type}) ->
    {ok, #state{type = Type, id = make_ref()}};
init({Type, StartFun}) ->
    StartFun(),
    {ok, #state{type = Type, id = make_ref()}}.



handle_call(get_id, _From, State) ->
    {reply, {State#state.type, State#state.id}, State};
handle_call({do_work, T}, _From, State) ->
    Sleep = random:uniform(T),
    timer:sleep(Sleep),
    {reply, {ok, Sleep}, State};
handle_call(ping, _From, #state{ping_count = C } = State) ->
    State1 = State#state{ping_count = C + 1},
    {reply, pong, State1};
handle_call(ping_count, _From, #state{ping_count = C } = State) ->
    {reply, C, State};
handle_call(stop, _From, State) ->
    {stop, normal, stop_ok, State};
handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

handle_cast(crash, _State) ->
    erlang:error({pooled_gs, requested_crash});
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

