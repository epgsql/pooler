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
    gen_server:start_link(?MODULE, Args, []).

get_id(S) ->
    gen_server:call(S, get_id).

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
    {ok, #state{type = Type, id = make_ref()}}.

handle_call(get_id, _From, State) ->
    {reply, {State#state.type, State#state.id}, State};
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

