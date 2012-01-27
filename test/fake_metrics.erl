%% @doc This stub module mimics part of folsom's API. It allows us to
%% test the metrics instrumentation of pooler without introducing a
%% dependency on folsom or another metrics application.
%%
-module(fake_metrics).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-include_lib("eunit/include/eunit.hrl").
%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0,
         notify/3,
         get_metrics/0,
         reset_metrics/0,
         stop/0
        ]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

notify(Name, Value, Type) ->
    gen_server:cast(?SERVER, {Name, Value, Type}).

reset_metrics() ->
    gen_server:call(?SERVER, reset).

stop() ->
    gen_server:call(?SERVER, stop).

get_metrics() ->
    gen_server:call(?SERVER, get_metrics).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
-record(state, {
          metrics = []
         }).

init(_) ->
    {ok, #state{}}.

handle_call(reset, _From, State) ->
    {reply, ok, State#state{metrics = []}};
handle_call(get_metrics, _From, #state{metrics = Metrics}=State) ->
    {reply, Metrics, State};
handle_call(stop, _From, State) ->
    {stop, normal, stop_ok, State};
handle_call(_Request, _From, State) ->
    erlang:error({what, _Request}),
    {noreply, ok, State}.

handle_cast({_N, _V, _T}=M, #state{metrics = Metrics} = State) ->
    {noreply, State#state{metrics = [M|Metrics]}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

