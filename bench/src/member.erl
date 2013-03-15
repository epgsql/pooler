%% @doc A pool member used for perf testing pooler. The member has a
%% configurable start-up delay. You set a delay value and actual start
%% delay will be `delay + random:uniform(delay)'. The module supports
%% a crash function to make the member crash.
-module(member).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1,
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

start_link(Config) ->
    % not registered
    gen_server:start_link(?MODULE, Config, []).

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
          id,
          ping_count = 0
         }).

init(Config) ->
    start_up_delay(Config),
    {ok, #state{id = make_ref()}}.

%% pause server init based on start_up_delay config plus jitter (of up
%% to 2x delay)
start_up_delay(Config) ->
    case proplists:get_value(start_up_delay, Config) of
        T when is_integer(T) ->
            random:seed(erlang:now()),
            J = random:uniform(T),
            timer:sleep(T + J),
            ok;
        _ ->
            ok
    end.

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
    erlang:error({member, requested_crash});
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

