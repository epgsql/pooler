%% @doc A consumer of pool members used for perf testing pooler. The
%% consumer has a configurable think time for how long it keeps a
%% member checked out, how many take/return cycles it performs. Jitter
%% is added to think time. You can also request a consumer to crash or
%% trigger a member crash.
-module(consumer).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-export([start_link/0,
         run/2
        ]).

-export([
         code_change/3,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         init/1,
         terminate/2
        ]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    % not registered
    gen_server:start_link(?MODULE, [], []).

run(S, Config) ->
    SelfCrash = proplists:get_value(consumer_crash, Config) =:= true,
    MemberCrash = proplists:get_value(member_crash, Config) =:= true,
    TakeCycles = proplists:get_value(take_cycles, Config),
    ThinkTime = proplists:get_value(think_time, Config),
    PoolName = proplists:get_value(pool_name, Config),
    gen_server:call(S, {run, PoolName, SelfCrash, MemberCrash,
                        TakeCycles, ThinkTime},
                    ThinkTime * 3 * TakeCycles).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
-record(state, {
          id,
          ping_count = 0
         }).

init([]) ->
    Now = erlang:now(),
    random:seed(Now),
    {ok, #state{id = Now}}.

handle_call({run, PoolName, SelfCrash, MemberCrash,
             TakeCycles, ThinkTime}, _From, State) ->
    CrashData = crash_data(SelfCrash, MemberCrash, TakeCycles),
    run_cycles(ThinkTime, TakeCycles, CrashData, PoolName),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

run_cycles(_ThinkTime, 0, _, _) ->
    done;
run_cycles(_ThinkTime, CrashIdx, {CrashIdx, _}, _) ->
    %% self crash
    erlang:error({consumer, self_crash_requested});
run_cycles(ThinkTime, CrashIdx, {_, CrashIdx} = CrashData, PoolName) ->
    %% member crash request
    M = pooler:take_member(PoolName),
    member:crash(M),
    run_cycles(ThinkTime, CrashIdx - 1, CrashData, PoolName);
run_cycles(ThinkTime, Idx, CrashData, PoolName) ->
    M = pooler:take_member(PoolName),
    Think = ThinkTime + random:uniform(ThinkTime),
    timer:sleep(Think),
    pooler:return_member(PoolName, M),
    run_cycles(ThinkTime, Idx - 1, CrashData, PoolName).

%% only support a single crash type. So if self crash is requested,
%% we'll never crash the member.
crash_data(false, false, _) ->
    {never, never};
crash_data(true, _, TakeCycles) ->
    {random:uniform(TakeCycles), never};
crash_data(false, true, TakeCycles) ->
    {never, random:uniform(TakeCycles)}.

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

