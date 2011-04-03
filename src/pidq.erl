-module(pidq).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-include_lib("eunit/include/eunit.hrl").

-record(pool, {
          name             :: string(),
          max_count = 100  :: non_neg_integer(),
          init_count = 10  :: non_neg_integer(),
          start_mfa        :: {atom(), atom(), [term()]},
          free_pids = []   :: [pid()],
          in_use_count = 0 :: non_neg_integer()
         }).

-record(state, {
          npools                       :: non_neg_integer(),
          pools = dict:new()           :: dict:dictionary(),
          pool_sups = dict:new()       :: dict:dictionary(),
          in_use_pids = dict:new()     :: dict:dictionary(),
          consumer_to_pid = dict:new() :: dict:dictionary()
         }).

-define(gv(X, Y), proplists:get_value(X, Y)).
-define(gv(X, Y, D), proplists:get_value(X, Y, D)).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start/1,
         start_link/1,
         stop/0,
         take_pid/0,
         return_pid/2,
         remove_pool/2,
         add_pool/1,
         pool_stats/1,
         status/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

start(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

stop() ->
    gen_server:call(?SERVER, stop).

take_pid() ->
    gen_server:call(?SERVER, take_pid).

return_pid(Pid, Status) when Status == ok; Status == fail ->
    CPid = self(),
    gen_server:cast(?SERVER, {return_pid, Pid, Status, CPid}),
    ok.

remove_pool(Name, How) when How == graceful; How == immediate ->
    gen_server:call(?SERVER, {remove_pool, Name, How}).

add_pool(Pool) ->
    gen_server:call(?SERVER, {add_pool, Pool}).

pool_stats(Pool) ->
    gen_server:call(?SERVER, {pool_stats, Pool}).

status() ->
    gen_server:call(?SERVER, status).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Config) ->
    PoolRecs = [ props_to_pool(P) || P <- ?gv(pools, Config) ],
    Pools = [ {Pool#pool.name, Pool} || Pool <-  PoolRecs ],
    PoolSups =
        lists:map(
          fun(#pool{name = Name, start_mfa = MFA}) ->
                  {ok, SupPid} = supervisor:start_child(pidq_pool_sup, [MFA]),
                  {Name, SupPid}
          end, PoolRecs),
    State0 = #state{npools = length(Pools),
                   pools = dict:from_list(Pools),
                   pool_sups = dict:from_list(PoolSups)
                  },
    {ok, State} = lists:foldl(
                    fun(#pool{name = PName, init_count = N}, {ok, AccState}) ->
                            add_pids(PName, N, AccState)
                    end, {ok, State0}, PoolRecs),
    process_flag(trap_exit, true),
    {ok, State}.

handle_call(take_pid, {CPid, _Tag}, State) ->
    % FIXME: load-balance?
    PoolName = hd(dict:fetch_keys(State#state.pools)),
    {NewPid, NewState} = take_pid(PoolName, CPid, State),
    {reply, NewPid, NewState};
handle_call(stop, _From, State) ->
    % FIXME:
    % loop over in use and free pids and stop them?
    % {M, F} = State#state.pid_stopper,
    {stop, normal, stop_ok, State};
handle_call({pool_stats, PoolName}, _From, State) ->
    Pool = dict:fetch(PoolName, State#state.pools),
    Stats = [{in_use, dict:fetch_keys(State#state.in_use_pids)},
             {free, Pool#pool.free_pids}],
    {reply, Stats, State};
handle_call(_Request, _From, State) ->
    {noreply, ok, State}.


handle_cast({return_pid, Pid, Status, CPid}, State) ->
    {noreply, do_return_pid({Pid, Status}, CPid, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State) ->
    % error_logger:info_report({got_exit, Pid, Reason}),
    State1 = case dict:find(Pid, State#state.in_use_pids) of
                 {ok, {_PName, CPid}} -> do_return_pid({Pid, fail}, CPid, State);
                 error ->
                     CPMap = State#state.consumer_to_pid,
                     case dict:find(Pid, CPMap) of

                         {ok, Pids} ->
                             IsOk = case Reason of
                                        normal -> ok;
                                        _Crash -> fail
                                    end,
                             lists:foldl(fun(P, S) ->
                                                 do_return_pid({P, IsOk}, Pid, S)
                                         end, State, Pids);
                         error ->
                             State
                     end
             end,
    {noreply, State1};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

props_to_pool(P) ->
    #pool{      name = ?gv(name, P),
           max_count = ?gv(max_count, P),
          init_count = ?gv(init_count, P),
           start_mfa = ?gv(start_mfa, P)}.

% FIXME: creation of new pids should probably happen
% in a spawned process to avoid typing up the loop.
add_pids(error, _N, State) ->
    {bad_pool_name, State};
add_pids(PoolName, N, State) ->
    #state{pools = Pools, pool_sups = PoolSups} = State,
    Pool = dict:fetch(PoolName, Pools),
    #pool{max_count = Max, free_pids = Free, in_use_count = NumInUse} = Pool,
    Total = length(Free) + NumInUse,
    case Total + N =< Max of
        true ->
            Sup = dict:fetch(PoolName, PoolSups),
            NewPids =
                lists:map(fun(_I) ->
                                  {ok, Pid} = supervisor:start_child(Sup, []),
                                  erlang:link(Pid),
                                  Pid
                          end, lists:seq(1, N)),
            Pool1 = Pool#pool{free_pids = Free ++ NewPids},
            {ok, State#state{pools = dict:store(PoolName, Pool1, Pools)}};
        false ->
            {max_count_reached, State}
    end.

take_pid(PoolName, From, State) ->
    #state{pools = Pools, in_use_pids = InUse, consumer_to_pid = CPMap} = State,
    Pool = dict:fetch(PoolName, Pools),
    #pool{max_count = Max, free_pids = Free, in_use_count = NumInUse} = Pool,
    case Free of
        [] when NumInUse == Max ->
            {error_no_pids, State};
        [] when NumInUse < Max ->
            case add_pids(PoolName, 1, State) of
                {ok, State1} ->
                    take_pid(PoolName, From, State1);
                {max_count_reached, _} ->
                    {error_no_pids, State}
            end;
        [Pid|Rest] ->
            erlang:link(From),
            Pool1 = Pool#pool{free_pids = Rest, in_use_count = NumInUse + 1},
            CPMap1 = dict:update(From, fun(O) -> [Pid|O] end, [Pid], CPMap),
            {Pid, State#state{pools = dict:store(PoolName, Pool1, Pools),
                              in_use_pids = dict:store(Pid, {PoolName, From}, InUse),
                              consumer_to_pid = CPMap1}}
    end.

do_return_pid({Pid, Status}, CPid, State) ->
    #state{in_use_pids = InUse, pools = Pools,
           consumer_to_pid = CPMap} = State,
    case dict:find(Pid, InUse) of
        {ok, {PoolName, _CPid2}} -> % FIXME, assert that CPid2 == CPid?
            Pool = dict:fetch(PoolName, Pools),
            {Pool1, State1} =
                case Status of
                    ok -> {add_pid_to_free(Pid, Pool), State};
                    fail -> handle_failed_pid(Pid, PoolName, Pool, State)
                    end,
            State1#state{in_use_pids = dict:erase(Pid, InUse),
                         pools = dict:store(PoolName, Pool1, Pools),
                         consumer_to_pid = cpmap_remove(Pid, CPid, CPMap)};
        error ->
            error_logger:warning_report({return_pid_not_found, Pid, dict:to_list(InUse)}),
            State
    end.

add_pid_to_free(Pid, Pool) ->
    #pool{free_pids = Free, in_use_count = NumInUse} = Pool,
    Pool#pool{free_pids = [Pid|Free], in_use_count = NumInUse - 1}.

handle_failed_pid(Pid, PoolName, Pool, State) ->
    exit(Pid, kill),
    {_, NewState} = add_pids(PoolName, 1, State),
    NumInUse = Pool#pool.in_use_count,
    {Pool#pool{in_use_count = NumInUse - 1}, NewState}.

cpmap_remove(Pid, CPid, CPMap) ->
    case dict:find(CPid, CPMap) of
        {ok, Pids0} ->
            unlink(CPid), % FIXME: flush msg queue here?
            Pids1 = lists:delete(Pid, Pids0),
            case Pids1 of
                [_H|_T] ->
                    dict:store(CPid, Pids1, CPMap);
                [] ->
                    dict:erase(CPid, CPMap)
            end;
        error ->
            % FIXME: this shouldn't happen, should we log or error?
            CPMap
    end.
