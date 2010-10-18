-module(pidq).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-include_lib("eunit/include/eunit.hrl").

-record(pool, {name,
               max_pids = 100,
               min_free = 3,
               init_size = 10,
               pid_starter_args = [],
               free_pids,
               in_use_count}).

-record(state, {
          npools,
          pools = dict:new(),
          in_use_pids = dict:new(),
          consumer_to_pid = dict:new(),
          pid_starter,
          pid_stopper}).

-define(gv(X, Y), proplists:get_value(X, Y)).
-define(gv(X, Y, D), proplists:get_value(X, Y, D)).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start/1,
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

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

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
    State = #state{pid_starter = ?gv(pid_starter, Config),
                   pid_stopper = ?gv(pid_stopper, Config,
                                     {?MODULE, default_stopper}),
                   npools = length(Pools),
                   pools = dict:from_list(Pools)},
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
                             error_logger:info_report({{consumer, Pid, Reason}, Pids}),
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

% default_stopper(Pid) ->
%     exit(Pid, kill).

props_to_pool(P) ->
    Defs = [{free_pids, []}, {in_use_count, 0}],
    % a record is just a tagged tuple
    P2 = lists:append(Defs, P),
    Values = [ ?gv(Field, P2) || Field <- record_info(fields, pool) ],
    list_to_tuple([pool|Values]).

add_pids(error, _N, State) ->
    {bad_pool_name, State};
add_pids(PoolName, N, State) ->
    #state{pools = Pools, pid_starter = {M, F}} = State,
    Pool = dict:fetch(PoolName, Pools),
    #pool{max_pids = Max, free_pids = Free, in_use_count = NumInUse,
          pid_starter_args = Args} = Pool,
    Total = length(Free) + NumInUse,
    case Total + N =< Max of
        true ->
            % FIXME: we'll want to link to these pids so we'll know if
            % they crash. Or should the starter function be expected
            % to do spawn_link?
            NewPids = [ apply(M, F, Args) || _X <- lists:seq(1, N) ],
            Pool1 = Pool#pool{free_pids = lists:append(Free, NewPids)},
            {ok, State#state{pools = dict:store(PoolName, Pool1, Pools)}};
        false ->
            {max_pids_reached, State}
    end.

take_pid(PoolName, From, State) ->
    #state{pools = Pools, in_use_pids = InUse, consumer_to_pid = CPMap} = State,
    Pool = dict:fetch(PoolName, Pools),
    #pool{max_pids = Max, free_pids = Free, in_use_count = NumInUse} = Pool,
    case Free of
        [] when NumInUse == Max ->
            {error_no_pids, State};
        [] when NumInUse < Max ->
            case add_pids(PoolName, 1, State) of
                {ok, State1} ->
                    take_pid(PoolName, From, State1);
                {max_pids_reached, _} ->
                    {error_no_pids, State}
            end;
        [Pid|Rest] ->
            % FIXME: handle min_free here -- should adding pids
            % to satisfy min_free be done in a spawned worker?
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
    {M, F} = State#state.pid_stopper,
    M:F(Pid),
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
