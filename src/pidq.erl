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
         stop/1,
         take_pid/0,
         return_pid/2,
         remove_pool/2,
         add_pool/1,
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

stop(_How) ->
    stop().

take_pid() ->
    gen_server:call(?SERVER, take_pid).

return_pid(Pid, Status) when Status == ok; Status == fail ->
    gen_server:call(?SERVER, {return_pid, Pid, Status}).

remove_pool(Name, How) when How == graceful; How == immediate ->
    gen_server:call(?SERVER, {remove_pool, Name, How}).

add_pool(Pool) ->
    gen_server:call(?SERVER, {add_pool, Pool}).

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
    {ok, State}.

handle_call(take_pid, {CPid, _Tag}, State) ->
    % FIXME: load-balance?
    PoolName = hd(dict:fetch_keys(State#state.pools)),
    {NewPid, NewState} = take_pid(PoolName, CPid, State),
    {reply, NewPid, NewState};
handle_call(stop, _From, State) ->
    {stop, normal, stop_ok, State};
handle_call(_Request, _From, State) ->
    {noreply, ok, State}.


handle_cast({return_pid, Pid, _Status}, State) ->
    {noreply, do_return_pid(Pid, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

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

add_pids(PoolName, N, State) ->
    #state{pools = Pools, pid_starter = {M, F}} = State,
    Pool = dict:fetch(PoolName, Pools),
    #pool{max_pids = Max, free_pids = Free, in_use_count = NumInUse,
          pid_starter_args = Args} = Pool,
    Total = length(Free) + NumInUse,
    case Total + N < Max of
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
            {_Status, State1} = add_pids(PoolName, 1, State),
            take_pid(PoolName, From, State1);
        [Pid|Rest] ->
            % FIXME: handle min_free here -- should adding pids
            % to satisfy min_free be done in a spawned worker?
            Pool1 = Pool#pool{free_pids = Rest, in_use_count = NumInUse + 1},
            CPMap1 = dict:update(From, fun(O) -> [Pid|O] end, [Pid], CPMap),
            {Pid, State#state{pools = dict:store(PoolName, Pool1, Pools),
                              in_use_pids = dict:store(Pid, PoolName, InUse),
                              consumer_to_pid = CPMap1}}
    end.

do_return_pid(Pid, State) ->
    #state{in_use_pids = InUse, pools = Pools} = State,
    case dict:find(Pid, InUse) of
        {ok, PoolName} ->
            Pool = dict:fetch(PoolName, Pools),
            #pool{free_pids = Free, in_use_count = NumInUse} = Pool,
            Pool1 = Pool#pool{free_pids = [Pid|Free], in_use_count = NumInUse - 1},
            State#state{in_use_pids = dict:erase(Pid, InUse),
                        pools = dict:store(PoolName, Pool1, Pools)};
        error ->
            error_logger:warning_report({return_pid_not_found, Pid}),
            State
    end.
