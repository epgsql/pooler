%% @author Seth Falcon <seth@userprimary.net>
%% @copyright 2011 Seth Falcon
%% @doc This is the main interface to the pooler application
%%
%% To integrate with your application, you probably want to call
%% application:start(pooler) after having specified appropriate
%% configuration for the pooler application (either via a config file
%% or appropriate calls to the application module to set the
%% application's config).
%%
-module(pooler).
-behaviour(gen_server).
-define(SERVER, ?MODULE).
-define(DEFAULT_ADD_RETRY, 1).

-include_lib("eunit/include/eunit.hrl").

-type member_info() :: {string(), free | pid(), {_, _, _}}.
-type free_member_info() :: {string(), free, {_, _, _}}.
-type time_unit() :: min | sec | ms | mu.
-type time_spec() :: {non_neg_integer(), time_unit()}.

-record(pool, {
          name             :: string(),
          max_count = 100  :: non_neg_integer(),
          init_count = 10  :: non_neg_integer(),
          start_mfa        :: {atom(), atom(), [term()]},
          free_pids = []   :: [pid()],
          in_use_count = 0 :: non_neg_integer(),
          free_count = 0   :: non_neg_integer(),
          %% The number times to attempt adding a pool member if the
          %% pool size is below max_count and there are no free
          %% members. After this many tries, error_no_members will be
          %% returned by a call to take_member. NOTE: this value
          %% should be >= 2 or else the pool will not grow on demand
          %% when max_count is larger than init_count.
          add_member_retry = ?DEFAULT_ADD_RETRY :: non_neg_integer()
         }).

-record(state, {
          npools                       :: non_neg_integer(),
          pools = dict:new()           :: dict(),
          pool_sups = dict:new()       :: dict(),
          all_members = dict:new()     :: dict(),
          consumer_to_pid = dict:new() :: dict(),
          pool_selector                :: array()
         }).

-define(gv(X, Y), proplists:get_value(X, Y)).
-define(gv(X, Y, D), proplists:get_value(X, Y, D)).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start/1,
         start_link/1,
         stop/0,
         take_member/0,
         take_member/1,
         return_member/1,
         return_member/2,
         % remove_pool/2,
         % add_pool/1,
         pool_stats/0,
         cull_pool/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% To help with testing internal functions
-ifdef(TEST).
-compile([export_all]).
-endif.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

start(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

stop() ->
    gen_server:call(?SERVER, stop).

%% @doc Obtain exclusive access to a member from a randomly selected pool.
%%
%% If there are no free members in the randomly selected pool, then a
%% member will be returned from the pool with the most free members.
%% If no free members are available, 'error_no_members' is returned.
%%
-spec take_member() -> pid() | error_no_members.
take_member() ->
    gen_server:call(?SERVER, take_member).

%% @doc Obtain exclusive access to a member from `PoolName'.
%%
%% If no free members are available, 'error_no_members' is returned.
%%
-spec take_member(string()) -> pid() | error_no_members | error_no_pool.
take_member(PoolName) when is_list(PoolName) ->
    gen_server:call(?SERVER, {take_member, PoolName}).

%% @doc Return a member to the pool so it can be reused.
%%
%% If `Status' is 'ok', the member is returned to the pool.  If
%% `Status' is 'fail', the member is destroyed and a new member is
%% added to the pool in its place.
-spec return_member(pid() | error_no_members, ok | fail) -> ok.
return_member(Pid, Status) when is_pid(Pid) andalso
                                (Status =:= ok orelse Status =:= fail) ->
    CPid = self(),
    gen_server:cast(?SERVER, {return_member, Pid, Status, CPid}),
    ok;
return_member(error_no_members, _) ->
    ok.

%% @doc Return a member to the pool so it can be reused.
%%
-spec return_member(pid() | error_no_members) -> ok.
return_member(Pid) when is_pid(Pid) ->
    CPid = self(),
    gen_server:cast(?SERVER, {return_member, Pid, ok, CPid}),
    ok;
return_member(error_no_members) ->
    ok.

% TODO:
% remove_pool(Name, How) when How == graceful; How == immediate ->
%     gen_server:call(?SERVER, {remove_pool, Name, How}).

% TODO:
% add_pool(Pool) ->
%     gen_server:call(?SERVER, {add_pool, Pool}).

%% @doc Obtain runtime state info for all pools.
%%
%% Format of the return value is subject to change.
-spec pool_stats() -> [tuple()].
pool_stats() ->
    gen_server:call(?SERVER, pool_stats).

%% @doc Remove members whose last return timestamp is older than
%% `MaxAgeMin' minutes.
%%
%% EXPERIMENTAL
%%
-spec cull_pool(string(), non_neg_integer()) -> ok.
cull_pool(PoolName, MaxAgeMin) when MaxAgeMin >= 0 ->
    gen_server:call(?SERVER, {cull_pool, PoolName, MaxAgeMin}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

-spec init([any()]) -> {'ok', #state{npools::'undefined' | non_neg_integer(),
                                     pools::dict(),
                                     pool_sups::dict(),
                                     all_members::dict(),
                                     consumer_to_pid::dict(),
                                     pool_selector::'undefined' | array()}}.
init(Config) ->
    process_flag(trap_exit, true),
    PoolRecs = [ props_to_pool(P) || P <- ?gv(pools, Config) ],
    Pools = [ {Pool#pool.name, Pool} || Pool <-  PoolRecs ],
    PoolSups = [ begin
                  {ok, SupPid} = supervisor:start_child(pooler_pool_sup, [MFA]),
                  {Name, SupPid}
                 end || #pool{name = Name, start_mfa = MFA} <- PoolRecs ],
    State0 = #state{npools = length(Pools),
                    pools = dict:from_list(Pools),
                    pool_sups = dict:from_list(PoolSups),
                    pool_selector = array:from_list([PN || {PN, _} <- Pools])
                  },
    lists:foldl(fun(#pool{name = PName, init_count = N}, {ok, AccState}) ->
                        add_pids(PName, N, AccState)
                end, {ok, State0}, PoolRecs).

handle_call(take_member, {CPid, _Tag},
            #state{pool_selector = PS, npools = NP} = State) ->
    % attempt to return a member from a randomly selected pool.  If
    % that pool has no members, find the pool with most free members
    % and return a member from there.
    PoolName = array:get(crypto:rand_uniform(0, NP), PS),
    case take_member(PoolName, CPid, State) of
        {error_no_members, NewState} ->
            case max_free_pool(State#state.pools) of
                error_no_members ->
                    {reply, error_no_members, NewState};
                MaxFreePoolName ->
                    {NewPid, State2} = take_member(MaxFreePoolName, CPid,
                                                   NewState),
                    {reply, NewPid, State2}
            end;
        {NewPid, NewState} ->
            {reply, NewPid, NewState}
    end;
handle_call({take_member, PoolName}, {CPid, _Tag}, #state{} = State) ->
    {Member, NewState} = take_member(PoolName, CPid, State),
    {reply, Member, NewState};
handle_call(stop, _From, State) ->
    {stop, normal, stop_ok, State};
handle_call(pool_stats, _From, State) ->
    {reply, dict:to_list(State#state.all_members), State};
handle_call({cull_pool, PoolName, MaxAgeMin}, _From, State) ->
    {reply, ok, cull_members(PoolName, MaxAgeMin, State)};
handle_call(_Request, _From, State) ->
    {noreply, State}.

-spec handle_cast(_,_) -> {'noreply', _}.
handle_cast({return_member, Pid, Status, _CPid}, State) ->
    {noreply, do_return_member(Pid, Status, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(_, _) -> {'noreply', _}.
handle_info({'EXIT', Pid, Reason}, State) ->
    State1 =
        case dict:find(Pid, State#state.all_members) of
            {ok, {_PoolName, _ConsumerPid, _Time}} ->
                do_return_member(Pid, fail, State);
            error ->
                case dict:find(Pid, State#state.consumer_to_pid) of
                    {ok, Pids} ->
                        IsOk = case Reason of
                                   normal -> ok;
                                   _Crash -> fail
                               end,
                        lists:foldl(
                          fun(P, S) -> do_return_member(P, IsOk, S) end,
                          State, Pids);
                    error ->
                        State
                end
        end,
    {noreply, State1};
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(_, _) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, _, _) -> {'ok', _}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-spec props_to_pool([{atom(), term()}]) -> #pool{}.
props_to_pool(P) ->
    #pool{      name = ?gv(name, P),
           max_count = ?gv(max_count, P),
          init_count = ?gv(init_count, P),
           start_mfa = ?gv(start_mfa, P),
 add_member_retry = ?gv(add_member_retry, P, ?DEFAULT_ADD_RETRY)}.

% FIXME: creation of new pids should probably happen
% in a spawned process to avoid tying up the loop.
-spec add_pids(error | string(), non_neg_integer(), #state{}) ->
    {bad_pool_name | max_count_reached | ok, #state{}}.
add_pids(error, _N, State) ->
    {bad_pool_name, State};
add_pids(PoolName, N, State) ->
    #state{pools = Pools, all_members = AllMembers} = State,
    Pool = fetch_pool(PoolName, Pools),
    #pool{max_count = Max, free_pids = Free,
          in_use_count = NumInUse, free_count = NumFree} = Pool,
    Total = NumFree + NumInUse,
    case Total + N =< Max of
        true ->
            PoolSup = dict:fetch(PoolName, State#state.pool_sups),
            {AllMembers1, NewPids} = start_n_pids(N, PoolName, PoolSup,
                                                  AllMembers),
            %% start_n_pids may return fewer than N if errors were
            %% encountered.
            NewPidCount = length(NewPids),
            case NewPidCount =:= N of
                true -> ok;
                false ->
                    error_logger:error_msg("tried to add ~B members, only added ~B~n",
                                           [N, NewPidCount]),
                    send_metric(<<"pooler.events">>,
                                {add_pids_failed, N, NewPidCount}, history)
            end,
            Pool1 = Pool#pool{free_pids = Free ++ NewPids,
                              free_count = length(Free) + NewPidCount},
            {ok, State#state{pools = store_pool(PoolName, Pool1, Pools),
                             all_members = AllMembers1}};
        false ->
            {max_count_reached, State}
    end.

-spec take_member(string(), {pid(), _}, #state{}) ->
    {error_no_pool | error_no_members | pid(), #state{}}.
take_member(PoolName, From, #state{pools = Pools} = State) ->
    Pool = fetch_pool(PoolName, Pools),
    take_member_from_pool(Pool, From, State, pool_add_retries(Pool)).

-spec take_member_from_pool(error_no_pool | #pool{}, {pid(), term()}, #state{},
                            non_neg_integer()) ->
                                   {error_no_pool | error_no_members | pid(), #state{}}.
take_member_from_pool(error_no_pool, _From, State, _) ->
    {error_no_pool, State};
take_member_from_pool(#pool{name = PoolName,
                            max_count = Max,
                            free_pids = Free,
                            in_use_count = NumInUse,
                            free_count = NumFree} = Pool,
                      From,
                      #state{pools = Pools, consumer_to_pid = CPMap} = State,
                      Retries) ->
    send_metric(pool_metric(PoolName, take_rate), 1, meter),
    case Free of
        [] when NumInUse =:= Max ->
            send_metric(<<"pooler.error_no_members_count">>, {inc, 1}, counter),
            send_metric(<<"pooler.events">>, error_no_members, history),
            {error_no_members, State};
        [] when NumInUse < Max andalso Retries > 0 ->
            case add_pids(PoolName, 1, State) of
                {ok, State1} ->
                    %% add_pids may have updated our pool
                    Pool1 = fetch_pool(PoolName, State1#state.pools),
                    take_member_from_pool(Pool1, From, State1, Retries - 1);
                {max_count_reached, _} ->
                    send_metric(<<"pooler.error_no_members_count">>, {inc, 1}, counter),
                    send_metric(<<"pooler.events">>, error_no_members, history),
                    {error_no_members, State}
            end;
        [] when Retries =:= 0 ->
            %% max retries reached
            send_metric(<<"pooler.error_no_members_count">>, {inc, 1}, counter),
            {error_no_members, State};
        [Pid|Rest] ->
            erlang:link(From),
            Pool1 = Pool#pool{free_pids = Rest, in_use_count = NumInUse + 1,
                              free_count = NumFree - 1},
            send_metric(pool_metric(PoolName, in_use_count), Pool1#pool.in_use_count, histogram),
            send_metric(pool_metric(PoolName, free_count), Pool1#pool.free_count, histogram),
            {Pid, State#state{
                    pools = store_pool(PoolName, Pool1, Pools),
                    consumer_to_pid = add_member_to_consumer(Pid, From, CPMap),
                    all_members = set_cpid_for_member(Pid, From,
                                                      State#state.all_members)
                   }}
    end.

-spec do_return_member(pid(), ok | fail, #state{}) -> #state{}.
do_return_member(Pid, ok, #state{} = State) ->
    {PoolName, CPid, _} = dict:fetch(Pid, State#state.all_members),
    Pool = fetch_pool(PoolName, State#state.pools),
    #pool{free_pids = Free, in_use_count = NumInUse,
          free_count = NumFree} = Pool,
    Pool1 = Pool#pool{free_pids = [Pid | Free], in_use_count = NumInUse - 1,
                      free_count = NumFree + 1},
    Entry = {PoolName, free, os:timestamp()},
    State#state{pools = store_pool(PoolName, Pool1, State#state.pools),
                all_members = store_all_members(Pid, Entry,
                                                State#state.all_members),
                consumer_to_pid = cpmap_remove(Pid, CPid,
                                               State#state.consumer_to_pid)};
do_return_member(Pid, fail, #state{all_members = AllMembers} = State) ->
    % for the fail case, perhaps the member crashed and was alerady
    % removed, so use find instead of fetch and ignore missing.
    case dict:find(Pid, AllMembers) of
        {ok, {PoolName, _, _}} ->
            State1 = remove_pid(Pid, State),
            case add_pids(PoolName, 1, State1) of
                {Status, State2} when Status =:= ok;
                                      Status =:= max_count_reached ->
                    State2;
                {Status, _} ->
                    erlang:error({error, "unexpected return from add_pid",
                                  Status, erlang:get_stacktrace()}),
                    send_metric(<<"pooler.events">>, bad_return_from_add_pid,
                                history)
            end;
        error ->
            State
    end.

% @doc Remove `Pid' from the pid list associated with `CPid' in the
% consumer to member map given by `CPMap'.
%
% If `Pid' is the last element in `CPid's pid list, then the `CPid'
% entry is removed entirely.
%
-spec cpmap_remove(pid(), pid() | free, dict()) -> dict().
cpmap_remove(_Pid, free, CPMap) ->
    CPMap;
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

% @doc Remove and kill a pool member.
%
% Handles in-use and free members.  Logs an error if the pid is not
% tracked in state.all_members.
%
-spec remove_pid(pid(), #state{}) -> #state{}.
remove_pid(Pid, State) ->
    #state{all_members = AllMembers, pools = Pools,
           consumer_to_pid = CPMap} = State,
    case dict:find(Pid, AllMembers) of
        {ok, {PoolName, free, _Time}} ->
            % remove an unused member
            Pool = fetch_pool(PoolName, Pools),
            FreePids = lists:delete(Pid, Pool#pool.free_pids),
            NumFree = Pool#pool.free_count - 1,
            Pool1 = Pool#pool{free_pids = FreePids, free_count = NumFree},
            exit(Pid, kill),
            send_metric(<<"pooler.killed_free_count">>, {inc, 1}, counter),
            State#state{pools = store_pool(PoolName, Pool1, Pools),
                        all_members = dict:erase(Pid, AllMembers)};
        {ok, {PoolName, CPid, _Time}} ->
            Pool = fetch_pool(PoolName, Pools),
            Pool1 = Pool#pool{in_use_count = Pool#pool.in_use_count - 1},
            exit(Pid, kill),
            send_metric(<<"pooler.killed_in_use_count">>, {inc, 1}, counter),
            State#state{pools = store_pool(PoolName, Pool1, Pools),
                        consumer_to_pid = cpmap_remove(Pid, CPid, CPMap),
                        all_members = dict:erase(Pid, AllMembers)};
        error ->
            error_logger:error_report({unknown_pid, Pid,
                                       erlang:get_stacktrace()}),
            send_metric(<<"pooler.event">>, unknown_pid, history),
            State
    end.

-spec max_free_pool(dict()) -> error_no_members | string().
max_free_pool(Pools) ->
    case dict:fold(fun fold_max_free_count/3, {"", 0}, Pools) of
        {"", 0} -> error_no_members;
        {MaxFreePoolName, _} -> MaxFreePoolName
    end.

-spec fold_max_free_count(string(), #pool{}, {string(), non_neg_integer()}) ->
    {string(), non_neg_integer()}.
fold_max_free_count(Name, Pool, {CName, CMax}) ->
    case Pool#pool.free_count > CMax of
        true -> {Name, Pool#pool.free_count};
        false -> {CName, CMax}
    end.


-spec start_n_pids(non_neg_integer(), string(), pid(), dict()) ->
    {dict(), [pid()]}.
start_n_pids(N, PoolName, PoolSup, AllMembers) ->
    NewPids = do_n(N, fun(Acc) ->
                              case supervisor:start_child(PoolSup, []) of
                                  {ok, Pid} ->
                                      erlang:link(Pid),
                                      [Pid | Acc];
                                  _Else ->
                                      Acc
                              end
                      end, []),
    AllMembers1 = lists:foldl(
                    fun(M, Dict) ->
                            Entry = {PoolName, free, os:timestamp()},
                            store_all_members(M, Entry, Dict)
                    end, AllMembers, NewPids),
    {AllMembers1, NewPids}.

do_n(0, _Fun, Acc) ->
    Acc;
do_n(N, Fun, Acc) ->
    do_n(N - 1, Fun, Fun(Acc)).


-spec fetch_pool(string(), dict()) -> #pool{} | error_no_pool.
fetch_pool(PoolName, Pools) ->
    case dict:find(PoolName, Pools) of
        {ok, Pool} -> Pool;
        error -> error_no_pool
    end.

pool_add_retries(#pool{add_member_retry = Retries}) ->
    Retries;
pool_add_retries(error_no_pool) ->
    0.

-spec store_pool(string(), #pool{}, dict()) -> dict().
store_pool(PoolName, Pool = #pool{}, Pools) ->
    dict:store(PoolName, Pool, Pools).

-spec store_all_members(pid(),
                        {string(), free | pid(), {_, _, _}}, dict()) -> dict().
store_all_members(Pid, Val = {_PoolName, _CPid, _Time}, AllMembers) ->
    dict:store(Pid, Val, AllMembers).

-spec set_cpid_for_member(pid(), pid(), dict()) -> dict().
set_cpid_for_member(MemberPid, CPid, AllMembers) ->
    dict:update(MemberPid,
                fun({PoolName, free, Time = {_, _, _}}) ->
                        {PoolName, CPid, Time}
                end, AllMembers).

-spec add_member_to_consumer(pid(), pid(), dict()) -> dict().
add_member_to_consumer(MemberPid, CPid, CPMap) ->
    dict:update(CPid, fun(O) -> [MemberPid|O] end, [MemberPid], CPMap).

-spec cull_members(string(), non_neg_integer(), #state{}) -> #state{}.
cull_members(PoolName, MaxAgeMin, #state{pools = Pools} = State) ->
    cull_members_from_pool(fetch_pool(PoolName, Pools), MaxAgeMin, State).

-spec cull_members_from_pool(#pool{}, non_neg_integer(), #state{}) -> #state{}.
cull_members_from_pool(#pool{free_count = FreeCount,
                             init_count = InitCount,
                             in_use_count = InUseCount} = Pool, MaxAgeMin,
                       #state{all_members = AllMembers} = State) ->
    MaxCull = FreeCount - (InitCount - InUseCount),
    case MaxCull > 0 of
        true ->
            MemberInfo = member_info(Pool#pool.free_pids, AllMembers),
            ExpiredMembers =
                expired_free_members(MemberInfo, os:timestamp(), MaxAgeMin),
            CullList = lists:sublist(ExpiredMembers, MaxCull),
            lists:foldl(fun({CullMe, _}, S) -> remove_pid(CullMe, S) end,
                        State, CullList);
        false ->
            State
    end.

-spec member_info([pid()], dict()) -> [{pid(), member_info()}].
member_info(Pids, AllMembers) ->
    [ {P, dict:fetch(P, AllMembers)} || P <- Pids ].

-spec expired_free_members([{pid(), member_info()}], {_, _, _},
                           non_neg_integer()) -> [{pid(), free_member_info()}].
expired_free_members(Members, Now, MaxAgeMin) ->
    Micros = 60 * 1000 * 1000,
    [ MI || MI = {_, {_, free, LastReturn}} <- Members,
            timer:now_diff(Now, LastReturn) >= (MaxAgeMin * Micros) ].

-spec send_metric(binary(),
                  'error_no_members' |
                  'unknown_pid' |
                  non_neg_integer() |
                  {'inc',1} |
                  {'add_pids_failed', non_neg_integer(), non_neg_integer()},
                  'counter' | 'histogram' | 'history' | 'meter') -> ok.
%% Send a metric using the metrics module from application config or
%% do nothing.
send_metric(Name, Value, Type) ->
    case application:get_env(pooler, metrics_module) of
        undefined -> ok;
        {ok, Mod} -> Mod:notify(Name, Value, Type)
    end,
    ok.

-spec pool_metric(string(), 'free_count' | 'in_use_count' | 'take_rate') -> binary().
pool_metric(PoolName, Metric) ->
    iolist_to_binary([<<"pooler.">>, PoolName, ".",
                      atom_to_binary(Metric, utf8)]).

-spec time_as_millis(time_spec()) -> non_neg_integer().
%% @doc Convert time unit into milliseconds.
time_as_millis({Time, Unit}) ->
    time_as_micros({Time, Unit}) div 1000.

-spec time_as_micros(time_spec()) -> non_neg_integer().
%% @doc Convert time unit into microseconds
time_as_micros({Time, min}) ->
    60 * 1000 * 1000 * Time;
time_as_micros({Time, sec}) ->
    1000 * 1000 * Time;
time_as_micros({Time, ms}) ->
    1000 * Time;
time_as_micros({Time, mu}) ->
    Time.
