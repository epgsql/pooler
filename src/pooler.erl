%% @author Seth Falcon <seth@userprimary.net>
%% @copyright 2011-2012 Seth Falcon
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

-include("pooler.hrl").
-include_lib("eunit/include/eunit.hrl").


%% type specs for pool metrics
-type metric_label() :: binary().
-type metric_value() :: 'unknown_pid' |
                        non_neg_integer() |
                        {'add_pids_failed', non_neg_integer(), non_neg_integer()} |
                        {'inc',1} |
                        'error_no_members'.
-type metric_type() :: 'counter' | 'histogram' | 'history' | 'meter'.

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1,
         take_member/1,
         take_group_member/1,
         return_group_member/2,
         return_group_member/3,
         return_member/2,
         return_member/3,
         pool_stats/1]).

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

start_link(#pool{name = Name} = Pool) ->
    gen_server:start_link({local, Name}, ?MODULE, Pool, []).

%% @doc Obtain exclusive access to a member from `PoolName'.
%%
%% If no free members are available, 'error_no_members' is returned.
%%
-spec take_member(atom() | pid()) -> pid() | error_no_members.
take_member(PoolName) when is_atom(PoolName) orelse is_pid(PoolName) ->
    gen_server:call(PoolName, take_member, infinity).

%% @doc Take a member from a randomly selected member of the group
%% `GroupName'. Returns `MemberPid' or `error_no_members'.  If no
%% members are available in the randomly chosen pool, all other pools
%% in the group are tried in order.
-spec take_group_member(atom()) -> pid() | error_no_members.
take_group_member(GroupName) ->
    case pg2:get_local_members(GroupName) of
        {error, {no_such_group, GroupName}} = Error ->
            Error;
        Members ->
            %% Put a random member at the front of the list and then
            %% return the first member you can walking the list.
            Idx = crypto:rand_uniform(1, length(Members) + 1),
            {Pid, Rest} = extract_nth(Idx, Members),
            take_first_member([Pid | Rest])
    end.

take_first_member([Pid | Rest]) ->
    case take_member(Pid) of
        error_no_members ->
            take_first_member(Rest);
        Member ->
            ets:insert(?POOLER_GROUP_TABLE, {Member, Pid}),
            Member
    end;
take_first_member([]) ->
    error_no_members.

%% this helper function returns `{Nth_Elt, Rest}' where `Nth_Elt' is
%% the nth element of `L' and `Rest' is `L -- [Nth_Elt]'.
extract_nth(N, L) ->
    extract_nth(N, L, []).

extract_nth(1, [H | T], Acc) ->
    {H, Acc ++ T};
extract_nth(N, [H | T], Acc) ->
    extract_nth(N - 1, T, [H | Acc]);
extract_nth(_, [], _) ->
    error(badarg).

%% @doc Return a member that was taken from the group
%% `GroupName'. This is a convenience function for
%% `return_group_member/3' with `Status' of `ok'.
-spec return_group_member(atom(), pid() | error_no_members) -> ok.
return_group_member(GroupName, MemberPid) ->
    return_group_member(GroupName, MemberPid, ok).

%% @doc Return a member that was taken from the group `GroupName'. If
%% `Status' is `ok' the member is returned to the pool from which is
%% came. If `Status' is `fail' the member will be terminated and a new
%% member added to the appropriate pool.
-spec return_group_member(atom(), pid() | error_no_members, ok | fail) -> ok.
return_group_member(_, error_no_members, _) ->
    ok;
return_group_member(_GroupName, MemberPid, Status) ->
    case ets:lookup(?POOLER_GROUP_TABLE, MemberPid) of
        [{MemberPid, PoolPid}] ->
            return_member(PoolPid, MemberPid, Status);
        [] ->
            ok
    end.

%% @doc Return a member to the pool so it can be reused.
%%
%% If `Status' is 'ok', the member is returned to the pool.  If
%% `Status' is 'fail', the member is destroyed and a new member is
%% added to the pool in its place.
-spec return_member(atom() | pid(), pid() | error_no_members, ok | fail) -> ok.
return_member(PoolName, Pid, Status) when is_pid(Pid) andalso
                                          (is_atom(PoolName) orelse
                                           is_pid(PoolName)) andalso
                                          (Status =:= ok orelse
                                           Status =:= fail) ->
    gen_server:call(PoolName, {return_member, Pid, Status}, infinity),
    ok;
return_member(_, error_no_members, _) ->
    ok.

%% @doc Return a member to the pool so it can be reused.
%%
-spec return_member(atom() | pid(), pid() | error_no_members) -> ok.
return_member(PoolName, Pid) when is_pid(Pid) andalso
                                  (is_atom(PoolName) orelse is_pid(PoolName)) ->
    gen_server:call(PoolName, {return_member, Pid, ok}, infinity),
    ok;
return_member(_, error_no_members) ->
    ok.

%% @doc Obtain runtime state info for all pools.
%%
%% Format of the return value is subject to change.
-spec pool_stats(atom() | pid()) -> [tuple()].
pool_stats(PoolName) ->
    gen_server:call(PoolName, pool_stats).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

-spec init(#pool{}) -> {'ok', #pool{}, 0}.
init(#pool{}=Pool) ->
    %% FIXME: change to a monitor only model so that this doesn't have
    %% to be a system process.
    process_flag(trap_exit, true),
    #pool{init_count = N} = Pool,
    MemberSup = pooler_pool_sup:member_sup_name(Pool),
    Pool1 = set_member_sup(Pool, MemberSup),
    Pool2 = cull_members_from_pool(Pool1),
    {ok, NewPool} = add_pids(N, Pool2),
    %% trigger an immediate timeout, handled by handle_info to allow
    %% us to register with pg2. We use the timeout mechanism to ensure
    %% that a server is added to a group only when it is ready to
    %% process messages.
    {ok, NewPool, 0}.

set_member_sup(#pool{} = Pool, MemberSup) ->
    Pool#pool{member_sup = MemberSup}.

handle_call(take_member, {CPid, _Tag}, #pool{} = Pool) ->
    Retries = pool_add_retries(Pool),
    {Member, NewPool} = take_member_from_pool(Pool, CPid, Retries),
    {reply, Member, NewPool};
handle_call({return_member, Pid, Status}, {_CPid, _Tag}, Pool) ->
    {reply, ok, do_return_member(Pid, Status, Pool)};
handle_call(stop, _From, Pool) ->
    {stop, normal, stop_ok, Pool};
handle_call(pool_stats, _From, Pool) ->
    {reply, dict:to_list(Pool#pool.all_members), Pool};
handle_call(_Request, _From, Pool) ->
    {noreply, Pool}.

-spec handle_cast(_,_) -> {'noreply', _}.
handle_cast(_Msg, Pool) ->
    {noreply, Pool}.

-spec handle_info(_, _) -> {'noreply', _}.
handle_info(timeout, #pool{group = undefined} = Pool) ->
    %% ignore
    {noreply, Pool};
handle_info(timeout, #pool{group = Group} = Pool) ->
    ok = pg2:create(Group),
    ok = pg2:join(Group, self()),
    {noreply, Pool};
handle_info({'EXIT', Pid, Reason}, State) ->
    State1 =
        case dict:find(Pid, State#pool.all_members) of
            {ok, {_PoolName, _ConsumerPid, _Time}} ->
                do_return_member(Pid, fail, State);
            error ->
                case dict:find(Pid, State#pool.consumer_to_pid) of
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
handle_info(cull_pool, Pool) ->
    {noreply, cull_members_from_pool(Pool)};
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

% FIXME: creation of new pids should probably happen
% in a spawned process to avoid tying up the loop.
-spec add_pids(non_neg_integer(), #pool{}) ->
    {max_count_reached | ok, #pool{}}.
add_pids(N, Pool) ->
    #pool{max_count = Max, free_pids = Free,
          in_use_count = NumInUse, free_count = NumFree,
          member_sup = PoolSup,
          all_members = AllMembers} = Pool,
    Total = NumFree + NumInUse,
    PoolName = Pool#pool.name,
    case Total + N =< Max of
        true ->
            {AllMembers1, NewPids} = start_n_pids(N, PoolName, PoolSup,
                                                  AllMembers),
            %% start_n_pids may return fewer than N if errors were
            %% encountered.
            NewPidCount = length(NewPids),
            case NewPidCount =:= N of
                true -> ok;
                false ->
                    %% FIXME: pool NAME specific
                    error_logger:error_msg("tried to add ~B members, only added ~B~n",
                                           [N, NewPidCount]),
                    send_metric(<<"pooler.events">>,
                                {add_pids_failed, N, NewPidCount}, history)
            end,
            Pool1 = Pool#pool{free_pids = Free ++ NewPids,
                              free_count = length(Free) + NewPidCount},
            {ok, Pool1#pool{all_members = AllMembers1}};
        false ->
            {max_count_reached, Pool}
    end.

-spec take_member_from_pool(#pool{}, {pid(), term()},
                            non_neg_integer()) ->
                                   {error_no_members | pid(), #pool{}}.
take_member_from_pool(#pool{name = PoolName,
                            max_count = Max,
                            free_pids = Free,
                            in_use_count = NumInUse,
                            free_count = NumFree,
                            consumer_to_pid = CPMap} = Pool,
                      From,
                      Retries) ->
    send_metric(pool_metric(PoolName, take_rate), 1, meter),
    case Free of
        [] when NumInUse =:= Max ->
            send_metric(<<"pooler.error_no_members_count">>, {inc, 1}, counter),
            send_metric(<<"pooler.events">>, error_no_members, history),
            {error_no_members, Pool};
        [] when NumInUse < Max andalso Retries > 0 ->
            case add_pids(1, Pool) of
                {ok, Pool1} ->
                    %% add_pids may have updated our pool
                    take_member_from_pool(Pool1, From, Retries - 1);
                {max_count_reached, _} ->
                    %% FIXME: make pool NAME specific
                    send_metric(<<"pooler.error_no_members_count">>, {inc, 1}, counter),
                    send_metric(<<"pooler.events">>, error_no_members, history),
                    {error_no_members, Pool}
            end;
        [] when Retries =:= 0 ->
            %% max retries reached
            %% FIXME: make pool NAME specific
            send_metric(<<"pooler.error_no_members_count">>, {inc, 1}, counter),
            {error_no_members, Pool};
        [Pid|Rest] ->
            erlang:link(From),
            Pool1 = Pool#pool{free_pids = Rest, in_use_count = NumInUse + 1,
                              free_count = NumFree - 1},
            send_metric(pool_metric(PoolName, in_use_count), Pool1#pool.in_use_count, histogram),
            send_metric(pool_metric(PoolName, free_count), Pool1#pool.free_count, histogram),
            {Pid, Pool1#pool{
                    consumer_to_pid = add_member_to_consumer(Pid, From, CPMap),
                    all_members = set_cpid_for_member(Pid, From,
                                                      Pool1#pool.all_members)
                   }}
    end.

-spec do_return_member(pid(), ok | fail, #pool{}) -> #pool{}.
do_return_member(Pid, ok, #pool{all_members = AllMembers} = Pool) ->
    clean_group_table(Pid, Pool),
    case dict:find(Pid, AllMembers) of
        {ok, {PoolName, CPid, _}} ->
            #pool{free_pids = Free, in_use_count = NumInUse,
                  free_count = NumFree} = Pool,
            Pool1 = Pool#pool{free_pids = [Pid | Free], in_use_count = NumInUse - 1,
                              free_count = NumFree + 1},
            Entry = {PoolName, free, os:timestamp()},
            Pool1#pool{all_members = store_all_members(Pid, Entry, AllMembers),
                       consumer_to_pid = cpmap_remove(Pid, CPid,
                                                      Pool1#pool.consumer_to_pid)};
        error ->
            Pool
    end;
do_return_member(Pid, fail, #pool{all_members = AllMembers} = Pool) ->
    % for the fail case, perhaps the member crashed and was alerady
    % removed, so use find instead of fetch and ignore missing.
    clean_group_table(Pid, Pool),
    case dict:find(Pid, AllMembers) of
        {ok, {_PoolName, _, _}} ->
            Pool1 = remove_pid(Pid, Pool),
            case add_pids(1, Pool1) of
                {Status, Pool2} when Status =:= ok;
                                     Status =:= max_count_reached ->
                    Pool2;
                {Status, _} ->
                    erlang:error({error, "unexpected return from add_pid",
                                  Status, erlang:get_stacktrace()}),
                    send_metric(<<"pooler.events">>, bad_return_from_add_pid,
                                history)
            end;
        error ->
            Pool
    end.

clean_group_table(_MemberPid, #pool{group = undefined}) ->
    ok;
clean_group_table(MemberPid, #pool{group = _GroupName}) ->
    ets:delete(?POOLER_GROUP_TABLE, MemberPid).

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
-spec remove_pid(pid(), #pool{}) -> #pool{}.
remove_pid(Pid, Pool) ->
    #pool{all_members = AllMembers,
          consumer_to_pid = CPMap} = Pool,
    case dict:find(Pid, AllMembers) of
        {ok, {_PoolName, free, _Time}} ->
            % remove an unused member
            FreePids = lists:delete(Pid, Pool#pool.free_pids),
            NumFree = Pool#pool.free_count - 1,
            Pool1 = Pool#pool{free_pids = FreePids, free_count = NumFree},
            exit(Pid, kill),
            %% FIXME: make this pool NAME specific
            send_metric(<<"pooler.killed_free_count">>, {inc, 1}, counter),
            Pool1#pool{all_members = dict:erase(Pid, AllMembers)};
        {ok, {_PoolName, CPid, _Time}} ->
            Pool1 = Pool#pool{in_use_count = Pool#pool.in_use_count - 1},
            exit(Pid, kill),
            %% FIXME: also fix to make pool NAME specific
            send_metric(<<"pooler.killed_in_use_count">>, {inc, 1}, counter),
            Pool1#pool{consumer_to_pid = cpmap_remove(Pid, CPid, CPMap),
                       all_members = dict:erase(Pid, AllMembers)};
        error ->
            %% FIXME: make msg and metric pool NAME specific
            error_logger:error_report({unknown_pid, Pid,
                                       erlang:get_stacktrace()}),
            send_metric(<<"pooler.event">>, unknown_pid, history),
            Pool
    end.

-spec start_n_pids(non_neg_integer(), atom() | pid(), pid(), dict()) ->
    {dict(), [pid()]}.
start_n_pids(N, PoolName, PoolSup, AllMembers) ->
    NewPids = do_n(N, fun(Acc) ->
                              case supervisor:start_child(PoolSup, []) of
                                  {ok, Pid} ->
                                      %% FIXME: we should monitor instead
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

pool_add_retries(#pool{add_member_retry = Retries}) ->
    Retries.

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

-spec cull_members_from_pool(#pool{}) -> #pool{}.
cull_members_from_pool(#pool{cull_interval = {0, _}} = Pool) ->
    %% 0 cull_interval means do not cull
    Pool;
cull_members_from_pool(#pool{name = PoolName,
                             free_count = FreeCount,
                             init_count = InitCount,
                             in_use_count = InUseCount,
                             cull_interval = Delay,
                             max_age = MaxAge,
                             all_members = AllMembers} = Pool) ->
    MaxCull = FreeCount - (InitCount - InUseCount),
    Pool1 = case MaxCull > 0 of
                true ->
                    MemberInfo = member_info(Pool#pool.free_pids, AllMembers),
                    ExpiredMembers =
                        expired_free_members(MemberInfo, os:timestamp(), MaxAge),
                    CullList = lists:sublist(ExpiredMembers, MaxCull),
                    lists:foldl(fun({CullMe, _}, S) -> remove_pid(CullMe, S) end,
                                Pool, CullList);
                false ->
                    Pool
            end,
    schedule_cull(PoolName, Delay),
    Pool1.

-spec schedule_cull(PoolName :: atom() | pid(),
                    Delay :: time_spec()) -> reference().
%% @doc Schedule a pool cleaning or "cull" for `PoolName' in which
%% members older than `max_age' will be removed until the pool has
%% `init_count' members. Uses `erlang:send_after/3' for light-weight
%% timer that will be auto-cancelled upon pooler shutdown.
schedule_cull(PoolName, Delay) ->
    DelayMillis = time_as_millis(Delay),
    %% use pid instead of server name atom to take advantage of
    %% automatic cancelling
    erlang:send_after(DelayMillis, PoolName, cull_pool).

-spec member_info([pid()], dict()) -> [{pid(), member_info()}].
member_info(Pids, AllMembers) ->
    [ {P, dict:fetch(P, AllMembers)} || P <- Pids ].

-spec expired_free_members(Members :: [{pid(), member_info()}],
                           Now :: {_, _, _},
                           MaxAge :: time_spec()) -> [{pid(), free_member_info()}].
expired_free_members(Members, Now, MaxAge) ->
    MaxMicros = time_as_micros(MaxAge),
    [ MI || MI = {_, {_, free, LastReturn}} <- Members,
            timer:now_diff(Now, LastReturn) >= MaxMicros ].

-spec send_metric(Name :: metric_label(),
                  Value :: metric_value(),
                  Type :: metric_type()) -> ok.
%% Send a metric using the metrics module from application config or
%% do nothing.
send_metric(Name, Value, Type) ->
    case application:get_env(pooler, metrics_module) of
        undefined -> ok;
        {ok, Mod} -> Mod:notify(Name, Value, Type)
    end,
    ok.

-spec pool_metric(atom(), 'free_count' | 'in_use_count' | 'take_rate') -> binary().
pool_metric(PoolName, Metric) ->
    iolist_to_binary([<<"pooler.">>, atom_to_binary(PoolName, utf8),
                      ".", atom_to_binary(Metric, utf8)]).

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
