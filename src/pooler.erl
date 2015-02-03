%% @author Seth Falcon <seth@userprimary.net>
%% @copyright 2011-2013 Seth Falcon
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
-type metric_value() :: 'unknown_pid' |
                        non_neg_integer() |
                        {'add_pids_failed', non_neg_integer(), non_neg_integer()} |
                        {'inc',1} |
                        'error_no_members'.
-type metric_type() :: 'counter' | 'histogram' | 'history' | 'meter'.

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([accept_member/2,
         start_link/1,
         take_member/1,
         take_member/2,
         take_group_member/1,
         return_group_member/2,
         return_group_member/3,
         return_member/2,
         return_member/3,
         pool_stats/1,
         manual_start/0,
         new_pool/1,
         pool_child_spec/1,
         rm_pool/1,
         rm_group/1
        ]).

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

manual_start() ->
    application:start(sasl),
    application:start(pooler).

%% @doc Start a new pool described by the proplist `PoolConfig'. The
%% following keys are required in the proplist:
%%
%% <dl>
%% <dt>`name'</dt>
%% <dd>An atom giving the name of the pool.</dd>
%% <dt>`init_count'</dt>
%% <dd>Number of members to add to the pool at start. When the pool is
%% started, `init_count' members will be started in parallel.</dd>
%% <dt>`max_count'</dt>
%% <dd>Maximum number of members in the pool.</dd>
%% <dt>`start_mfa'</dt>
%% <dd>A tuple of the form `{Mod, Fun, Args}' describing how to start
%% new pool members.</dd>
%% </dl>
%%
%% In addition, you can specify any of the following optional
%% configuration options:
%%
%% <dl>
%% <dt>`group'</dt>
%% <dd>An atom giving the name of the group this pool belongs
%% to. Pools sharing a common `group' value can be accessed using
%% {@link take_group_member/1} and {@link return_group_member/2}.</dd>
%% <dt>`cull_interval'</dt>
%% <dd>Time between checks for stale pool members. Specified as
%% `{Time, Unit}' where `Time' is a non-negative integer and `Unit' is
%% one of `min', `sec', `ms', or `mu'. The default value of `{1, min}'
%% triggers a once per minute check to remove members that have not
%% been accessed in `max_age' time units. Culling can be disabled by
%% specifying a zero time vaule (e.g. `{0, min}'. Culling will also be
%% disabled if `init_count' is the same as `max_count'.</dd>
%% <dt>`max_age'</dt>
%% <dd>Members idle longer than `max_age' time units are removed from
%% the pool when stale checking is enabled via
%% `cull_interval'. Culling of idle members will never reduce the pool
%% below `init_count'. The value is specified as `{Time, Unit}'. Note
%% that timers are not set on individual pool members and may remain
%% in the pool beyond the configured `max_age' value since members are
%% only removed on the interval configured via `cull_interval'. The
%% default value is `{30, sec}'.</dd>
%% <dt>`member_start_timeout'</dt>
%% <dd>Time limit for member starts. Specified as `{Time,
%% Unit}'. Defaults to `{1, min}'.</dd>
%% </dl>
new_pool(PoolConfig) ->
    pooler_sup:new_pool(PoolConfig).

%% @doc Terminate the named pool.
rm_pool(PoolName) ->
    pooler_sup:rm_pool(PoolName).

%% @doc Terminates the group and all pools in that group.
%%
%% If termination of any member pool fails, `rm_group/1` returns
%% `{error, {failed_delete_pools, Pools}}`, where `Pools` is a list
%% of pools that failed to terminate.
%%
%% The group is NOT terminated if any member pool did not
%% successfully terminate.
%%
-spec rm_group(atom()) -> ok | {error, {failed_rm_pools, [atom()]}}.
rm_group(GroupName) ->
    case pg2:get_local_members(GroupName) of
        {error, {no_such_group, GroupName}} ->
            ok;
        Pools ->
            case rm_group_members(Pools) of
                [] ->
                    pg2:delete(GroupName);
                Failures ->
                    {error, {failed_rm_pools, Failures}}
            end
    end.

-spec rm_group_members([pid()]) -> [atom()].
rm_group_members(MemberPids) ->
    lists:foldl(
      fun(MemberPid, Acc) ->
              Pool = gen_server:call(MemberPid, dump_pool),
              PoolName = Pool#pool.name,
              case pooler_sup:rm_pool(PoolName) of
                  ok -> Acc;
                  _  -> [PoolName | Acc]
              end
      end,
      [],
      MemberPids).

%% @doc Get child spec described by the proplist `PoolConfig'.
%%
%% See {@link pooler:new_pool/1} for info about `PoolConfig'.
-spec pool_child_spec([{atom(), term()}]) -> supervisor:child_spec().
pool_child_spec(PoolConfig) ->
    pooler_sup:pool_child_spec(PoolConfig).

%% @doc For INTERNAL use. Adds `MemberPid' to the pool.
-spec accept_member(atom() | pid(), pid() | {noproc, _}) -> ok.
accept_member(PoolName, MemberPid) ->
    gen_server:call(PoolName, {accept_member, MemberPid}).

%% @doc Obtain exclusive access to a member from `PoolName'.
%%
%% If no free members are available, 'error_no_members' is returned.
%%
-spec take_member(atom() | pid()) -> pid() | error_no_members.
take_member(PoolName) when is_atom(PoolName) orelse is_pid(PoolName) ->
    gen_server:call(PoolName, {take_member, 0}, infinity).

%% @doc Obtain exclusive access to a member of 'PoolName'.
%%
%% If no members are available, wait for up to Timeout milliseconds for a member
%% to become available. Waiting requests are served in FIFO order. If no member
%% is available within the specified timeout, error_no_members is returned.
%% `Timeout' can be either milliseconds as integer or `{duration, time_unit}'
%%
-spec take_member(atom() | pid(), non_neg_integer() | time_spec()) -> pid() | error_no_members.
take_member(PoolName, Timeout) when is_atom(PoolName) orelse is_pid(PoolName) ->
    gen_server:call(PoolName, {take_member, time_as_millis(Timeout)}, infinity).


%% @doc Take a member from a randomly selected member of the group
%% `GroupName'. Returns `MemberPid' or `error_no_members'.  If no
%% members are available in the randomly chosen pool, all other pools
%% in the group are tried in order.
-spec take_group_member(atom()) -> pid() | error_no_members | {error_no_group, atom()}.
take_group_member(GroupName) ->
    case pg2:get_local_members(GroupName) of
        {error, {no_such_group, GroupName}} ->
            {error_no_group, GroupName};
        [] ->
            error_no_members;
        Pools ->
            %% Put a random member at the front of the list and then
            %% return the first member you can walking the list.
            {_, _, X} = erlang:now(),
            Idx = (X rem length(Pools)) + 1,
            {PoolPid, Rest} = extract_nth(Idx, Pools),
            take_first_pool([PoolPid | Rest])
    end.

take_first_pool([PoolPid | Rest]) ->
    case take_member(PoolPid) of
        error_no_members ->
            take_first_pool(Rest);
        Member ->
            ets:insert(?POOLER_GROUP_TABLE, {Member, PoolPid}),
            Member
    end;
take_first_pool([]) ->
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
    #pool{init_count = N} = Pool,
    MemberSup = pooler_pool_sup:member_sup_name(Pool),
    Pool1 = set_member_sup(Pool, MemberSup),
    %% This schedules the next cull when the pool is configured for
    %% such and is otherwise a no-op.
    Pool2 = cull_members_from_pool(Pool1),
    {ok, NewPool} = init_members_sync(N, Pool2),
    %% trigger an immediate timeout, handled by handle_info to allow
    %% us to register with pg2. We use the timeout mechanism to ensure
    %% that a server is added to a group only when it is ready to
    %% process messages.
    {ok, NewPool, 0}.

set_member_sup(#pool{} = Pool, MemberSup) ->
    Pool#pool{member_sup = MemberSup}.

handle_call({take_member, Timeout}, From = {APid, _}, #pool{} = Pool) when is_pid(APid) ->
    maybe_reply(take_member_from_pool_queued(Pool, From, Timeout));

handle_call({return_member, Pid, Status}, {_CPid, _Tag}, Pool) ->
    {reply, ok, do_return_member(Pid, Status, Pool)};
handle_call({accept_member, Pid}, _From, Pool) ->
    {reply, ok, do_accept_member(Pid, Pool)};
handle_call(stop, _From, Pool) ->
    {stop, normal, stop_ok, Pool};
handle_call(pool_stats, _From, Pool) ->
    {reply, dict:to_list(Pool#pool.all_members), Pool};
handle_call(dump_pool, _From, Pool) ->
    {reply, Pool, Pool};
handle_call(_Request, _From, Pool) ->
    {noreply, Pool}.

-spec handle_cast(_,_) -> {'noreply', _}.
handle_cast(_Msg, Pool) ->
    {noreply, Pool}.

-spec handle_info(_, _) -> {'noreply', _}.
handle_info({requestor_timeout, From}, Pool = #pool{ queued_requestors = RequestorQueue }) ->
    NewQueue = queue:filter(fun({RequestorFrom, _TRef}) when RequestorFrom =:= From ->
                                    gen_server:reply(RequestorFrom, error_no_members),
                                    false;
                               ({_, _}) ->
                                    true
                            end, RequestorQueue),
    {noreply, Pool#pool{ queued_requestors = NewQueue} };
handle_info(timeout, #pool{group = undefined} = Pool) ->
    %% ignore
    {noreply, Pool};
handle_info(timeout, #pool{group = Group} = Pool) ->
    ok = pg2:create(Group),
    ok = pg2:join(Group, self()),
    {noreply, Pool};
handle_info({'DOWN', MRef, process, Pid, Reason}, State) ->
    State1 =
        case dict:find(Pid, State#pool.all_members) of
            {ok, {_PoolName, _ConsumerPid, _Time}} ->
                do_return_member(Pid, fail, State);
            error ->
                case dict:find(Pid, State#pool.consumer_to_pid) of
                    {ok, {MRef, Pids}} ->
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

do_accept_member({StarterPid, Pid},
                 #pool{
                    all_members = AllMembers,
                    starting_members = StartingMembers0,
                    member_start_timeout = StartTimeout
                   } = Pool) when is_pid(Pid) ->
    %% make sure we don't accept a timedout member
    Pool1 = #pool{starting_members = StartingMembers} =
        remove_stale_starting_members(Pool, StartingMembers0, StartTimeout),
    case lists:keymember(StarterPid, 1, StartingMembers) of
        false ->
            %% A starter completed even though we invalidated the pid
            %% Ask the starter to kill the child and stop. In most cases, the
            %% starter has already received this message. However, when pools
            %% are dynamically re-created with the same name, it is possible
            %% to receive an accept from a pool that has since gone away.
            %% In this case, we should cleanup.
            pooler_starter:stop_member_async(StarterPid),
            Pool1;
        true ->
            StartingMembers1 = lists:keydelete(StarterPid, 1, StartingMembers),
            MRef = erlang:monitor(process, Pid),
            Entry = {MRef, free, os:timestamp()},
            AllMembers1 = store_all_members(Pid, Entry, AllMembers),
            pooler_starter:stop(StarterPid),
            maybe_reply_with_pid(Pid, Pool1#pool{all_members = AllMembers1,
                                                 starting_members = StartingMembers1})
    end;
do_accept_member({StarterPid, _Reason},
                 #pool{starting_members = StartingMembers0,
                       member_start_timeout = StartTimeout} = Pool) ->
    %% member start failed, remove in-flight ref and carry on.
    pooler_starter:stop(StarterPid),
    Pool1 = #pool{starting_members = StartingMembers} =
        remove_stale_starting_members(Pool, StartingMembers0,
                                      StartTimeout),
    StartingMembers1 = lists:keydelete(StarterPid, 1, StartingMembers),
    Pool1#pool{starting_members = StartingMembers1}.


maybe_reply_with_pid(Pid,
                     Pool = #pool{queued_requestors = QueuedRequestors,
                                  free_pids = Free,
                                  free_count = NumFree}) when is_pid(Pid) ->
    case queue:out(QueuedRequestors) of
        {empty, _} ->
            Pool#pool{free_pids = [Pid | Free],
                      free_count = NumFree + 1};
        {{value, {From = {APid, _}, TRef}}, NewQueuedRequestors} when is_pid(APid) ->
            reply_to_queued_requestor(TRef, Pid, From, NewQueuedRequestors, Pool)
    end.

reply_to_queued_requestor(TRef, Pid, From = {APid, _}, NewQueuedRequestors, Pool) when is_pid(APid) ->
    erlang:cancel_timer(TRef),
    Pool1 = take_member_bookkeeping(Pid, From, NewQueuedRequestors, Pool),
    send_metric(Pool, in_use_count, Pool1#pool.in_use_count, histogram),
    send_metric(Pool, free_count, Pool1#pool.free_count, histogram),
    send_metric(Pool, events, error_no_members, history),
    gen_server:reply(From, Pid),
    Pool1.
    

-spec take_member_bookkeeping(pid(),
                              {pid(), _},
                              [pid()] | p_requestor_queue(),
                              #pool{}) -> #pool{}.
take_member_bookkeeping(MemberPid,
                        {CPid, _},
                        Rest,
                        Pool = #pool{in_use_count = NumInUse,
                                     free_count = NumFree,
                                     consumer_to_pid = CPMap,
                                     all_members = AllMembers})
  when is_pid(MemberPid),
       is_pid(CPid),
       is_list(Rest) ->
    Pool#pool{free_pids = Rest,
              in_use_count = NumInUse + 1,
              free_count = NumFree - 1,
              consumer_to_pid = add_member_to_consumer(MemberPid, CPid, CPMap),
              all_members = set_cpid_for_member(MemberPid, CPid, AllMembers)
             };
take_member_bookkeeping(MemberPid,
                        {ReplyPid, _Tag},
                        NewQueuedRequestors,
                        Pool = #pool{
                                  in_use_count = NumInUse,
                                  all_members = AllMembers,
                                  consumer_to_pid = CPMap
                                 }) ->
    Pool#pool{
      in_use_count = NumInUse + 1,
      all_members = set_cpid_for_member(MemberPid, ReplyPid, AllMembers),
      consumer_to_pid = add_member_to_consumer(MemberPid, ReplyPid, CPMap),
      queued_requestors = NewQueuedRequestors
     }.

-spec remove_stale_starting_members(#pool{}, [{reference(), erlang:timestamp()}],
                                    time_spec()) -> #pool{}.
remove_stale_starting_members(Pool, StartingMembers, MaxAge) ->
    Now = os:timestamp(),
    MaxAgeSecs = time_as_secs(MaxAge),
    FilteredStartingMembers = lists:foldl(fun(SM, AccIn) ->
                         accumulate_starting_member_not_stale(Pool, Now, SM, MaxAgeSecs, AccIn)
                 end, [], StartingMembers),
    Pool#pool{starting_members = FilteredStartingMembers}.

accumulate_starting_member_not_stale(Pool, Now, SM = {Pid, StartTime}, MaxAgeSecs, AccIn) ->
    case secs_between(StartTime, Now) < MaxAgeSecs of
        true ->
            [SM | AccIn];
        false ->
            error_logger:error_msg("pool '~s': starting member timeout", [Pool#pool.name]),
            send_metric(Pool, starting_member_timeout, {inc, 1}, counter),
            pooler_starter:stop_member_async(Pid),
            AccIn
    end.

init_members_sync(N, #pool{name = PoolName} = Pool) ->
    Self = self(),
    StartTime = os:timestamp(),
    StartRefs = [ {pooler_starter:start_member(Pool, Self), StartTime}
                  || _I <- lists:seq(1, N) ],
    Pool1 = Pool#pool{starting_members = StartRefs},
    case collect_init_members(Pool1) of
        timeout ->
            error_logger:error_msg("pool '~s': exceeded timeout waiting for ~B members",
                                   [PoolName, Pool1#pool.init_count]),
            error({timeout, "unable to start members"});
        #pool{} = Pool2 ->
            {ok, Pool2}
    end.

collect_init_members(#pool{starting_members = []} = Pool) ->
    Pool;
collect_init_members(#pool{member_start_timeout = StartTimeout} = Pool) ->
    Timeout = time_as_millis(StartTimeout),
    receive
        {accept_member, {Ref, Member}} ->
            collect_init_members(do_accept_member({Ref, Member}, Pool))
    after
        Timeout ->
            timeout
    end.

-spec take_member_from_pool(#pool{}, {pid(), term()}) ->
                                   {error_no_members | pid(), #pool{}}.
take_member_from_pool(#pool{init_count = InitCount,
                            max_count = Max,
                            free_pids = Free,
                            in_use_count = NumInUse,
                            free_count = NumFree,
                            starting_members = StartingMembers,
                            member_start_timeout = StartTimeout} = Pool,
                      From) ->
    send_metric(Pool, take_rate, 1, meter),
    Pool1 = remove_stale_starting_members(Pool, StartingMembers, StartTimeout),
    NonStaleStartingMemberCount = length(Pool1#pool.starting_members),
    NumCanAdd = Max - (NumInUse + NumFree + NonStaleStartingMemberCount),
    case Free of
        [] when NumCanAdd =< 0  ->
            send_metric(Pool, error_no_members_count, {inc, 1}, counter),
            send_metric(Pool, events, error_no_members, history),
            {error_no_members, Pool1};
        [] when NumCanAdd > 0 ->
            %% Limit concurrently starting members to init_count. Add
            %% up to init_count members. Starting members here means
            %% we always return an error_no_members for a take request
            %% when all members are in-use. By adding a batch of new
            %% members, the pool should reach a steady state with
            %% unused members culled over time (if scheduled cull is
            %% enabled).
            NumToAdd = max(min(InitCount - NonStaleStartingMemberCount, NumCanAdd), 1),
            Pool2 = add_members_async(NumToAdd, Pool1),
            send_metric(Pool, error_no_members_count, {inc, 1}, counter),
            send_metric(Pool, events, error_no_members, history),
            {error_no_members, Pool2};
        [Pid|Rest] ->
            Pool2 = take_member_bookkeeping(Pid, From, Rest, Pool1),
            Pool3 = case Pool2#pool.auto_grow_threshold of
                        N when is_integer(N) andalso
                               Pool2#pool.free_count =< N andalso
                               NumCanAdd > 0 ->
                            NumToAdd = max(min(InitCount - NonStaleStartingMemberCount, NumCanAdd), 0),
                            add_members_async(NumToAdd, Pool2);
                        _ ->
                            Pool2
                    end,
            send_metric(Pool, in_use_count, Pool3#pool.in_use_count, histogram),
            send_metric(Pool, free_count, Pool3#pool.free_count, histogram),
            {Pid, Pool3}
    end.

-spec take_member_from_pool_queued(#pool{},
                                   {pid(), _},
                                   non_neg_integer()) ->
                                   {error_no_members | queued | pid(), #pool{}}.
take_member_from_pool_queued(Pool0 = #pool{queue_max = QMax,
                                           queued_requestors = Requestors},
                             From = {CPid, _},
                             Timeout) when is_pid(CPid) ->
    case {take_member_from_pool(Pool0, From), queue:len(Requestors)} of
        {{error_no_members, Pool1}, QLen} when QLen >= QMax ->
            send_metric(Pool1, events, error_no_members, history),
            send_metric(Pool1, queue_max_reached, {inc, 1}, counter),
            {error_no_members, Pool1};
        {{error_no_members, Pool1}, _} when Timeout =:= 0 ->
            {error_no_members, Pool1};
        {{error_no_members, Pool1 = #pool{queued_requestors = QueuedRequestors}}, QueueCount} ->
            TRef = erlang:send_after(Timeout, self(), {requestor_timeout, From}),
            send_metric(Pool1, queue_count, QueueCount, histogram),
            {queued, Pool1#pool{queued_requestors = queue:in({From, TRef}, QueuedRequestors)}};
        {{Member, NewPool}, _} when is_pid(Member) ->
            {Member, NewPool}
    end.

%% @doc Add `Count' members to `Pool' asynchronously. Returns updated
%% `Pool' record with starting member refs added to field
%% `starting_members'.
add_members_async(Count, #pool{starting_members = StartingMembers} = Pool) ->
    StartTime = os:timestamp(),
    StartRefs = [ {pooler_starter:start_member(Pool), StartTime}
                  || _I <- lists:seq(1, Count) ],
    Pool#pool{starting_members = StartRefs ++ StartingMembers}.

-spec do_return_member(pid(), ok | fail, #pool{}) -> #pool{}.
do_return_member(Pid, ok, #pool{name = PoolName,
                                all_members = AllMembers,
                                queued_requestors = QueuedRequestors} = Pool) ->
    clean_group_table(Pid, Pool),
    case dict:find(Pid, AllMembers) of
        {ok, {_, free, _}} ->
            Fmt = "pool '~s': ignored return of free member ~p",
            error_logger:warning_msg(Fmt, [PoolName, Pid]),
            Pool;
        {ok, {MRef, CPid, _}} ->
            #pool{free_pids = Free, in_use_count = NumInUse,
                  free_count = NumFree} = Pool,
            Pool1 = Pool#pool{in_use_count = NumInUse - 1},
            Entry = {MRef, free, os:timestamp()},
            Pool2 = Pool1#pool{all_members = store_all_members(Pid, Entry, AllMembers),
                       consumer_to_pid = cpmap_remove(Pid, CPid,
                                                      Pool1#pool.consumer_to_pid)},
            case queue:out(QueuedRequestors) of
                {empty, _ } ->
                    Pool2#pool{free_pids = [Pid | Free], free_count = NumFree + 1};
                {{value, {From = {APid, _}, TRef}}, NewQueuedRequestors} when is_pid(APid) ->
                    reply_to_queued_requestor(TRef, Pid, From, NewQueuedRequestors, Pool2)
            end;
        error ->
            Pool
    end;
do_return_member(Pid, fail, #pool{all_members = AllMembers} = Pool) ->
    % for the fail case, perhaps the member crashed and was alerady
    % removed, so use find instead of fetch and ignore missing.
    clean_group_table(Pid, Pool),
    case dict:find(Pid, AllMembers) of
        {ok, {_MRef, _, _}} ->
            Pool1 = remove_pid(Pid, Pool),
            add_members_async(1, Pool1);
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
-spec cpmap_remove(pid(), pid() | free, p_dict()) -> p_dict().
cpmap_remove(_Pid, free, CPMap) ->
    CPMap;
cpmap_remove(Pid, CPid, CPMap) ->
    case dict:find(CPid, CPMap) of
        {ok, {MRef, Pids0}} ->
            Pids1 = lists:delete(Pid, Pids0),
            case Pids1 of
                [_H|_T] ->
                    dict:store(CPid, {MRef, Pids1}, CPMap);
                [] ->
                    %% no more members for this consumer
                    erlang:demonitor(MRef, [flush]),
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
    #pool{name = PoolName,
          all_members = AllMembers,
          consumer_to_pid = CPMap} = Pool,
    case dict:find(Pid, AllMembers) of
        {ok, {MRef, free, _Time}} ->
            % remove an unused member
            erlang:demonitor(MRef, [flush]),
            FreePids = lists:delete(Pid, Pool#pool.free_pids),
            NumFree = Pool#pool.free_count - 1,
            Pool1 = Pool#pool{free_pids = FreePids, free_count = NumFree},
            exit(Pid, kill),
            send_metric(Pool1, killed_free_count, {inc, 1}, counter),
            Pool1#pool{all_members = dict:erase(Pid, AllMembers)};
        {ok, {MRef, CPid, _Time}} ->
            %% remove a member being consumed. No notice is sent to
            %% the consumer.
            erlang:demonitor(MRef, [flush]),
            Pool1 = Pool#pool{in_use_count = Pool#pool.in_use_count - 1},
            exit(Pid, kill),
            send_metric(Pool1, killed_in_use_count, {inc, 1}, counter),
            Pool1#pool{consumer_to_pid = cpmap_remove(Pid, CPid, CPMap),
                       all_members = dict:erase(Pid, AllMembers)};
        error ->
            error_logger:error_report({{pool, PoolName}, unknown_pid, Pid,
                                       erlang:get_stacktrace()}),
            send_metric(Pool, events, unknown_pid, history),
            Pool
    end.

-spec store_all_members(pid(),
                        {reference(), free | pid(), {_, _, _}}, p_dict()) -> p_dict().
store_all_members(Pid, Val = {_MRef, _CPid, _Time}, AllMembers) ->
    dict:store(Pid, Val, AllMembers).

-spec set_cpid_for_member(pid(), pid(), p_dict()) -> p_dict().
set_cpid_for_member(MemberPid, CPid, AllMembers) ->
    dict:update(MemberPid,
                fun({MRef, free, Time = {_, _, _}}) ->
                        {MRef, CPid, Time}
                end, AllMembers).

-spec add_member_to_consumer(pid(), pid(), p_dict()) -> p_dict().
add_member_to_consumer(MemberPid, CPid, CPMap) ->
    %% we can't use dict:update here because we need to create the
    %% monitor if we aren't already tracking this consumer.
    case dict:find(CPid, CPMap) of
        {ok, {MRef, MList}} ->
            dict:store(CPid, {MRef, [MemberPid | MList]}, CPMap);
        error ->
            MRef = erlang:monitor(process, CPid),
            dict:store(CPid, {MRef, [MemberPid]}, CPMap)
    end.

-spec cull_members_from_pool(#pool{}) -> #pool{}.
cull_members_from_pool(#pool{cull_interval = {0, _}} = Pool) ->
    %% 0 cull_interval means do not cull
    Pool;
cull_members_from_pool(#pool{init_count = C, max_count = C} = Pool) ->
    %% if init_count matches max_count, then we will not dynamically
    %% add capacity and should not schedule culling regardless of
    %% cull_interval config.
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

-spec member_info([pid()], p_dict()) -> [{pid(), member_info()}].
member_info(Pids, AllMembers) ->
    [ {P, dict:fetch(P, AllMembers)} || P <- Pids ].

-spec expired_free_members(Members :: [{pid(), member_info()}],
                           Now :: {_, _, _},
                           MaxAge :: time_spec()) -> [{pid(), free_member_info()}].
expired_free_members(Members, Now, MaxAge) ->
    MaxMicros = time_as_micros(MaxAge),
    [ MI || MI = {_, {_, free, LastReturn}} <- Members,
            timer:now_diff(Now, LastReturn) >= MaxMicros ].

%% Send a metric using the metrics module from application config or
%% do nothing.
-spec send_metric(Pool  :: #pool{},
                  Label :: atom(),
                  Value :: metric_value(),
                  Type  :: metric_type()) -> ok.
send_metric(#pool{metrics_mod = pooler_no_metrics}, _Label, _Value, _Type) ->
    ok;
send_metric(#pool{name = PoolName, metrics_mod = MetricsMod,
                  metrics_api = exometer}, Label, {inc, Value}, counter) ->
    MetricName = pool_metric_exometer(PoolName, Label),
    MetricsMod:update_or_create(MetricName, Value, counter, []),
    ok;
send_metric(#pool{name = PoolName, metrics_mod = MetricsMod,
                   metrics_api = exometer}, Label, {dec, Value}, counter) ->
    MetricName = pool_metric_exometer(PoolName, Label),
    MetricsMod:update_or_create(MetricName, - Value, counter, []),
    ok;
% Exometer does not support 'history' type metrics right now.
send_metric(#pool{name = _PoolName, metrics_mod = _MetricsMod,
                  metrics_api = exometer}, _Label, _Value, history) ->
    ok;
send_metric(#pool{name = PoolName, metrics_mod = MetricsMod,
                   metrics_api = exometer}, Label, Value, Type) ->
    MetricName = pool_metric_exometer(PoolName, Label),
    MetricsMod:update_or_create(MetricName, Value, Type, []),
    ok;
%folsom API is the default one.
send_metric(#pool{name = PoolName, metrics_mod = MetricsMod, metrics_api = folsom},
                   Label, Value, Type) ->
    MetricName = pool_metric(PoolName, Label),
    MetricsMod:notify(MetricName, Value, Type),
    ok.

-spec pool_metric(atom(), atom()) -> binary().
pool_metric(PoolName, Metric) ->
    iolist_to_binary([<<"pooler.">>, atom_to_binary(PoolName, utf8),
                      ".", atom_to_binary(Metric, utf8)]).

%% Exometer metric names are lists, not binaries.
-spec pool_metric_exometer(atom(), atom()) -> nonempty_list(binary()).
pool_metric_exometer(PoolName, Metric) ->
    [<<"pooler">>, atom_to_binary(PoolName, utf8),
     atom_to_binary(Metric, utf8)].


-spec time_as_secs(time_spec()) -> non_neg_integer().
time_as_secs({Time, Unit}) ->
    time_as_micros({Time, Unit}) div 1000000.

-spec time_as_millis(time_spec()) -> non_neg_integer().
%% @doc Convert time unit into milliseconds.
time_as_millis({Time, Unit}) ->
    time_as_micros({Time, Unit}) div 1000;
%% Allows blind convert
time_as_millis(Time) when is_integer(Time) ->
    Time.


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

secs_between({Mega1, Secs1, _}, {Mega2, Secs2, _}) ->
    (Mega2 - Mega1) * 1000000 + (Secs2 - Secs1).

-spec maybe_reply({'queued' | 'error_no_members' | pid(), #pool{}}) ->
                         {noreply, #pool{}} | {reply, 'error_no_members' | pid(), #pool{}}.
maybe_reply({Member, NewPool}) ->
    case Member of
        queued ->
            {noreply, NewPool};
        error_no_members ->
            {reply, error_no_members, NewPool};
        Member when is_pid(Member) ->
            {reply, Member, NewPool}
    end.
