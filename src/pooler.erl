-module(pooler).
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
         return_member/2,
         % remove_pool/2,
         % add_pool/1,
         pool_stats/0]).

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

%% @doc Obtain exclusive access to a member from a randomly selected pool.
-spec take_member() -> pid().
take_member() ->
    gen_server:call(?SERVER, take_member).

%% @doc Return a member to the pool so it can be reused.
%%
%% If `Status' is 'ok', the member is returned to the pool.  If
%% `Status' is 'fail', the member is destroyed and a new member is
%% added to the pool in its place.
-spec return_member(pid(), ok | fail) -> ok.
return_member(Pid, Status) when Status == ok; Status == fail ->
    CPid = self(),
    gen_server:cast(?SERVER, {return_member, Pid, Status, CPid}),
    ok.

% TODO:
% remove_pool(Name, How) when How == graceful; How == immediate ->
%     gen_server:call(?SERVER, {remove_pool, Name, How}).

% TODO:
% add_pool(Pool) ->
%     gen_server:call(?SERVER, {add_pool, Pool}).

%% @doc Obtain runtime state info for a given pool.
-spec pool_stats() -> [tuple()].
pool_stats() ->
    gen_server:call(?SERVER, pool_stats).


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Config) ->
    PoolRecs = [ props_to_pool(P) || P <- ?gv(pools, Config) ],
    Pools = [ {Pool#pool.name, Pool} || Pool <-  PoolRecs ],
    PoolSups =
        lists:map(
          fun(#pool{name = Name, start_mfa = MFA}) ->
                  {ok, SupPid} = supervisor:start_child(pooler_pool_sup, [MFA]),
                  {Name, SupPid}
          end, PoolRecs),
    State0 = #state{npools = length(Pools),
                    pools = dict:from_list(Pools),
                    pool_sups = dict:from_list(PoolSups),
                    pool_selector = array:from_list([PN || {PN, _} <- Pools])
                  },
    {ok, State} = lists:foldl(
                    fun(#pool{name = PName, init_count = N}, {ok, AccState}) ->
                            add_pids(PName, N, AccState)
                    end, {ok, State0}, PoolRecs),
    process_flag(trap_exit, true),
    {ok, State}.

handle_call(take_member, {CPid, _Tag},
            State = #state{pool_selector = PS, npools = NP}) ->
    PoolName = array:get(crypto:rand_uniform(0, NP), PS),
    {NewPid, NewState} = take_member(PoolName, CPid, State),
    {reply, NewPid, NewState};
handle_call(stop, _From, State) ->
    {stop, normal, stop_ok, State};
handle_call(pool_stats, _From, State) ->
    {reply, dict:to_list(State#state.all_members), State};
handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

handle_cast({return_member, Pid, Status, _CPid}, State) ->
    {noreply, do_return_member2(Pid, Status, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State) ->
    State1 =
        case dict:find(Pid, State#state.all_members) of
            {ok, {_PoolName, _ConsumerPid, _Time}} ->
                do_return_member2(Pid, fail, State);
            error ->
                case dict:find(Pid, State#state.consumer_to_pid) of
                    {ok, Pids} ->
                        IsOk = case Reason of
                                   normal -> ok;
                                   _Crash -> fail
                               end,
                        lists:foldl(
                          fun(P, S) -> do_return_member2(P, IsOk, S) end,
                          State, Pids);
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
% in a spawned process to avoid tying up the loop.
add_pids(error, _N, State) ->
    {bad_pool_name, State};
add_pids(PoolName, N, State) ->
    #state{pools = Pools, pool_sups = PoolSups,
           all_members = AllMembers} = State,
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
            AllMembers1 = lists:foldl(
                            fun(M, Dict) ->
                                    Time = os:timestamp(),
                                    dict:store(M, {PoolName, free, Time}, Dict)
                            end, AllMembers, NewPids),
            Pool1 = Pool#pool{free_pids = Free ++ NewPids},
            {ok, State#state{pools = dict:store(PoolName, Pool1, Pools),
                             all_members = AllMembers1}};
        false ->
            {max_count_reached, State}
    end.

take_member(PoolName, From, State) ->
    #state{pools = Pools, consumer_to_pid = CPMap} = State,
    Pool = dict:fetch(PoolName, Pools),
    #pool{max_count = Max, free_pids = Free, in_use_count = NumInUse} = Pool,
    case Free of
        [] when NumInUse == Max ->
            {error_no_members, State};
        [] when NumInUse < Max ->
            case add_pids(PoolName, 1, State) of
                {ok, State1} ->
                    take_member(PoolName, From, State1);
                {max_count_reached, _} ->
                    {error_no_members, State}
            end;
        [Pid|Rest] ->
            erlang:link(From),
            Pool1 = Pool#pool{free_pids = Rest, in_use_count = NumInUse + 1},
            CPMap1 = dict:update(From, fun(O) -> [Pid|O] end, [Pid], CPMap),
            AllMembers =
                dict:update(Pid,
                            fun({PName, free, Time}) -> {PName, From, Time} end,
                            State#state.all_members),
            {Pid, State#state{pools = dict:store(PoolName, Pool1, Pools),
                              consumer_to_pid = CPMap1,
                              all_members = AllMembers}}
    end.

-spec do_return_member2(pid(), ok | fail, #state{}) -> #state{}.
do_return_member2(Pid, ok, State = #state{all_members = AllMembers}) ->
    {PoolName, _CPid, _} = dict:fetch(Pid, AllMembers),
    Pool = dict:fetch(PoolName, State#state.pools),
    #pool{free_pids = Free, in_use_count = NumInUse} = Pool,
    Pool1 = Pool#pool{free_pids = [Pid | Free], in_use_count = NumInUse - 1},
    State#state{pools = dict:store(PoolName, Pool1, State#state.pools),
                all_members = dict:store(Pid, {PoolName, free, os:timestamp()},
                                         AllMembers)};
do_return_member2(Pid, fail, State = #state{all_members = AllMembers}) ->
    % for the fail case, perhaps the member crashed and was alerady
    % removed, so use find instead of fetch and ignore missing.
    case dict:find(Pid, AllMembers) of
        {ok, {PoolName, _, _}} ->
            State1 = remove_pid(Pid, State),
            {Status, State2} = add_pids(PoolName, 1, State1),
            case Status =:= ok orelse Status =:= max_count_reached of
                true ->
                    State2;
                false ->
                    erlang:error({error, "unexpected return from add_pid",
                                  Status, erlang:get_stacktrace()})
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
-spec cpmap_remove(pid(), pid(), dict()) -> dict().
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
            Pool = dict:fetch(PoolName, Pools),
            Pool1 = lists:delete(Pid, Pool#pool.free_pids),
            exit(Pid, kill),
            State#state{pools = dict:store(PoolName, Pool1, Pools),
                        all_members = dict:erase(Pid, AllMembers)};
        {ok, {PoolName, CPid, _Time}} ->
            Pool = dict:fetch(PoolName, Pools),
            Pool1 = Pool#pool{in_use_count = Pool#pool.in_use_count - 1},
            exit(Pid, kill),
            State#state{pools = dict:store(PoolName, Pool1, Pools),
                        consumer_to_pid = cpmap_remove(Pid, CPid, CPMap),
                        all_members = dict:erase(Pid, AllMembers)};
        error ->
            error_logger:error_report({unknown_pid, Pid,
                                       erlang:get_stacktrace()}),
            State
    end.
