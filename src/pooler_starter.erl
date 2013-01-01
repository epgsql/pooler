%% @author Seth Falcon <seth@userprimary.net>
%% @copyright 2012 Seth Falcon
%% @doc Helper gen_server to start pool members
%%
-module(pooler_starter).
-behaviour(gen_server).

-include("pooler.hrl").
-include_lib("eunit/include/eunit.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0,
         start_member/2]).

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

start_link() ->
    gen_server:start_link(?MODULE, [], []).

%% @doc Start a member for the specified `Pool'.
%%
%% The start member request is a sent as a cast to the starter
%% server. The starter server mailbox is treated as the member start
%% work queue. Members are started serially and sent back to the
%% requesting pool via `pooler:accept_member/2'. It is expected that
%% callers keep track of, and limit, their start requests so that the
%% starters queue doesn't grow unbounded. A likely enhancement would
%% be to allow parallel starts either by having the starter spawn a
%% subprocess and manage or by using pg2 to group a number of starter
%% servers. Note that timeout could be handled client-side using the
%% `gen_server:call/3' timeout value.
-spec start_member(atom() | pid(), #pool{}) -> reference().
start_member(Starter, #pool{} = Pool) ->
    Ref = make_ref(),
    gen_server:cast(Starter, {start_member, Pool, Ref}),
    Ref.

%% @doc Start `Count' members in parallel and return a list of created
%% members. The returned list may contain fewer than `Count' members
%% if errors occured for some member starts.
-spec start_members_sync(atom() | pid(), #pool{},
                         non_neg_integer()) -> [pid()].
start_members_sync(Starter, #pool{} = Pool, Count) ->
    gen_server:call(Starter, {start_members_sync, Pool, Count}, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

-spec init([]) -> {'ok', {}}.
init([]) ->
    {ok, {}}.

handle_call({start_members_sync, Pool, Count}, _From, State) ->
    {reply, do_start_members_sync(Pool, Count), State};
handle_call(stop, _From, State) ->
    {stop, normal, stop_ok, State};
handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({start_member, Pool, Ref}, State) ->
    ok = do_start_member(Pool, Ref),
    {noreply, State}.

do_start_member(#pool{name = PoolName,
                      member_sup = PoolSup},
                Ref) ->
    case supervisor:start_child(PoolSup, []) of
        {ok, Pid} ->
            ok = pooler:accept_member(PoolName, {Ref, Pid}),
            ok;
        Error ->
            error_logger:error_msg("pool '~s' failed to start member: ~p",
                                   [PoolName, Error]),
            pooler:accept_member(PoolName, {Ref, Error}),
            ok
    end.

do_start_members_sync(#pool{name = PoolName,
                            member_sup = PoolSup}, Count) ->
    Parent = self(),
    StarterPids = [ launch_starter(Parent, PoolName, PoolSup)
                    || _I <- lists:seq(1, Count) ],
    gather_pids(StarterPids, []).

launch_starter(Parent, PoolName, PoolSup) ->
    proc_lib:spawn_link(fun() ->
                                Result = start_or_log_error(PoolName, PoolSup),
                                Parent ! {self(), Result}
                        end).

start_or_log_error(PoolName, PoolSup) ->
    case supervisor:start_child(PoolSup, []) of
        {ok, Pid} ->
            {ok, Pid};
        Error ->
            error_logger:error_msg("pool '~s' failed to start member: ~p",
                                   [PoolName, Error]),
            error
    end.

gather_pids([Pid|Rest], Acc) ->
    receive
        {Pid, error} ->
            gather_pids(Rest, Acc);
        {Pid, {ok, MemberPid}} ->
            gather_pids(Rest, [MemberPid | Acc])
    end;
gather_pids([], Acc) ->
    Acc.

-spec handle_info(_, _) -> {'noreply', _}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(_, _) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, _, _) -> {'ok', _}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
