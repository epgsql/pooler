%% @author Seth Falcon <seth@userprimary.net>
%% @copyright 2012-2013 Seth Falcon
%% @doc Helper gen_server to start pool members
%%
-module(pooler_starter).
-behaviour(gen_server).

-include("pooler.hrl").
-include_lib("eunit/include/eunit.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/2,
         start_member/1,
         start_member/2,
         stop_member_async/1,
         stop/1]).

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

start_link(Pool, Parent) ->
    gen_server:start_link(?MODULE, {Pool, Parent}, []).

stop(Starter) ->
    gen_server:cast(Starter, stop).

%% @doc Start a member for the specified `Pool'.
%%
%% Member creation with this call is async. This function returns
%% immediately with create process' pid. When the member has been
%% created it is sent to the specified pool via
%% {@link pooler:accept_member/2}.
%%
%% Each call starts a single use `pooler_starter' instance via
%% `pooler_starter_sup'. The instance terminates normally after
%% creating a single member.
-spec start_member(#pool{}) -> pid().
start_member(Pool) ->
    {ok, Pid} = pooler_starter_sup:new_starter(Pool, pool),
    Pid.

%% @doc Same as {@link start_member/1} except that instead of calling
%% {@link pooler:accept_member/2} a raw message is sent to `Parent' of
%% the form `{accept_member, {Ref, Member}'. Where `Member' will
%% either be the member pid or an error term and `Ref' will be the
%% Pid of the starter.
%%
%% This is used by the init function in the `pooler' to start the
%% initial set of pool members in parallel.
start_member(Pool, Parent) ->
    {ok, Pid} = pooler_starter_sup:new_starter(Pool, Parent),
    Pid.

%% @doc Stop a member in the pool

%% Member creation can take too long. In this case, the starter
%% needs to be informed that even if creation succeeds, the
%% started child should be not be sent back and should be
%% cleaned up
-spec stop_member_async(pid()) -> ok.
stop_member_async(Pid) ->
    gen_server:cast(Pid, stop_member).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
-record(starter, {pool,
                  parent,
                  msg}).

-spec init({#pool{}, pid() | atom()}) -> {'ok', #starter{}, 0}.
init({Pool, Parent}) ->
    %% trigger immediate timeout message, which we'll use to trigger
    %% the member start.
    {ok, #starter{pool = Pool, parent = Parent}, 0}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(stop_member, #starter{msg = {_Me, Pid}, pool = #pool{member_sup = MemberSup}} = State) ->
    %% The process we were starting is no longer valid for the pool.
    %% Cleanup the process and stop normally.
    supervisor:terminate_child(MemberSup, Pid),
    {stop, normal, State};

handle_cast(accept_member, #starter{msg = Msg, parent = Parent, pool = #pool{name = PoolName}} = State) ->
    %% Process creation has succeeded. Send the member to the pooler
    %% gen_server to be accepted. Pooler gen_server will notify
    %% us if the member was accepted or needs to cleaned up.
    send_accept_member(Parent, PoolName, Msg),
    {noreply, State};

handle_cast(stop, State) ->
    {stop, normal, State};


handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(_, _) -> {'noreply', _}.
handle_info(timeout,
            #starter{pool = Pool} = State) ->
    Msg = do_start_member(Pool),
    accept_member_async(self()),
    {noreply, State#starter{msg = Msg}};
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(_, _) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, _, _) -> {'ok', _}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_start_member(#pool{member_sup = PoolSup, name = PoolName}) ->
    case supervisor:start_child(PoolSup, []) of
        {ok, Pid} ->
            {self(), Pid};
        Error ->
            error_logger:error_msg("pool '~s' failed to start member: ~p",
                                   [PoolName, Error]),
            {self(), Error}
    end.

send_accept_member(pool, PoolName, Msg) ->
    pooler:accept_member(PoolName, Msg);
send_accept_member(Pid, _PoolName, Msg) ->
    Pid ! {accept_member, Msg},
    ok.

accept_member_async(Pid) ->
    gen_server:cast(Pid, accept_member).
