%% @author Seth Falcon <seth@userprimary.net>
%% @copyright 2012-2013 Seth Falcon
%% @doc Helper gen_server to start pool members
%%
-module(pooler_starter).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    start_link/1,
    start_member/2,
    start_member/3,
    start_member/4,
    stop_member_async/1,
    stop/1
]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export_type([start_spec/0, start_result/0]).

-type pool_member_sup() :: pid() | atom().
-type parent() :: pid() | pool.
-type initialize_mfa() :: undefined | {module(), atom(), [term()]}.
-type start_result() :: {StarterPid :: pid(), Result :: pid() | {error, _}}.
-opaque start_spec() :: {pooler:pool_name(), pool_member_sup(), parent(), initialize_mfa()}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

-spec start_link(start_spec()) -> {ok, pid()}.
start_link({_, _, _, _} = Spec) ->
    gen_server:start_link(?MODULE, Spec, []).

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
-spec start_member(pooler:pool_name(), pool_member_sup()) -> pid().
start_member(PoolName, PoolMemberSup) ->
    start_member(PoolName, PoolMemberSup, undefined).

-spec start_member(pooler:pool_name(), pool_member_sup(), initialize_mfa()) -> pid().
start_member(PoolName, PoolMemberSup, InitMFA) ->
    {ok, Pid} = pooler_starter_sup:new_starter({PoolName, PoolMemberSup, pool, InitMFA}),
    Pid.

%% @doc Same as {@link start_member/2} except that instead of calling
%% {@link pooler:accept_member/2} a raw message is sent to `Parent' of
%% the form `{accept_member, {Ref, Member}'. Where `Member' will
%% either be the member pid or an error term and `Ref' will be the
%% Pid of the starter.
%%
%% This is used by the init function in the `pooler' to start the
%% initial set of pool members in parallel.
-spec start_member(pooler:pool_name(), pool_member_sup(), pid(), initialize_mfa()) -> pid().
start_member(PoolName, PoolMemberSup, Parent, InitMFA) ->
    {ok, Pid} = pooler_starter_sup:new_starter({PoolName, PoolMemberSup, Parent, InitMFA}),
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
-record(starter, {
    parent :: parent(),
    pool_name :: pooler:pool_name(),
    pool_member_sup :: pool_member_sup(),
    initialize_mfa :: initialize_mfa(),
    msg :: start_result() | undefined
}).

-spec init(start_spec()) -> {ok, #starter{}, {continue, start}}.
init({PoolName, PoolMemberSup, Parent, InitMFA}) ->
    {ok, #starter{pool_name = PoolName, pool_member_sup = PoolMemberSup, parent = Parent, initialize_mfa = InitMFA},
        {continue, start}}.

handle_continue(
    start,
    #starter{pool_member_sup = PoolSup, pool_name = PoolName, initialize_mfa = InitMFA} = State
) ->
    Msg = do_start_member(PoolSup, PoolName, InitMFA),
    % asynchronously in order to receive potential `stop*'
    accept_member_async(self()),
    {noreply, State#starter{msg = Msg}}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(stop_member, #starter{msg = {_Me, Pid}, pool_member_sup = MemberSup} = State) when is_pid(Pid) ->
    %% The process we were starting is no longer valid for the pool.
    %% Cleanup the process and stop normally.
    supervisor:terminate_child(MemberSup, Pid),
    {stop, normal, State};
handle_cast(stop_member, State) ->
    %% Either init failed (msg is an error) or start_member hasn't completed yet.
    {stop, normal, State};
handle_cast(accept_member, #starter{msg = Msg, parent = Parent, pool_name = PoolName} = State) ->
    %% Process creation has succeeded. Send the member to the pooler
    %% gen_server to be accepted. Pooler gen_server will notify
    %% us if the member was accepted or needs to cleaned up.
    send_accept_member(Parent, PoolName, Msg),
    {noreply, State};
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(_, _) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, _, _) -> {'ok', _}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_start_member(PoolSup, PoolName, InitMFA) ->
    case supervisor:start_child(PoolSup, []) of
        {ok, Pid} ->
            case call_initialize_mfa(Pid, InitMFA) of
                ok ->
                    {self(), Pid};
                Error ->
                    supervisor:terminate_child(PoolSup, Pid),
                    {self(), Error}
            end;
        Error ->
            ?LOG_ERROR(
                #{
                    label => "failed to start member",
                    pool => PoolName,
                    error => Error
                },
                #{domain => [pooler]}
            ),
            {self(), Error}
    end.

-spec call_initialize_mfa(pid(), initialize_mfa()) -> ok | {error, term()}.
call_initialize_mfa(_Pid, undefined) ->
    ok;
call_initialize_mfa(Pid, {Mod, Fun, Args}) ->
    NewArgs = [
        case A of
            '$pooler_pid' -> Pid;
            _ -> A
        end
     || A <- Args
    ],
    case catch erlang:apply(Mod, Fun, NewArgs) of
        ok -> ok;
        {error, _} = Err -> Err;
        {'EXIT', Reason} -> {error, {initialize_mfa_exit, Reason}};
        Other -> {error, {unexpected_initialize_mfa_return, Other}}
    end.

-spec send_accept_member(parent(), pooler:pool_name(), start_result()) -> ok.
send_accept_member(pool, PoolName, Msg) ->
    %% used to grow pool
    pooler:accept_member(PoolName, Msg);
send_accept_member(Pid, _PoolName, Msg) ->
    %% used during pool initialization
    Pid ! {accept_member, Msg},
    ok.

accept_member_async(Pid) ->
    gen_server:cast(Pid, accept_member).
