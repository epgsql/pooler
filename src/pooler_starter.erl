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

-export([start_link/3,
         start_member/1,
         start_member/2,
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

start_link(Pool, Ref, Parent) ->
    gen_server:start_link(?MODULE, {Pool, Ref, Parent}, []).

stop(Starter) ->
    gen_server:call(Starter, stop).

%% @doc Start a member for the specified `Pool'.
%%
%% Member creation with this call is async. This function returns
%% immediately with a reference. When the member has been created it
%% is sent to the specified pool via {@link pooler:accept_member/2}.
%%
%% Each call starts a single use `pooler_starter' instance via
%% `pooler_starter_sup'. The instance terminates normally after
%% creating a single member.
-spec start_member(#pool{}) -> reference().
start_member(Pool) ->
    Ref = make_ref(),
    {ok, _Pid} = pooler_starter_sup:new_starter(Pool, Ref, pool),
    Ref.

%% @doc Same as {@link start_member/1} except that instead of calling
%% {@link pooler:accept_member/2} a raw message is sent to `Parent' of
%% the form `{accept_member, {Ref, Member}'. Where `Member' will
%% either be the member pid or an error term and `Ref' will be the
%% reference returned from this function.
%%
%% This is used by the init function in the `pooler' to start the
%% initial set of pool members in parallel.
start_member(Pool, Parent) ->
    Ref = make_ref(),
    {ok, _Pid} = pooler_starter_sup:new_starter(Pool, Ref, Parent),
    Ref.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
-record(starter, {pool,
                  ref,
                  parent}).

-spec init({#pool{}, reference(), pid() | atom()}) -> {'ok', #starter{}, 0}.
init({Pool, Ref, Parent}) ->
    %% trigger immediate timeout message, which we'll use to trigger
    %% the member start.
    {ok, #starter{pool = Pool, ref = Ref, parent = Parent}, 0}.

handle_call(stop, _From, State) ->
    {stop, normal, stop_ok, State};
handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(_, _) -> {'noreply', _}.
handle_info(timeout,
            #starter{pool = Pool, ref = Ref, parent = Parent} = State) ->
    ok = do_start_member(Pool, Ref, Parent),
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(_, _) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, _, _) -> {'ok', _}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_start_member(#pool{name = PoolName, member_sup = PoolSup}, Ref, Parent) ->
    Msg = case supervisor:start_child(PoolSup, []) of
              {ok, Pid} ->
                  {Ref, Pid};
              Error ->
                  error_logger:error_msg("pool '~s' failed to start member: ~p",
                                         [PoolName, Error]),
                  {Ref, Error}
          end,
    send_accept_member(Parent, PoolName, Msg),
    ok.

send_accept_member(pool, PoolName, Msg) ->
    pooler:accept_member(PoolName, Msg);
send_accept_member(Pid, _PoolName, Msg) ->
    Pid ! {accept_member, Msg},
    ok.
