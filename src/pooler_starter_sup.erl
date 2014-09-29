%% @doc Simple one for one supervisor for pooler_starter.
%%
%% This supervisor is shared by all pools since pooler_starter is a
%% generic helper to fasciliate async member start.
-module(pooler_starter_sup).

-behaviour(supervisor).

-export([new_starter/2,
         start_link/0,
         init/1]).

-include("pooler.hrl").

new_starter(Pool, Parent) ->
    supervisor:start_child(?MODULE, [Pool, Parent]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Worker = {pooler_starter, {pooler_starter, start_link, []},
              temporary, brutal_kill, worker, [pooler_starter]},
    Specs = [Worker],
    Restart = {simple_one_for_one, 1, 1},
    {ok, {Restart, Specs}}.
