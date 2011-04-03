-module(pooler_pooled_worker_sup).

-behaviour(supervisor).

-export([start_link/1, init/1]).

start_link(Config) ->
    supervisor:start_link(?MODULE, Config).

init({Mod, Fun, Args}) ->
    Worker = {Mod, {Mod, Fun, Args}, temporary, brutal_kill, worker, [Mod]},
    Specs = [Worker],
    Restart = {simple_one_for_one, 1, 1},
    {ok, {Restart, Specs}}.
