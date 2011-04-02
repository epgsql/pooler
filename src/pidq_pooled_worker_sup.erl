-module(pidq_pooled_worker_sup).

-behaviour(supervisor).

-export([start_link/1, init/1]).

start_link(Config) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Config).

init({Mod, Fun, Args}) ->
    error_logger:info_report(in_pidq_pooled_worker_sup),
    Worker = {Mod, {Mod, Fun, Args}, temporary, brutal_kill, worker, [Mod]},
    Specs = [Worker],
    Restart = {simple_one_for_one, 1, 1},
    {ok, {Restart, Specs}}.
