-module(pidq_pool_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    error_logger:info_report(in_pidq_pool_sup),
    Worker = {pidq_pooled_worker_sup,
              {pidq_pooled_worker_sup, start_link, []},
              temporary, 5000, supervisor, [pidq_pooled_worker_sup]},
    Restart = {simple_one_for_one, 1, 1},
    {ok, {Restart, [Worker]}}.
