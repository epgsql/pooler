-module(pooler_pool_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Worker = {pooler_pooled_worker_sup,
              {pooler_pooled_worker_sup, start_link, []},
              temporary, 5000, supervisor, [pooler_pooled_worker_sup]},
    Restart = {simple_one_for_one, 1, 1},
    {ok, {Restart, [Worker]}}.
