-module(pooler_sup).

-behaviour(supervisor).

-export([start_link/1, init/1]).

start_link(PoolConfig) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [PoolConfig]).

init([PoolConfig]) ->
    Pooler = {pooler, {pooler, start_link, [PoolConfig]},
            permanent, 5000, worker, [pooler]},
    PoolerPool = {pooler_pool_sup, {pooler_pool_sup, start_link, []},
                permanent, 5000, supervisor, [pooler_pool_sup]},
    {ok, {{one_for_one, 5, 10}, [PoolerPool, Pooler]}}.

