-module(pooler_pooled_worker_sup).

-behaviour(supervisor).

-export([start_link/1, init/1]).

-spec start_link(pooler:pool_config()) -> {ok, pid()} | {error, any()}.
start_link(#{start_mfa := MFA} = PoolConf) ->
    SupName = pooler_pool_sup:member_sup_name(PoolConf),
    supervisor:start_link({local, SupName}, ?MODULE, MFA).

init({Mod, Fun, Args}) ->
    Worker = {Mod, {Mod, Fun, Args}, temporary, brutal_kill, worker, [Mod]},
    Specs = [Worker],
    Restart = {simple_one_for_one, 1, 1},
    {ok, {Restart, Specs}}.
