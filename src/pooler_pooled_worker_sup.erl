-module(pooler_pooled_worker_sup).

-behaviour(supervisor).

-export([start_link/1, init/1]).

-spec start_link(pooler:pool_config()) -> {ok, pid()} | {error, any()}.
start_link(#{start_mfa := MFA} = PoolConf) ->
    SupName = pooler_pool_sup:member_sup_name(PoolConf),
    supervisor:start_link({local, SupName}, ?MODULE, MFA).

init({Mod, Fun, Args}) ->
    Worker = #{
        id => Mod,
        start => {Mod, Fun, Args},
        restart => temporary,
        shutdown => brutal_kill,
        type => worker,
        modules => [Mod]
    },
    Specs = [Worker],
    Restart = #{strategy => simple_one_for_one, intensity => 1, period => 1},
    {ok, {Restart, Specs}}.
