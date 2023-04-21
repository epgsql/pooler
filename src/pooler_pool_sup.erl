-module(pooler_pool_sup).

-behaviour(supervisor).

-export([
    start_link/1,
    init/1,
    pool_sup_name/1,
    member_sup_name/1,
    build_member_sup_name/1
]).

-spec start_link(pooler:pool_config()) -> {ok, pid()}.
start_link(PoolConf) ->
    SupName = pool_sup_name(PoolConf),
    supervisor:start_link({local, SupName}, ?MODULE, PoolConf).

init(PoolConf) when is_map(PoolConf) ->
    PoolerSpec = #{
        id => pooler,
        start => {pooler, start_link, [PoolConf]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [pooler]
    },
    MemberSupName = member_sup_name(PoolConf),
    MemberSupSpec =
        #{
            id => MemberSupName,
            start => {pooler_pooled_worker_sup, start_link, [PoolConf]},
            restart => transient,
            shutdown => 5000,
            type => supervisor,
            modules => [pooler_pooled_worker_sup]
        },

    %% five restarts in 60 seconds, then shutdown
    Restart = #{strategy => one_for_all, intensity => 5, period => 60},
    {ok, {Restart, [MemberSupSpec, PoolerSpec]}};
init(PoolRecord) when is_tuple(PoolRecord), element(1, PoolRecord) =:= pool ->
    %% This clause is for the hot code upgrade from pre-1.6.0;
    %% can be removed when "upgrade-from-version" below 1.6.0 are removed from `pooler.appup.src'
    {ok, PoolRecord1} = pooler:code_change(0, PoolRecord, []),
    AsMap = pooler:to_map(PoolRecord1),
    init(
        maps:with(
            [
                name,
                init_count,
                max_count,
                start_mfa,
                group,
                cull_interval,
                max_age,
                member_start_timeout,
                queue_max,
                metrics_api,
                metrics_mod,
                stop_mfa,
                auto_grow_threshold,
                add_member_retry,
                metrics_mod,
                metrics_api
            ],
            AsMap
        )
    ).

-spec member_sup_name(pooler:pool_config()) -> atom().
member_sup_name(#{name := Name}) ->
    build_member_sup_name(Name).

-spec build_member_sup_name(pooler:pool_name()) -> atom().
build_member_sup_name(PoolName) ->
    list_to_atom("pooler_" ++ atom_to_list(PoolName) ++ "_member_sup").

pool_sup_name(#{name := Name}) ->
    list_to_atom("pooler_" ++ atom_to_list(Name) ++ "_pool_sup").
