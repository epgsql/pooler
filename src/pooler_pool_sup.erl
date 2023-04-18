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

init(PoolConf) ->
    PoolerSpec = {pooler, {pooler, start_link, [PoolConf]}, transient, 5000, worker, [pooler]},
    MemberSupName = member_sup_name(PoolConf),
    MemberSupSpec =
        {MemberSupName, {pooler_pooled_worker_sup, start_link, [PoolConf]}, transient, 5000, supervisor, [
            pooler_pooled_worker_sup
        ]},

    %% five restarts in 60 seconds, then shutdown
    Restart = {one_for_all, 5, 60},
    {ok, {Restart, [MemberSupSpec, PoolerSpec]}}.

-spec member_sup_name(pooler:pool_config()) -> atom().
member_sup_name(#{name := Name}) ->
    build_member_sup_name(Name).

-spec build_member_sup_name(pooler:pool_name()) -> atom().
build_member_sup_name(PoolName) ->
    list_to_atom("pooler_" ++ atom_to_list(PoolName) ++ "_member_sup").

pool_sup_name(#{name := Name}) ->
    list_to_atom("pooler_" ++ atom_to_list(Name) ++ "_pool_sup").
