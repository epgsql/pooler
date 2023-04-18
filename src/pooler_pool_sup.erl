-module(pooler_pool_sup).

-behaviour(supervisor).

-export([
    start_link/1,
    init/1,
    pool_sup_name/1,
    member_sup_name/1,
    build_member_sup_name/1
]).

-spec start_link(pooler:pool_state()) -> {ok, pid()}.
start_link(Pool) ->
    SupName = pool_sup_name(Pool),
    supervisor:start_link({local, SupName}, ?MODULE, Pool).

init(Pool) ->
    PoolerSpec = {pooler, {pooler, start_link, [Pool]}, transient, 5000, worker, [pooler]},
    MemberSupName = member_sup_name(Pool),
    MemberSupSpec =
        {MemberSupName, {pooler_pooled_worker_sup, start_link, [Pool]}, transient, 5000, supervisor, [
            pooler_pooled_worker_sup
        ]},

    %% five restarts in 60 seconds, then shutdown
    Restart = {one_for_all, 5, 60},
    {ok, {Restart, [MemberSupSpec, PoolerSpec]}}.

member_sup_name(Pool) ->
    build_member_sup_name(pooler_config:get_name(Pool)).

build_member_sup_name(PoolName) ->
    list_to_atom("pooler_" ++ atom_to_list(PoolName) ++ "_member_sup").

pool_sup_name(Pool) ->
    list_to_atom("pooler_" ++ atom_to_list(pooler_config:get_name(Pool)) ++ "_pool_sup").
