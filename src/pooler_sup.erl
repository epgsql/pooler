-module(pooler_sup).

-behaviour(supervisor).

-export([init/1,
         new_pool/1,
         rm_pool/1,
         pool_child_spec/1,
         start_link/0]).

-include("pooler.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% a list of pool configs
    Config = case application:get_env(pooler, pools) of
                 {ok, C} ->
                     C;
                 undefined ->
                     []
             end,
    MetricsConfig = {metrics_mod, metrics_module()},
    Pools = [ pooler_config:list_to_pool([MetricsConfig | L]) || L <- Config ],
    PoolSupSpecs = [ pool_sup_spec(Pool) || Pool <- Pools ],
    ets:new(?POOLER_GROUP_TABLE, [set, public, named_table, {write_concurrency, true}]),
    {ok, {{one_for_one, 5, 60}, [starter_sup_spec() | PoolSupSpecs]}}.

%% @doc Create a new pool from proplist pool config `PoolConfig'. The
%% public API for this functionality is {@link pooler:new_pool/1}.
new_pool(PoolConfig) ->
    Spec = pool_child_spec(PoolConfig),
    supervisor:start_child(?MODULE, Spec).

%% @doc Create a child spec for new pool from proplist pool config `PoolConfig'. The
%% public API for this functionality is {@link pooler:pool_child_spec/1}.
pool_child_spec(PoolConfig) ->
    MetricsConfig = {metrics_mod, metrics_module()},
    NewPool = pooler_config:list_to_pool([MetricsConfig | PoolConfig]),
    pool_sup_spec(NewPool).

%% @doc Shutdown the named pool.
rm_pool(Name) ->
    SupName = pool_sup_name(Name),
    case supervisor:terminate_child(?MODULE, SupName) of
        {error, not_found} ->
            ok;
        ok ->
            supervisor:delete_child(?MODULE, SupName);
        Error ->
            Error
    end.

starter_sup_spec() ->
    {pooler_starter_sup, {pooler_starter_sup, start_link, []},
     transient, 5000, supervisor, [pooler_starter_sup]}.

pool_sup_spec(#pool{name = Name} = Pool) ->
    SupName = pool_sup_name(Name),
    {SupName, {pooler_pool_sup, start_link, [Pool]},
     transient, 5000, supervisor, [pooler_pool_sup]}.

pool_sup_name(Name) ->
    list_to_atom("pooler_" ++ atom_to_list(Name) ++ "_pool_sup").

metrics_module() ->
    case application:get_env(pooler, metrics_module) of
        {ok, Mod} ->
            Mod;
        undefined ->
            pooler_no_metrics
    end.
