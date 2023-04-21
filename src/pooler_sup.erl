-module(pooler_sup).

-behaviour(supervisor).

-export([
    init/1,
    new_pool/1,
    rm_pool/1,
    pool_child_spec/1,
    start_link/0
]).

-include_lib("kernel/include/logger.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% a list of pool configs
    Configs =
        case application:get_env(pooler, pools) of
            {ok, C} ->
                C;
            undefined ->
                []
        end,
    {MetricsApi, MetricsMod} = metrics_module(),
    PoolSupSpecs = [
        pool_sup_spec((pooler:config_as_map(Config))#{
            metrics_mod => MetricsMod,
            metrics_api => MetricsApi
        })
     || Config <- Configs
    ],
    try
        pooler:create_group_table()
    catch
        error:badarg:Stack ->
            ?LOG_ERROR(
                #{
                    label => "Failed to start pool groups ETS table",
                    reason => badarg,
                    stack => Stack
                },
                #{domain => [pooler]}
            )
    end,
    {ok, {#{strategy => one_for_one, intensity => 5, period => 60}, [starter_sup_spec() | PoolSupSpecs]}}.

%% @doc Create a new pool from proplist pool config `PoolConfig'. The
%% public API for this functionality is {@link pooler:new_pool/1}.
new_pool(PoolConfig) ->
    Spec = pool_child_spec(PoolConfig),
    supervisor:start_child(?MODULE, Spec).

%% @doc Create a child spec for new pool from proplist pool config `PoolConfig'. The
%% public API for this functionality is {@link pooler:pool_child_spec/1}.
pool_child_spec(PoolConfig) ->
    {MetricsApi, MetricsMod} = metrics_module(),
    pool_sup_spec(PoolConfig#{
        metrics_mod => MetricsMod,
        metrics_api => MetricsApi
    }).

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
    #{
        id => pooler_starter_sup,
        start => {pooler_starter_sup, start_link, []},
        restart => transient,
        shutdown => 5000,
        type => supervisor,
        modules => [pooler_starter_sup]
    }.

pool_sup_spec(#{name := Name} = PoolConfig) ->
    SupName = pool_sup_name(Name),
    #{
        id => SupName,
        start => {pooler_pool_sup, start_link, [PoolConfig]},
        restart => transient,
        shutdown => 5000,
        type => supervisor,
        modules => [pooler_pool_sup]
    }.

pool_sup_name(Name) ->
    list_to_atom("pooler_" ++ atom_to_list(Name) ++ "_pool_sup").

metrics_module() ->
    case application:get_env(pooler, metrics_module) of
        {ok, Mod} ->
            case application:get_env(pooler, metrics_api) of
                {ok, exometer} ->
                    {exometer, Mod};
                % folsom is the default
                _V ->
                    {folsom, Mod}
            end;
        undefined ->
            {folsom, pooler_no_metrics}
    end.
