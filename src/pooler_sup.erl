-module(pooler_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

-include("pooler.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% a list of pool configs
    {ok, Config} = application:get_env(pooler, pools),
    Pools = [ pooler_config:list_to_pool(L) || L <- Config ],
    PoolSupSpecs = [ pool_sup_spec(Pool) || Pool <- Pools ],
    {ok, {{one_for_one, 5, 60}, PoolSupSpecs}}.

pool_sup_spec(#pool{name = Name} = Pool) ->
    SupName = pool_sup_name(Name),
    {SupName, {pooler_pool_sup, start_link, [Pool]},
     transient, 5000, supervisor, [pooler_pool_sup]}.

pool_sup_name(Name) ->
    list_to_atom("pooler_" ++ atom_to_list(Name) ++ "_pool_sup").


