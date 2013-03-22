-module(pooler_pooled_worker_sup).

-behaviour(supervisor).

-export([start_link/1, init/1]).

-include("pooler.hrl").

start_link(#pool{start_mfa = {_, _, _} = MFA} = Pool) ->
    SupName = pooler_pool_sup:member_sup_name(Pool),
    supervisor:start_link({local, SupName}, ?MODULE, MFA).

init({Mod, Fun, Args}) ->
    Worker = {Mod, {Mod, Fun, Args}, temporary, brutal_kill, worker, [Mod]},
    Specs = [Worker],
    Restart = {simple_one_for_one, 1, 1},
    {ok, {Restart, Specs}}.
