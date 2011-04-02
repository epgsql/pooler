-module(pidq_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Config = application:get_all_env(),
    Pidq = {pidq, {pidq, start_link, [Config]},
            permanent, 5000, worker, [pidq]},
    PidqPool = {pidq_pool_sup, {pidq_pool_sup, start_link, []},
                permanent, 5000, supervisor, [pidq_pool_sup]},
    {ok, {{one_for_one, 5, 10}, [PidqPool, Pidq]}}.

