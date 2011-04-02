-module(pidq_sup).

-behaviour(supervisor).

-export([start_link/1, init/1]).

start_link(Config) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Config]).

init(Config) ->
    % Pidq = {pidq, {pidq, start_link, [Config]},
    %         permanent, 5000, worker, [pidq]},
    PidqPool = {pidq_pool_sup, {pidq_pool_sup, start_link, []},
                permanent, 5000, supervisor, [pidq_pool_sup]},
    {ok, {{one_for_one, 5, 10}, [PidqPool]}}.

