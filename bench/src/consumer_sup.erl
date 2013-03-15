-module(consumer_sup).

-behaviour(supervisor).

-export([
         init/1,
         new_consumer/0,
         start_link/0
        ]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(Args) ->
    Worker = {consumer, {consumer, start_link, Args},
              temporary,                        % never restart workers
              brutal_kill, worker, [consumer]},
    Specs = [Worker],
    Restart = {simple_one_for_one, 1, 1},
    {ok, {Restart, Specs}}.

new_consumer() ->
    supervisor:start_child(?MODULE, []).
