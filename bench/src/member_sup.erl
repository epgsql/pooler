-module(member_sup).

-behaviour(supervisor).

-export([
         init/1,
         new_member/1,
         start_link/0
        ]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(Args) ->
    Worker = {member, {member, start_link, Args},
              temporary,                        % never restart workers
              brutal_kill, worker, [member]},
    Specs = [Worker],
    Restart = {simple_one_for_one, 1, 1},
    {ok, {Restart, Specs}}.

new_member(Delay) ->
    Config = [{start_up_delay, Delay}],
    supervisor:start_child(?MODULE, [Config]).
