%% @author Slava Yurin <v.yurin@office.ngs.ru>
%% @doc Fake supervisor that not under pooler_sup
-module(fake_external_supervisor).

-behaviour(supervisor).

-export([init/1]).

init(Pool) ->
	{ok, {{one_for_one, 1, 1}, [pooler:pool_child_spec(Pool)]}}.
