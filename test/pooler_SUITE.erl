%% @author Slava Yurin <YurinVV@ya.ru>
-module(pooler_SUITE).

%% ct.
-export([all/0]).

%% Tests
-export([eunit/1]).

%- Test suite -----------------------------------------------------------------%
all() ->
	[eunit].

%- Tests ----------------------------------------------------------------------%
eunit(_Config) ->
	ok = eunit:test([{module, pooler_perf_test}, {application, pooler}]).

%- Helper function ------------------------------------------------------------%
