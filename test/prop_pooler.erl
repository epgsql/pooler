-module(prop_pooler).

-export([
    prop_fixed_start/1,
    prop_fixed_checkout_all/1,
    prop_dynamic_checkout/1,
    prop_fixed_take_return/1,
    prop_fixed_take_return_broken/1,
    prop_fixed_client_died/1,
    prop_group_take_return/1
]).

-include_lib("proper/include/proper.hrl").
-include_lib("stdlib/include/assert.hrl").

prop_fixed_start(doc) ->
    "Check that the pool of any fixed size can be started, internal statistics is correct".

prop_fixed_start() ->
    Conf0 = [
        {name, ?FUNCTION_NAME},
        {start_mfa, {pooled_gs, start_link, [{?FUNCTION_NAME}]}}
    ],
    ?FORALL(
        Size,
        pos_integer(),
        with_pool(
            [
                {init_count, Size},
                {max_count, Size}
                | Conf0
            ],
            fun() ->
                %% Pool is not utilized
                pool_is_free(?FUNCTION_NAME, Size),
                true
            end
        )
    ).

prop_fixed_checkout_all(doc) ->
    "Can take all members from fixed-size pool. Following attempts will return error. Stats is correct.".

prop_fixed_checkout_all() ->
    Conf0 = [
        {name, ?FUNCTION_NAME},
        {start_mfa, {pooled_gs, start_link, [{?FUNCTION_NAME}]}}
    ],
    ?FORALL(
        Size,
        pos_integer(),
        with_pool(
            [
                {init_count, Size},
                {max_count, Size}
                | Conf0
            ],
            fun() ->
                ?assert(
                    lists:all(
                        fun(Res) -> is_pid(Res) end,
                        take_n(?FUNCTION_NAME, 0, Size)
                    )
                ),
                %% Fixed pool - can't take more members than pool size
                ?assertEqual(error_no_members, pooler:take_member(?FUNCTION_NAME, 10)),
                %% Pool is fully utilized
                pool_is_utilized(?FUNCTION_NAME, self(), Size),
                true
            end
        )
    ).

prop_dynamic_checkout(doc) ->
    "It's possible to take all fixed and then all dynamic members, but no more than max_count; stats is correct".

prop_dynamic_checkout() ->
    Conf0 = [
        {name, ?FUNCTION_NAME},
        {max_age, {1, min}},
        {start_mfa, {pooled_gs, start_link, [{?FUNCTION_NAME}]}}
    ],
    ?FORALL(
        {Size, Extra},
        {pos_integer(), pos_integer()},
        with_pool(
            [
                {init_count, Size},
                {max_count, Size + Extra}
                | Conf0
            ],
            fun() ->
                MaxCount = Size + Extra,
                ?assert(
                    lists:all(
                        fun(Res) -> is_pid(Res) end,
                        take_n(?FUNCTION_NAME, 0, Size)
                    )
                ),
                %% Fixed pool is fully utilized up to init_count
                pool_is_utilized(?FUNCTION_NAME, self(), Size),
                %% Take all dynamic workers
                ?assert(
                    lists:all(
                        fun(Res) -> is_pid(Res) end,
                        take_n(?FUNCTION_NAME, 1000, Extra)
                    )
                ),
                %% Pool is fully utilized now
                ?assertEqual(error_no_members, pooler:take_member(?FUNCTION_NAME, 10)),
                %% Dynamic pool is fully utilized up to max_count
                pool_is_utilized(?FUNCTION_NAME, self(), MaxCount),
                true
            end
        )
    ).

prop_fixed_take_return(doc) ->
    "The state of the pool is same before all members are taken and after they are returned".

prop_fixed_take_return() ->
    Conf0 = [
        {name, ?FUNCTION_NAME},
        {start_mfa, {pooled_gs, start_link, [{?FUNCTION_NAME}]}}
    ],
    Stats = fun() ->
        lists:sort([{Pid, State} || {Pid, {_, State, _}} <- pooler:pool_stats(?FUNCTION_NAME)])
    end,
    ?FORALL(
        Size,
        pos_integer(),
        with_pool(
            [
                {init_count, Size},
                {max_count, Size}
                | Conf0
            ],
            fun() ->
                UtilizationBefore = utilization(?FUNCTION_NAME),
                StatsBefore = Stats(),
                Taken = take_n(?FUNCTION_NAME, 0, Size),
                ?assert(lists:all(fun(Res) -> is_pid(Res) end, Taken)),
                pool_is_utilized(?FUNCTION_NAME, self(), Size),
                [pooler:return_member(?FUNCTION_NAME, Pid) || Pid <- Taken],
                pool_is_free(?FUNCTION_NAME, Size),
                UtilizationAfter = utilization(?FUNCTION_NAME),
                StatsAfter = Stats(),
                ?assertEqual(UtilizationBefore, UtilizationAfter),
                ?assertEqual(StatsBefore, StatsAfter),
                true
            end
        )
    ).

prop_fixed_take_return_broken(doc) ->
    "Pool recovers to initial state when all members are returned with 'fail' flag, but workers are replaced".

prop_fixed_take_return_broken() ->
    Conf0 = [
        {name, ?FUNCTION_NAME},
        {start_mfa, {pooled_gs, start_link, [{?FUNCTION_NAME}]}}
    ],
    Stats = fun() ->
        lists:sort([{Pid, State} || {Pid, {_, State, _}} <- pooler:pool_stats(?FUNCTION_NAME)])
    end,
    ?FORALL(
        Size,
        pos_integer(),
        with_pool(
            [
                {init_count, Size},
                {max_count, Size}
                | Conf0
            ],
            fun() ->
                UtilizationBefore = utilization(?FUNCTION_NAME),
                StatsBefore = Stats(),
                Taken = take_n(?FUNCTION_NAME, 0, Size),
                ?assert(lists:all(fun(Res) -> is_pid(Res) end, Taken)),
                pool_is_utilized(?FUNCTION_NAME, self(), Size),
                [pooler:return_member(?FUNCTION_NAME, Pid, fail) || Pid <- Taken],
                %% Since failed workers are replaced asynchronously, we need to wait for pool to recover
                UtilizationAfter =
                    wait_for_utilization(
                        ?FUNCTION_NAME,
                        5000,
                        fun(#{free_count := Free, starting_count := Starting}) ->
                            Free =:= Size andalso Starting =:= 0
                        end
                    ),
                pool_is_free(?FUNCTION_NAME, Size),
                StatsAfter = Stats(),
                ?assertEqual(UtilizationBefore, UtilizationAfter),
                {PidsBefore, StatusBefore} = lists:unzip(StatsBefore),
                {PidsAfter, StatusAfter} = lists:unzip(StatsAfter),
                %% all workers have status `free` before and after
                ?assertEqual(StatusBefore, StatusAfter),
                %% however, all workers are new processes, none reused
                ?assertEqual([], ordsets:intersection(ordsets:from_list(PidsBefore), ordsets:from_list(PidsAfter))),
                true
            end
        )
    ).

prop_fixed_client_died(doc) ->
    "Pool recovers to initial state when client that have taken processes have died with reason 'normal'".

prop_fixed_client_died() ->
    Conf0 = [
        {name, ?FUNCTION_NAME},
        {start_mfa, {pooled_gs, start_link, [{?FUNCTION_NAME}]}}
    ],
    Stats = fun() ->
        lists:sort([{Pid, State} || {Pid, {_, State, _}} <- pooler:pool_stats(?FUNCTION_NAME)])
    end,
    ?FORALL(
        Size,
        pos_integer(),
        with_pool(
            [
                {init_count, Size},
                {max_count, Size}
                | Conf0
            ],
            fun() ->
                Main = self(),
                UtilizationBefore = utilization(?FUNCTION_NAME),
                StatsBefore = Stats(),
                {Pid, MRef} =
                    erlang:spawn_monitor(
                        fun() ->
                            Taken = take_n(?FUNCTION_NAME, 0, Size),
                            ?assert(lists:all(fun(Res) -> is_pid(Res) end, Taken)),
                            Main ! {taken, self()},
                            receive
                                {finish, Main} -> ok
                            after 5000 ->
                                exit(timeout)
                            end,
                            exit(normal)
                        end
                    ),
                %% Wait for spawned client to take all workers
                receive
                    {taken, Pid} -> ok
                after 5000 ->
                    error(timeout)
                end,
                pool_is_utilized(?FUNCTION_NAME, Pid, Size),
                %% Wait for the client to die
                Pid ! {finish, self()},
                receive
                    {'DOWN', MRef, process, Pid, normal} ->
                        ok
                after 5000 ->
                    error(timeout)
                end,
                %% Since worker monitors are asynchronous, we need to wait for pool to recover
                UtilizationAfter =
                    wait_for_utilization(
                        ?FUNCTION_NAME,
                        5000,
                        fun(#{free_count := Free, in_use_count := InUse}) ->
                            Free =:= Size andalso InUse =:= 0
                        end
                    ),
                pool_is_free(?FUNCTION_NAME, Size),
                StatsAfter = Stats(),
                ?assertEqual(UtilizationBefore, UtilizationAfter),
                ?assertEqual(StatsBefore, StatsAfter),
                true
            end
        )
    ).

prop_group_take_return(doc) ->
    "Take all workers from all group members - no more workers can be taken. Return them - pools are free.".

prop_group_take_return() ->
    Conf0 = [
        {start_mfa, {pooled_gs, start_link, [{?FUNCTION_NAME}]}}
    ],
    PoolName = fun(I) -> list_to_atom(atom_to_list(?FUNCTION_NAME) ++ integer_to_list(I)) end,
    ?FORALL(
        {NumWorkers, NumPools},
        {pos_integer(), pos_integer()},
        begin
            with_pools(
                [
                    [
                        {name, PoolName(I)},
                        {init_count, NumWorkers},
                        {max_count, NumWorkers},
                        {group, ?FUNCTION_NAME}
                        | Conf0
                    ]
                 || I <- lists:seq(1, NumPools)
                ],
                fun() ->
                    Client = self(),
                    %% Group registration is asynchronous, so, need to wait for it to happen
                    GroupPoolPids = wait_for_group_size(?FUNCTION_NAME, NumPools, 5000),
                    %% All pools are members of the group
                    ?assertEqual(NumPools, length(GroupPoolPids)),
                    %% It's possible to take all workers from all members of a group
                    Taken = group_take_n(?FUNCTION_NAME, NumWorkers * NumPools),
                    ?assert(lists:all(fun(Res) -> is_pid(Res) end, Taken)),
                    %% All pools are saturated
                    ?assertEqual(error_no_members, pooler:take_group_member(?FUNCTION_NAME)),
                    %% All pools are utilized
                    lists:foreach(
                        fun(Pool) -> pool_is_utilized(Pool, Client, NumWorkers) end,
                        GroupPoolPids
                    ),
                    %% Now return all the workers
                    [ok = pooler:return_group_member(?FUNCTION_NAME, Pid) || Pid <- Taken],
                    %% All pools are free
                    lists:foreach(
                        fun(Pool) -> pool_is_free(Pool, NumWorkers) end,
                        GroupPoolPids
                    ),
                    true
                end
            )
        end
    ).

%% Helpers

take_n(Pool, Timeout, N) when N > 0 ->
    [pooler:take_member(Pool, Timeout) | take_n(Pool, Timeout, N - 1)];
take_n(_Pool, _Timeout, 0) ->
    [].

group_take_n(Group, N) when N > 0 ->
    [pooler:take_group_member(Group) | group_take_n(Group, N - 1)];
group_take_n(_Group, 0) ->
    [].

with_pool(Conf, Fun) ->
    with_pools([Conf], Fun).

with_pools(Confs, Fun) ->
    pg_start(),
    %% Disable SASL logs
    logger:set_handler_config(default, filters, []),
    try
        {ok, _} = application:ensure_all_started(pooler),
        [{ok, _} = pooler:new_pool(Conf) || Conf <- Confs],
        Res = Fun(),
        [ok = pooler:rm_pool(proplists:get_value(name, Conf)) || Conf <- Confs],
        Res
    after
        application:stop(pooler)
    end.

wait_for_utilization(Pool, Timeout, Fun) when Timeout > 0 ->
    Utilization = utilization(Pool),
    case Fun(Utilization) of
        true ->
            Utilization;
        false ->
            timer:sleep(50),
            wait_for_utilization(Pool, Timeout - 50, Fun)
    end;
wait_for_utilization(_, _, _) ->
    error(timeout).

wait_for_group_size(GroupName, Size, Timeout) when Timeout > 0 ->
    Pools = pooler:group_pools(GroupName),
    case length(Pools) of
        Size ->
            Pools;
        Larger when Larger > Size ->
            error(group_size_exceeded);
        Smaller when Smaller < Size ->
            timer:sleep(50),
            wait_for_group_size(GroupName, Size, Timeout - 50)
    end;
wait_for_group_size(_, _, _) ->
    error(timeout).

utilization(Pool) ->
    maps:from_list(pooler:pool_utilization(Pool)).

pool_is_utilized(Pool, Client, NumWorkers) ->
    Utilization = utilization(Pool),
    ?assertMatch(
        #{
            in_use_count := NumWorkers,
            free_count := 0,
            queued_count := 0
        },
        Utilization
    ),
    %% All members are taken by Client
    ?assert(
        lists:all(
            fun({_, {_, State, _}}) -> State =:= Client end,
            pooler:pool_stats(Pool)
        )
    ),
    true.

pool_is_free(Pool, NumWorkers) ->
    Utilization = utilization(Pool),
    ?assertMatch(
        #{
            in_use_count := 0,
            free_count := NumWorkers,
            queued_count := 0
        },
        Utilization
    ),
    %% All members are free
    ?assert(
        lists:all(
            fun({_, {_, State, _}}) -> State =:= free end,
            pooler:pool_stats(Pool)
        )
    ),
    true.

-if(?OTP_RELEASE >= 23).
pg_start() ->
    pg:start(pg).
-else.
pg_start() ->
    pg2:start().
-endif.
