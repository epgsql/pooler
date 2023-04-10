-module(bench_take_return).

-export([
    size_5_take_return_one/1,
    bench_size_5_take_return_one/2,
    size_1000_take_return_one/1,
    bench_size_1000_take_return_one/2,
    size_1_empty_take_no_members/1,
    bench_size_1_empty_take_no_members/2,
    size_5_take_return_all/1,
    bench_size_5_take_return_all/2,
    size_500_take_return_all/1,
    bench_size_500_take_return_all/2,
    size_500_clients_50_take_return_all/1,
    bench_size_500_clients_50_take_return_all/2,
    size_0_max_500_take_return_all/1,
    bench_size_0_max_500_take_return_all/2
]).

%% @doc Pool of fixed size 5 - try to take just one member and instantly return
size_5_take_return_one(init) ->
    % init is called only once in the same process where benchmark will be running
    % at the very beginning of benchmark.
    % Value returned from this callback will be passed to server({input, State}),
    % bench_server(_, State) and server({stop, State})
    start_fixed(?FUNCTION_NAME, 5),
    ?FUNCTION_NAME;
size_5_take_return_one({input, _Server}) ->
    % This callback is called after `init` to generate benchmark input data.
    % Returned value will be passed to bench_server(Input, _)
    [];
size_5_take_return_one({stop, PoolName}) ->
    % Called only once at the very end of benchmark
    stop(PoolName).

bench_size_5_take_return_one(_Input, PoolName) ->
    Member = pooler:take_member(PoolName),
    true = is_pid(Member),
    pooler:return_member(PoolName, Member).

%% @doc Pool of fixed size 1000 - try to take just one member and instantly return
size_1000_take_return_one(init) ->
    start_fixed(?FUNCTION_NAME, 1000),
    ?FUNCTION_NAME;
size_1000_take_return_one({input, _Server}) ->
    % This callback is called after `init` to generate benchmark input data.
    % Returned value will be passed to bench_server(Input, _)
    [];
size_1000_take_return_one({stop, PoolName}) ->
    % Called only once at the very end of benchmark
    stop(PoolName).

bench_size_1000_take_return_one(_Input, PoolName) ->
    Member = pooler:take_member(PoolName),
    true = is_pid(Member),
    pooler:return_member(PoolName, Member).

%% @doc Fully satrurated pool of fixed size 1 - try to take just one member, get `error_no_members'
size_1_empty_take_no_members(init) ->
    start_fixed(?FUNCTION_NAME, 1),
    Worker = pooler:take_member(?FUNCTION_NAME),
    {?FUNCTION_NAME, Worker};
size_1_empty_take_no_members({input, _}) ->
    [];
size_1_empty_take_no_members({stop, {PoolName, Worker}}) ->
    pooler:return_member(PoolName, Worker),
    stop(PoolName).

bench_size_1_empty_take_no_members(_Input, {PoolName, _}) ->
    error_no_members = pooler:take_member(PoolName).

%% @doc Pool of fixed size 5 - take all members sequentially and instantly return them
size_5_take_return_all(init) ->
    start_fixed(?FUNCTION_NAME, 5),
    {?FUNCTION_NAME, 5};
size_5_take_return_all({input, {_Pool, _Size}}) ->
    [];
size_5_take_return_all({stop, {PoolName, _}}) ->
    stop(PoolName).

bench_size_5_take_return_all(_Input, {PoolName, Size}) ->
    Members = take_n(PoolName, Size),
    [pooler:return_member(PoolName, Member) || Member <- Members].

%% @doc Pool of fixed size 500 - take all members and instantly return them
size_500_take_return_all(init) ->
    start_fixed(?FUNCTION_NAME, 500),
    {?FUNCTION_NAME, 500};
size_500_take_return_all({input, {_Pool, Size}}) ->
    lists:seq(1, Size);
size_500_take_return_all({stop, {PoolName, _}}) ->
    stop(PoolName).

bench_size_500_take_return_all(_Input, {PoolName, Size}) ->
    Members = take_n(PoolName, Size),
    [pooler:return_member(PoolName, Member) || Member <- Members].

%% @doc Pool of fixed size 500 - take all members from 50 workers and let workers instantly return them
size_500_clients_50_take_return_all(init) ->
    PoolSize = 500,
    NumClients = 50,
    PerClient = PoolSize div NumClients,
    start_fixed(?FUNCTION_NAME, 500),
    Clients = [
        erlang:spawn(
            fun() ->
                client(?FUNCTION_NAME, PerClient)
            end
        )
     || _ <- lists:seq(1, NumClients)
    ],
    {?FUNCTION_NAME, 500, Clients};
size_500_clients_50_take_return_all({input, {_Pool, _Size, _Clients}}) ->
    [];
size_500_clients_50_take_return_all({stop, {PoolName, _Size, Clients}}) ->
    [exit(Pid, shutdown) || Pid <- Clients],
    stop(PoolName).

bench_size_500_clients_50_take_return_all(_Input, {_PoolName, _Size, Clients}) ->
    Ref = erlang:make_ref(),
    Self = self(),
    lists:foreach(fun(C) -> C ! {do, Self, Ref} end, Clients),
    lists:foreach(
        fun(_C) ->
            receive
                {done, RecRef} ->
                    RecRef = Ref
            after 5000 ->
                error(timeout)
            end
        end,
        Clients
    ).

%% @doc Artificial example: pool with init_count=500, max_count=500 that is culled to 0 on each iteration.
%% Try to take 500 workers sequentially and instantly return them (and trigger culling).
%% This benchmark, while is quite unrealistic (why have pool that instantly kills workers), but it
%% helps to test worker spawn/kill routines.
size_0_max_500_take_return_all(init) ->
    Size = 500,
    start([
        {name, ?FUNCTION_NAME},
        {init_count, 0},
        {max_count, Size},
        {max_age, {0, sec}}
    ]),
    {?FUNCTION_NAME, Size};
size_0_max_500_take_return_all({input, {_Pool, _Size}}) ->
    [];
size_0_max_500_take_return_all({stop, {PoolName, _}}) ->
    stop(PoolName).

bench_size_0_max_500_take_return_all(_Input, {PoolName, Size}) ->
    Members = take_n(PoolName, 500, Size),
    [pooler:return_member(PoolName, Member) || Member <- Members],
    whereis(PoolName) ! cull_pool,
    Utilization = pooler:pool_utilization(PoolName),
    0 = proplists:get_value(free_count, Utilization),
    0 = proplists:get_value(in_use_count, Utilization).

%% Internal

start_fixed(Name, Size) ->
    Conf = [
        {name, Name},
        {init_count, Size},
        {max_count, Size}
    ],
    start(Conf).

start(Conf0) ->
    Conf = [{start_mfa, {pooled_gs, start_link, [{"test"}]}} | Conf0],
    logger:set_handler_config(default, filters, []),
    {ok, _} = application:ensure_all_started(pooler),
    {ok, _} = pooler:new_pool(Conf).

stop(Name) ->
    pooler:rm_pool(Name),
    application:stop(pooler).

take_n(PoolName, N) ->
    take_n(PoolName, 0, N).

take_n(_, _, 0) ->
    [];
take_n(PoolName, Timeout, N) ->
    Member = pooler:take_member(PoolName, Timeout),
    true = is_pid(Member),
    [Member | take_n(PoolName, Timeout, N - 1)].

client(Pool, N) ->
    receive
        {do, From, Ref} ->
            Taken = take_n(Pool, N),
            [pooler:return_member(Pool, Member) || Member <- Taken],
            From ! {done, Ref}
    end,
    client(Pool, N).
