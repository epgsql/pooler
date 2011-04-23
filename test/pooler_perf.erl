-module(pooler_perf).

-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").

setup() ->
    MakePool = fun(I) ->
                       N = integer_to_list(I),
                       Name = "p" ++ N,
                       Arg0 = "pool-" ++ Name,
                       [{name, Name},
                        {max_count, 100},
                        {init_count, 10},
                        {start_mfa,
                         {pooled_gs, start_link, [{Arg0}]}}]
               end,
    Pools = [ MakePool(I) || I <- lists:seq(1, 5) ],
    application:set_env(pooler, pools, Pools),
    application:start(pooler).

consumer_cycle(N) ->
    consumer_cycle(N, 0, 0).

consumer_cycle(N, NumOk, NumFail) when N > 0 ->
    P = pooler:take_member(),
    case P of
        Pid when is_pid(Pid) ->
            true = is_process_alive(P),
            pooler:return_member(P, ok),
            consumer_cycle(N - 1, NumOk + 1, NumFail);
        _ ->
            consumer_cycle(N - 1, NumOk, NumFail + 1)
    end;
consumer_cycle(0, NumOk, NumFail) ->
    [{ok, NumOk}, {fail, NumFail}].

test1() ->
    N = 10000,
    {Time, _} = timer:tc(fun() ->
                                  consumer_cycle(N)
                          end, []),
    Time / N.

consumer_worker(N, Parent) ->
    spawn(fun() ->
                  Parent ! {self(), consumer_cycle(N)}
          end).

test3(W, N) ->
    [consumer_worker(W, self()) || _I <- lists:seq(1, N)].

test2(Iter, Workers) ->
    Self = self(),
    {Time, Res} =
        timer:tc(fun() ->
                         Pids = [ consumer_worker(Iter, Self)
                                  || _I <- lists:seq(1, Workers) ],
                         gather_pids(Pids)
                 end, []),
    {NumOk, NumFail} = lists:foldr(fun({_, L}, {O, F}) ->
                                           {O + proplists:get_value(ok, L),
                                            F + proplists:get_value(fail, L)}
                                   end, {0, 0}, Res),
    {Time, [{ok, NumOk}, {fail, NumFail}]}.

gather_pids(Pids) ->
    gather_pids(Pids, []).

gather_pids([Pid|Rest], Acc) ->
    receive
        {Pid, Ans} ->
            gather_pids(Rest, [{Pid, Ans}|Acc]);
        stop -> stop
    end;
gather_pids([], Acc) ->
    Acc.

