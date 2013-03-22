-module(pooler_perf_test).

-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").

-define(gv(X, L), proplists:get_value(X, L)).

setup() ->
    setup(10, 100, 5).

setup(InitCount, MaxCount, NumPools) ->
    MakePool = fun(I) ->
                       N = integer_to_list(I),
                       Name = "p" ++ N,
                       Arg0 = "pool-" ++ Name,
                       [{name, list_to_atom(Name)},
                        {max_count, MaxCount},
                        {init_count, InitCount},
                        {start_mfa,
                         {pooled_gs, start_link, [{Arg0}]}}]
               end,
    Pools = [ MakePool(I) || I <- lists:seq(1, NumPools) ],
    application:set_env(pooler, pools, Pools),
    application:start(pooler).

consumer_cycle(N) ->
    consumer_cycle(N, 0, 0).

consumer_cycle(N, NumOk, NumFail) when N > 0 ->
    P = pooler:take_member(p1),
    case P of
        Pid when is_pid(Pid) ->
            true = is_process_alive(P),
            pooler:return_member(p1, P, ok),
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
                                           {O + ?gv(ok, L),
                                            F + ?gv(fail, L)}
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

pooler_take_return_test_() ->
    {foreach,
     % setup
     fun() ->
             InitCount = 100,
             MaxCount = 100,
             NumPools = 5,
             error_logger:delete_report_handler(error_logger_tty_h),
             setup(InitCount, MaxCount, NumPools)
     end,
     fun(_X) ->
             application:stop(pooler)
     end,
     [
      {"take return cycle single worker",
       fun() ->
               NumCycles = 10000,
               Ans = consumer_cycle(NumCycles),
               ?assertEqual(NumCycles, ?gv(ok, Ans)),
               ?assertEqual(0, ?gv(fail, Ans))
       end},

      {"take return cycle multiple workers",
       fun() ->
               Self = self(),
               Iter = 100,
               Workers = 100,
               Pids = [ consumer_worker(Iter, Self)
                        || _I <- lists:seq(1, Workers) ],
               Res = gather_pids(Pids),
               {NumOk, NumFail} =
                   lists:foldr(fun({_, L}, {O, F}) ->
                                       {O + ?gv(ok, L), F + ?gv(fail, L)}
                               end, {0, 0}, Res),
               %% not sure what to test here now. We expect some
               %% failures if init count is less than max count
               %% because of async start.
               ?assertEqual(0, NumFail),
               ?assertEqual(100*100, NumOk)
       end}
      ]
    }.

