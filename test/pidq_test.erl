-module(pidq_test).

-include_lib("eunit/include/eunit.hrl").

-compile([export_all]).

% The `user' processes represent users of the pidq library.  A user
% process will take a pid, report details on the pid it has, release
% and take a new pid, stop cleanly, and crash.

start_user() ->
    spawn(fun() ->
                  TC = pidq:take_pid(),
                  user_loop(TC)
          end).

user_id(Pid) ->
    Pid ! {get_tc_id, self()},
    receive
        {Type, Id} ->
            {Type, Id}
    end.

user_new_tc(Pid) ->
    Pid ! new_tc.

user_stop(Pid) ->
    Pid ! stop.

user_crash(Pid) ->
    Pid ! crash.

user_loop(MyTC) ->
    receive
        {get_tc_id, From} ->
            From ! get_tc_id(MyTC),
            user_loop(MyTC);
        new_tc ->
            pidq:return_pid(MyTC, ok),
            MyNewTC = pidq:take_pid(),
            user_loop(MyNewTC);
        stop ->
            pidq:return_pid(MyTC, ok),
            stopped;
        crash ->
            erlang:error({user_loop, kaboom})
    end.

% The `tc' processes represent the pids tracked by pidq for testing.
% They have a type and an ID and can report their type and ID and
% stop.

tc_loop({Type, Id}) ->
    receive
        {get_id, From} ->
            From ! {ok, Type, Id},
            tc_loop({Type, Id});
        stop -> stopped;
        crash ->
            erlang:error({tc_loop, kaboom})
    end.

get_tc_id(Pid) ->
    Pid ! {get_id, self()},
    receive
        {ok, Type, Id} ->
            {Type, Id}
    after 200 ->
            timeout
    end.

stop_tc(Pid) ->
    Pid ! stop.

tc_starter(Type) ->
    Ref = make_ref(),
    spawn_link(fun() -> tc_loop({Type, Ref}) end).

assert_tc_valid(Pid) ->
    ?assertMatch({_Type, _Ref}, get_tc_id(Pid)),
    ok.

tc_sanity_test() ->
    Pid1 = tc_starter("1"),
    {"1", Id1} = get_tc_id(Pid1),
    Pid2 = tc_starter("1"),
    {"1", Id2} = get_tc_id(Pid2),
    ?assertNot(Id1 == Id2),
    stop_tc(Pid1),
    stop_tc(Pid2).

user_sanity_test() ->
    Pid1 = tc_starter("1"),
    User = spawn(fun() -> user_loop(Pid1) end),
    ?assertMatch({"1", _Ref}, user_id(User)),
    user_crash(User),
    stop_tc(Pid1).

pidq_basics_test_() ->
    {foreach,
     % setup
     fun() ->
             Pools = [[{name, "p1"},
                       {max_pids, 3}, {min_free, 1},
                       {init_size, 2}, {pid_starter_args, ["type-0"]}]],

             Config = [{pid_starter, {?MODULE, tc_starter}},
                       {pid_stopper, {?MODULE, stop_tc}},
                       {pools, Pools}],
             pidq:start(Config)
     end,
     fun(_X) ->
             pidq:stop()
     end,
     [
      {"take and return one",
       fun() ->
               P = pidq:take_pid(),
               ?assertMatch({"type-0", _Id}, get_tc_id(P)),
               ok = pidq:return_pid(P, ok)
       end},

      {"pids are created on demand until max",
       fun() ->
               Pids = [pidq:take_pid(), pidq:take_pid(), pidq:take_pid()],
               ?assertMatch(error_no_pids, pidq:take_pid()),
               ?assertMatch(error_no_pids, pidq:take_pid()),
               PRefs = [ R || {_T, R} <- [ get_tc_id(P) || P <- Pids ] ],
               ?assertEqual(length(PRefs), length(lists:usort(PRefs)))
       end
      },

      {"pids are reused most recent return first",
       fun() ->
               P1 = pidq:take_pid(),
               P2 = pidq:take_pid(),
               ?assertNot(P1 == P2),
               ok = pidq:return_pid(P1, ok),
               ok = pidq:return_pid(P2, ok),
               % pids are reused most recent first
               ?assertEqual(P2, pidq:take_pid()),
               ?assertEqual(P1, pidq:take_pid())
       end},

      {"if a pid crashes it is replaced",
       fun() ->
               Pids0 = [pidq:take_pid(), pidq:take_pid(), pidq:take_pid()],
               Ids0 = [ get_tc_id(P) || P <- Pids0 ],
               % crash them all
               [ P ! crash || P <- Pids0 ],
               Pids1 = get_n_pids(3, []),
               Ids1 = [ get_tc_id(P) || P <- Pids1 ],
               [ ?assertNot(lists:member(I, Ids0)) || I <- Ids1 ]
       end
       },

      {"if a pid is returned with bad status it is replaced",
       fun() ->
               Pids0 = [pidq:take_pid(), pidq:take_pid(), pidq:take_pid()],
               Ids0 = [ get_tc_id(P) || P <- Pids0 ],
               % return them all marking as bad
               [ pidq:return_pid(P, fail) || P <- Pids0 ],
               Pids1 = get_n_pids(3, []),
               Ids1 = [ get_tc_id(P) || P <- Pids1 ],
               [ ?assertNot(lists:member(I, Ids0)) || I <- Ids1 ]
       end
      },

      {"if a consumer crashes, pid is replaced",
       fun() ->
               Consumer = start_user(),
               StartId = user_id(Consumer),
               ?debugVal(pidq:pool_stats("p1")),
               user_crash(Consumer),
               NewPid = hd(get_n_pids(1, [])),
               NewId = get_tc_id(NewPid),
               ?debugVal(pidq:pool_stats("p1")),
               ?assertNot(NewId == StartId)
       end
      }
     ]}.


pidq_integration_test_() ->
    {foreach,
     % setup
     fun() ->
             Pools = [[{name, "p1"},
                      {max_pids, 10},
                      {min_free, 3},
                      {init_size, 10},
                      {pid_starter_args, ["type-0"]}]],

             Config = [{pid_starter, {?MODULE, tc_starter}},
                       {pid_stopper, {?MODULE, stop_tc}},
                       {pools, Pools}],
             pidq:start(Config),
             Users = [ start_user() || _X <- lists:seq(1, 10) ],
             Users
     end,
     % cleanup
     fun(Users) ->
             [ user_stop(U) || U <- Users ],
             pidq:stop()
     end,
     %
     [
      fun(Users) ->
             fun() ->
                     % each user has a different tc ID
                     TcIds = lists:sort([ user_id(UPid) || UPid <- Users ]),
                     ?assertEqual(lists:usort(TcIds), TcIds)
             end
      end,

      fun(Users) ->
              fun() ->
                      % users still unique after a renew cycle
                      [ user_new_tc(UPid) || UPid <- Users ],
                      TcIds = lists:sort([ user_id(UPid) || UPid <- Users ]),
                      ?assertEqual(lists:usort(TcIds), TcIds)
              end
      end
      % ,

      % fun(Users) ->
      %         fun() ->
      %                 % all users crash, pids reused
      %                 TcIds1 = lists:sort([ user_id(UPid) || UPid <- Users ]),
      %                 [ user_crash(UPid) || UPid <- Users ],
      %                 % Seq = lists:seq(1, length(Users)),
      %                 Seq = lists:seq(1, 5),
      %                 Users2 = [ start_user() || _X <- Seq ],
      %                 TcIds2 = lists:sort([ user_id(UPid) || UPid <- Users2 ]),
      %                 ?assertEqual(TcIds1, TcIds2)
      %         end
      % end
     ]
    }.

      %          % return and take new tc pids, still unique
      %          [ user_new_tc(UPid) || UPid <- Users ],
      %          TcIds2 = lists:sort([ user_id(UPid) || UPid <- Users ]),
      %          ?assertEqual(lists:usort(TcIds2), TcIds2),
      %          % if the users all crash...


% testing crash recovery means race conditions when either pids
% haven't yet crashed or pidq hasn't recovered.  So this helper loops
% forver until N pids are obtained, ignoring error_no_pids.
get_n_pids(0, Acc) ->
    Acc;
get_n_pids(N, Acc) ->
    case pidq:take_pid() of
        error_no_pids ->
            get_n_pids(N, Acc);
        Pid ->
            get_n_pids(N - 1, [Pid|Acc])
    end.
