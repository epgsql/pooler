-module(pooler_test).

-include_lib("eunit/include/eunit.hrl").

-compile([export_all]).

% The `user' processes represent users of the pooler library.  A user
% process will take a pid, report details on the pid it has, release
% and take a new pid, stop cleanly, and crash.

start_user() ->
    spawn(fun() -> user_loop(start) end).

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

user_loop(Atom) when Atom =:= error_no_members orelse Atom =:= start ->
    user_loop(pooler:take_member());
user_loop(MyTC) ->
    receive
        {get_tc_id, From} ->
            From ! pooled_gs:get_id(MyTC),
            user_loop(MyTC);
        {ping_tc, From} ->
            From ! pooled_gs:ping(MyTC),
            user_loop(MyTC);
        {ping_count, From} ->
            From ! pooled_gs:ping_count(MyTC),
            user_loop(MyTC);
        new_tc ->
            pooler:return_member(MyTC, ok),
            MyNewTC = pooler:take_member(),
            user_loop(MyNewTC);
        stop ->
            pooler:return_member(MyTC, ok),
            stopped;
        crash ->
            erlang:error({user_loop, kaboom})
    end.

% The `tc' processes represent the pids tracked by pooler for testing.
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

% tc_sanity_test() ->
%     Pid1 = tc_starter("1"),
%     {"1", Id1} = get_tc_id(Pid1),
%     Pid2 = tc_starter("1"),
%     {"1", Id2} = get_tc_id(Pid2),
%     ?assertNot(Id1 == Id2),
%     stop_tc(Pid1),
%     stop_tc(Pid2).

% user_sanity_test() ->
%     Pid1 = tc_starter("1"),
%     User = spawn(fun() -> user_loop(Pid1) end),
%     ?assertMatch({"1", _Ref}, user_id(User)),
%     user_crash(User),
%     stop_tc(Pid1).

pooler_basics_test_() ->
    {foreach,
     % setup
     fun() ->
             Pools = [[{name, "p1"},
                       {max_count, 3},
                       {init_count, 2},
                       {start_mfa,
                        {pooled_gs, start_link, [{"type-0"}]}}]],
             application:set_env(pooler, pools, Pools),
             application:start(pooler)
     end,
     fun(_X) ->
             application:stop(pooler)
     end,
     [
      {"there are init_count members at start",
       fun() ->
               Stats = [ P || {P, {_, free, _}} <- pooler:pool_stats() ],
               ?assertEqual(2, length(Stats))
       end},

      {"take and return one",
       fun() ->
               P = pooler:take_member(),
               ?assertMatch({"type-0", _Id}, pooled_gs:get_id(P)),
               ok = pooler:return_member(P, ok)
       end},

      {"pids are created on demand until max",
       fun() ->
               Pids = [pooler:take_member(), pooler:take_member(), pooler:take_member()],
               ?assertMatch(error_no_members, pooler:take_member()),
               ?assertMatch(error_no_members, pooler:take_member()),
               PRefs = [ R || {_T, R} <- [ pooled_gs:get_id(P) || P <- Pids ] ],
               % no duplicates
               ?assertEqual(length(PRefs), length(lists:usort(PRefs)))
       end
      },

      {"pids are reused most recent return first",
       fun() ->
               P1 = pooler:take_member(),
               P2 = pooler:take_member(),
               ?assertNot(P1 == P2),
               ok = pooler:return_member(P1, ok),
               ok = pooler:return_member(P2, ok),
               % pids are reused most recent first
               ?assertEqual(P2, pooler:take_member()),
               ?assertEqual(P1, pooler:take_member())
       end},

      {"if an in-use pid crashes it is replaced",
       fun() ->
               Pids0 = [pooler:take_member(), pooler:take_member(),
                        pooler:take_member()],
               Ids0 = [ pooled_gs:get_id(P) || P <- Pids0 ],
               % crash them all
               [ pooled_gs:crash(P) || P <- Pids0 ],
               Pids1 = get_n_pids(3, []),
               Ids1 = [ pooled_gs:get_id(P) || P <- Pids1 ],
               [ ?assertNot(lists:member(I, Ids0)) || I <- Ids1 ]
       end
       },

      {"if a free pid crashes it is replaced",
       fun() ->
               FreePids = [ P || {P, {_, free, _}} <- pooler:pool_stats() ],
               [ exit(P, kill) || P <- FreePids ],
               Pids1 = get_n_pids(3, []),
               ?assertEqual(3, length(Pids1))
       end},

      {"if a pid is returned with bad status it is replaced",
       fun() ->
               Pids0 = [pooler:take_member(), pooler:take_member(), pooler:take_member()],
               Ids0 = [ pooled_gs:get_id(P) || P <- Pids0 ],
               % return them all marking as bad
               [ pooler:return_member(P, fail) || P <- Pids0 ],
               Pids1 = get_n_pids(3, []),
               Ids1 = [ pooled_gs:get_id(P) || P <- Pids1 ],
               [ ?assertNot(lists:member(I, Ids0)) || I <- Ids1 ]
       end
      },

      {"if a consumer crashes, pid is replaced",
       fun() ->
               Consumer = start_user(),
               StartId = user_id(Consumer),
               user_crash(Consumer),
               NewPid = hd(get_n_pids(1, [])),
               NewId = pooled_gs:get_id(NewPid),
               ?assertNot(NewId == StartId)
       end
      }
     ]}.


pooler_integration_test_() ->
    {foreach,
     % setup
     fun() ->
             Pools = [[{name, "p1"},
                       {max_count, 10},
                       {init_count, 10},
                       {start_mfa,
                        {pooled_gs, start_link, [{"type-0"}]}}]],
             application:set_env(pooler, pools, Pools),
             application:start(pooler),
             Users = [ start_user() || _X <- lists:seq(1, 10) ],
             Users
     end,
     % cleanup
     fun(Users) ->
             [ user_stop(U) || U <- Users ],
             application:stop(pooler)
     end,
     %
     [
      fun(Users) ->
             fun() ->
                     % each user has a different tc ID
                     TcIds = lists:sort([ user_id(UPid) || UPid <- Users ]),
                     ?assertEqual(lists:usort(TcIds), TcIds)
             end
      end
      ,

      fun(Users) ->
              fun() ->
                      % users still unique after a renew cycle
                      [ user_new_tc(UPid) || UPid <- Users ],
                      TcIds = lists:sort([ user_id(UPid) || UPid <- Users ]),
                      ?assertEqual(lists:usort(TcIds), TcIds)
              end
      end
      ,

      fun(Users) ->
              fun() ->
                      % all users crash, pids are replaced
                      TcIds1 = lists:sort([ user_id(UPid) || UPid <- Users ]),
                      [ user_crash(UPid) || UPid <- Users ],
                      Seq = lists:seq(1, 5),
                      Users2 = [ start_user() || _X <- Seq ],
                      TcIds2 = lists:sort([ user_id(UPid) || UPid <- Users2 ]),
                      Both =
                          sets:to_list(sets:intersection([sets:from_list(TcIds1),
                                                          sets:from_list(TcIds2)])),
                      ?assertEqual([], Both)
              end
      end
     ]
    }.

% testing crash recovery means race conditions when either pids
% haven't yet crashed or pooler hasn't recovered.  So this helper loops
% forver until N pids are obtained, ignoring error_no_members.
get_n_pids(0, Acc) ->
    Acc;
get_n_pids(N, Acc) ->
    case pooler:take_member() of
        error_no_members ->
            get_n_pids(N, Acc);
        Pid ->
            get_n_pids(N - 1, [Pid|Acc])
    end.
