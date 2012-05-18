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
    {setup,
     fun() ->
             application:set_env(pooler, metrics_module, fake_metrics),
             fake_metrics:start_link()
     end,
     fun(_X) ->
             fake_metrics:stop()
     end,
    {foreach,
     % setup
     fun() ->
             Pools = [[{name, "p1"},
                       {max_count, 3},
                       {init_count, 2},
                       {start_mfa,
                        {pooled_gs, start_link, [{"type-0"}]}}]],
             application:set_env(pooler, pools, Pools),
             error_logger:delete_report_handler(error_logger_tty_h),
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

      {"take and return one, named pool",
       fun() ->
               P = pooler:take_member("p1"),
               ?assertMatch({"type-0", _Id}, pooled_gs:get_id(P)),
               ok, pooler:return_member(P)
       end},

      {"attempt to take form unknown pool",
       fun() ->
               ?assertEqual(error_no_pool, pooler:take_member("bad_pool_name"))
       end},

      {"pids are created on demand until max",
       fun() ->
               Pids = [pooler:take_member(), pooler:take_member(), pooler:take_member()],
               ?assertEqual(error_no_members, pooler:take_member()),
               ?assertEqual(error_no_members, pooler:take_member()),
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
      },

      {"it is ok to return an unknown pid",
       fun() ->
               Bogus1 = spawn(fun() -> ok end),
               Bogus2 = spawn(fun() -> ok end),
               ?assertEqual(ok, pooler:return_member(Bogus1, ok)),
               ?assertEqual(ok, pooler:return_member(Bogus2, fail))
       end
      },

      {"calling return_member on error_no_members is ignored",
       fun() ->
               ?assertEqual(ok, pooler:return_member(error_no_members)),
               ?assertEqual(ok, pooler:return_member(error_no_members, ok)),
               ?assertEqual(ok, pooler:return_member(error_no_members, fail))
       end
      },

      {"cull_pool can be called and do nothing",
       %% FIXME: this exercises the code path, but doesn't test anything
       fun() ->
               ?assertEqual(ok, pooler:cull_pool("p1", 10))
       end
      },

      {"cull_pool culls unused members",
       fun() ->
               %% take all
               [P1, P2, _P3] = [pooler:take_member(), pooler:take_member(), pooler:take_member()],
               %% return one
               pooler:return_member(P1),
               pooler:return_member(P2),
               %% call a sync action since return_member is async
               _Ignore = pooler:pool_stats(),
               ?assertEqual(ok, pooler:cull_pool("p1", 0)),
               ?assertEqual(2, length(pooler:pool_stats()))
       end
      },

      {"metrics have been called",
       fun() ->
               %% exercise the API to ensure we have certain keys reported as metrics
               fake_metrics:reset_metrics(),
               Pids = [ pooler:take_member() || _I <- lists:seq(1, 10) ],
               [ pooler:return_member(P) || P <- Pids ],
               pooler:take_member("bad_pool_name"),
               %% kill and unused member
               exit(hd(Pids), kill),
               %% kill a used member
               KillMe = pooler:take_member("p1"),
               exit(KillMe, kill),
               %% FIXME: We need to wait for pooler to process the
               %% exit message. This is ugly, will fix later.
               timer:sleep(200),                % :(
               ExpectKeys = [<<"pooler.error_no_members_count">>,
                             <<"pooler.events">>,
                             <<"pooler.killed_free_count">>,
                             <<"pooler.killed_in_use_count">>,
                             <<"pooler.p1.free_count">>,
                             <<"pooler.p1.in_use_count">>,
                             <<"pooler.p1.take_rate">>],
               Metrics = fake_metrics:get_metrics(),
               GotKeys = lists:usort([ Name || {Name, _, _} <- Metrics ]),
               ?assertEqual(ExpectKeys, GotKeys)
       end}
     ]}}.

pooler_limit_failed_adds_test_() ->
    %% verify that pooler crashes completely if too many failures are
    %% encountered while trying to add pids.
    {setup,
     fun() ->
             Pools = [[{name, "p1"},
                       {max_count, 10},
                       {init_count, 10},
                       {start_mfa,
                        {pooled_gs, start_link, [crash]}}]],
             application:set_env(pooler, pools, Pools)
     end,
     fun(_) ->
             application:stop(pooler)
     end,
     fun() ->
             application:start(pooler),
             ?assertEqual(error_no_members, pooler:take_member()),
             ?assertEqual(error_no_members, pooler:take_member("p1"))
     end}.

random_message_test_() ->
    {setup,
     fun() ->
             Pools = [[{name, "p1"},
                       {max_count, 2},
                       {init_count, 1},
                       {start_mfa,
                        {pooled_gs, start_link, [{"type-0"}]}}]],
             application:set_env(pooler, pools, Pools),
             error_logger:delete_report_handler(error_logger_tty_h),
             application:start(pooler),
             %% now send some bogus messages
             %% do the call in a throw-away process to avoid timeout error
             spawn(fun() -> catch gen_server:call(pooler, {unexpected_garbage_msg, 5}) end),
             gen_server:cast(pooler, {unexpected_garbage_msg, 6}),
            whereis(pooler) ! {unexpected_garbage_msg, 7},
             ok
     end,
     fun(_) ->
             application:stop(pooler)
     end,
    [
     fun() ->
             Pid = pooler:take_member("p1"),
             {Type, _} =  pooled_gs:get_id(Pid),
             ?assertEqual("type-0", Type)
     end
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
             error_logger:delete_report_handler(error_logger_tty_h),
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
