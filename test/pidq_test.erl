-module(pidq_test).

-include_lib("eunit/include/eunit.hrl").

-compile([export_all]).

% The `user' processes represent users of the pidq library.  A user
% process will take a pid, report details on the pid it has, release
% and take a new pid, stop cleanly, and crash.

start_user() ->
    TC = pidq:take_pid(),
    spawn(fun() -> user_loop(TC) end).

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
    spawn(fun() -> tc_loop({Type, Ref}) end).


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

pidq_integration_test_() ->
    {foreach,
     % setup
     fun() ->
             Pools = [[{name, "p1"},
                      {max_pids, 20},
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
     end
     ]
    }.

      % fun(Users) ->
      % ]}

      % {"users still unique after a renew cycle",
      %  fun() ->
      %          Users = [ start_user() || _X <- lists:seq(1, 10) ],
      %          % return and take new tc pids, expect unique
      %          [ user_new_tc(UPid) || UPid <- Users ],
      %          TcIds = lists:sort([ user_id(UPid) || UPid <- Users ]),
      %          % each user has a different tc ID
      %          ?assertEqual(lists:usort(TcIds), TcIds)


      % ]}.



      %          % return and take new tc pids, still unique
      %          [ user_new_tc(UPid) || UPid <- Users ],
      %          TcIds2 = lists:sort([ user_id(UPid) || UPid <- Users ]),
      %          ?assertEqual(lists:usort(TcIds2), TcIds2),
      %          % if the users all crash...
      %          [ user_crash(UPid) || UPid <- Users ],
      %          Users2 = [ start_user() || _X <- lists:seq(1, 10) ],
      %          TcIds3 = lists:sort([ user_id(UPid) || UPid <- Users ]),
      %          ?assertEqual(lists:usort(TcIds3), TcIds3)

             
