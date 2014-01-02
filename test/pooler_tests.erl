-module(pooler_tests).

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
    user_loop(pooler:take_member(test_pool_1));
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
            pooler:return_member(test_pool_1, MyTC, ok),
            MyNewTC = pooler:take_member(test_pool_1),
            user_loop(MyNewTC);
        stop ->
            pooler:return_member(test_pool_1, MyTC, ok),
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

pooler_basics_via_config_test_() ->
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
             Pools = [[{name, test_pool_1},
                       {max_count, 3},
                       {init_count, 2},
                       {cull_interval, {0, min}},
                       {start_mfa,
                        {pooled_gs, start_link, [{"type-0"}]}}]],
             application:set_env(pooler, pools, Pools),
             error_logger:delete_report_handler(error_logger_tty_h),
             application:start(pooler)
     end,
     fun(_X) ->
             application:stop(pooler)
     end,
     basic_tests()}}.

pooler_basics_dynamic_test_() ->
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
             Pool = [{name, test_pool_1},
                     {max_count, 3},
                     {init_count, 2},
                     {start_mfa,
                      {pooled_gs, start_link, [{"type-0"}]}}],
             application:unset_env(pooler, pools),
             error_logger:delete_report_handler(error_logger_tty_h),
             application:start(pooler),
             pooler:new_pool(Pool)
     end,
     fun(_X) ->
             application:stop(pooler)
     end,
     basic_tests()}}.

pooler_basics_integration_to_other_supervisor_test_() ->
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
             Pool = [{name, test_pool_1},
                     {max_count, 3},
                     {init_count, 2},
                     {start_mfa,
                      {pooled_gs, start_link, [{"type-0"}]}}],
             application:unset_env(pooler, pools),
             error_logger:delete_report_handler(error_logger_tty_h),
             application:start(pooler),
             supervisor:start_link(fake_external_supervisor, Pool)
     end,
     fun({ok, SupPid}) ->
             exit(SupPid, normal),
             application:stop(pooler)
     end,
     basic_tests()}}.


basic_tests() ->
     [
      {"there are init_count members at start",
       fun() ->
               Stats = [ P || {P, {_, free, _}} <- pooler:pool_stats(test_pool_1) ],
               ?assertEqual(2, length(Stats))
       end},

      {"take and return one",
       fun() ->
               P = pooler:take_member(test_pool_1),
               ?assertMatch({"type-0", _Id}, pooled_gs:get_id(P)),
               ok = pooler:return_member(test_pool_1, P, ok)
       end},

      {"take and return one, named pool",
       fun() ->
               P = pooler:take_member(test_pool_1),
               ?assertMatch({"type-0", _Id}, pooled_gs:get_id(P)),
               ok, pooler:return_member(test_pool_1, P)
       end},

      {"attempt to take form unknown pool",
       fun() ->
               %% since pools are now servers, an unknown pool will timeout
               ?assertExit({noproc, _}, pooler:take_member(bad_pool_name))
       end},

      {"members creation is triggered after pool exhaustion until max",
       fun() ->
               %% init count is 2
               Pids0 = [pooler:take_member(test_pool_1), pooler:take_member(test_pool_1)],
               %% since new member creation is async, can only assert
               %% that we will get a pid, but may not be first try.
               Pids = get_n_pids(1, Pids0),
               %% pool is at max now, requests should give error
               ?assertEqual(error_no_members, pooler:take_member(test_pool_1)),
               ?assertEqual(error_no_members, pooler:take_member(test_pool_1)),
               PRefs = [ R || {_T, R} <- [ pooled_gs:get_id(P) || P <- Pids ] ],
               % no duplicates
               ?assertEqual(length(PRefs), length(lists:usort(PRefs)))
       end
      },

      {"pids are reused most recent return first",
       fun() ->
               P1 = pooler:take_member(test_pool_1),
               P2 = pooler:take_member(test_pool_1),
               ?assertNot(P1 == P2),
               ok = pooler:return_member(test_pool_1, P1, ok),
               ok = pooler:return_member(test_pool_1, P2, ok),
               % pids are reused most recent first
               ?assertEqual(P2, pooler:take_member(test_pool_1)),
               ?assertEqual(P1, pooler:take_member(test_pool_1))
       end},

      {"if an in-use pid crashes it is replaced",
       fun() ->
               Pids0 = get_n_pids(3, []),
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
               FreePids = [ P || {P, {_, free, _}} <- pooler:pool_stats(test_pool_1) ],
               [ exit(P, kill) || P <- FreePids ],
               Pids1 = get_n_pids(3, []),
               ?assertEqual(3, length(Pids1))
       end},

      {"if a pid is returned with bad status it is replaced",
       fun() ->
               Pids0 = get_n_pids(3, []),
               Ids0 = [ pooled_gs:get_id(P) || P <- Pids0 ],
               % return them all marking as bad
               [ pooler:return_member(test_pool_1, P, fail) || P <- Pids0 ],
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
               ?assertEqual(ok, pooler:return_member(test_pool_1, Bogus1, ok)),
               ?assertEqual(ok, pooler:return_member(test_pool_1, Bogus2, fail))
       end
      },

      {"calling return_member on error_no_members is ignored",
       fun() ->
               ?assertEqual(ok, pooler:return_member(test_pool_1, error_no_members)),
               ?assertEqual(ok, pooler:return_member(test_pool_1, error_no_members, ok)),
               ?assertEqual(ok, pooler:return_member(test_pool_1, error_no_members, fail))
       end
      },

      {"dynamic pool creation",
       fun() ->
               PoolSpec = [{name, dyn_pool_1},
                           {max_count, 3},
                           {init_count, 2},
                           {start_mfa,
                            {pooled_gs, start_link, [{"dyn-0"}]}}],
               {ok, SupPid1} = pooler:new_pool(PoolSpec),
               ?assert(is_pid(SupPid1)),
               M = pooler:take_member(dyn_pool_1),
               ?assertMatch({"dyn-0", _Id}, pooled_gs:get_id(M)),
               ?assertEqual(ok, pooler:rm_pool(dyn_pool_1)),
               ?assertExit({noproc, _}, pooler:take_member(dyn_pool_1)),
               %% verify pool of same name can be created after removal
               {ok, SupPid2} = pooler:new_pool(PoolSpec),
               ?assert(is_pid(SupPid2)),
               %% remove non-existing pool
               ?assertEqual(ok, pooler:rm_pool(dyn_pool_X)),
               ?assertEqual(ok, pooler:rm_pool(dyn_pool_1))
       end},

      {"metrics have been called",
       fun() ->
               %% exercise the API to ensure we have certain keys reported as metrics
               fake_metrics:reset_metrics(),
               Pids = [ pooler:take_member(test_pool_1) || _I <- lists:seq(1, 10) ],
               [ pooler:return_member(test_pool_1, P) || P <- Pids ],
               catch pooler:take_member(bad_pool_name),
               %% kill and unused member
               exit(hd(Pids), kill),
               %% kill a used member
               KillMe = pooler:take_member(test_pool_1),
               exit(KillMe, kill),
               %% FIXME: We need to wait for pooler to process the
               %% exit message. This is ugly, will fix later.
               timer:sleep(200),                % :(
               ExpectKeys = lists:sort([<<"pooler.test_pool_1.error_no_members_count">>,
                                        <<"pooler.test_pool_1.events">>,
                                        <<"pooler.test_pool_1.free_count">>,
                                        <<"pooler.test_pool_1.in_use_count">>,
                                        <<"pooler.test_pool_1.killed_free_count">>,
                                        <<"pooler.test_pool_1.killed_in_use_count">>,
                                        <<"pooler.test_pool_1.take_rate">>]),
               Metrics = fake_metrics:get_metrics(),
               GotKeys = lists:usort([ Name || {Name, _, _} <- Metrics ]),
               ?assertEqual(ExpectKeys, GotKeys)
       end},

      {"accept bad member is handled",
       fun() ->
               Bad = spawn(fun() -> ok end),
               Ref = erlang:make_ref(),
               ?assertEqual(ok, pooler:accept_member(test_pool_1, {Ref, Bad}))
       end}
      ].

pooler_groups_test_() ->
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
             Pools = [[{name, test_pool_1},
                       {group, group_1},
                       {max_count, 3},
                       {init_count, 2},
                       {start_mfa,
                        {pooled_gs, start_link, [{"type-1-1"}]}}],
                      [{name, test_pool_2},
                       {group, group_1},
                       {max_count, 3},
                       {init_count, 2},
                       {start_mfa,
                        {pooled_gs, start_link, [{"type-1-2"}]}}],
                      %% test_pool_3 not part of the group
                      [{name, test_pool_3},
                       {group, undefined},
                       {max_count, 3},
                       {init_count, 2},
                       {start_mfa,
                        {pooled_gs, start_link, [{"type-3"}]}}]
                     ],
             application:set_env(pooler, pools, Pools),
             %% error_logger:delete_report_handler(error_logger_tty_h),
             pg2:start(),
             application:start(pooler)
     end,
     fun(_X) ->
             application:stop(pooler),
             application:stop(pg2)
     end,
     [
      {"take and return one group member (repeated)",
       fun() ->
               Types = [ begin
                             Pid = pooler:take_group_member(group_1),
                             {Type, _} = pooled_gs:get_id(Pid),
                             ?assertMatch("type-1" ++ _, Type),
                             ok = pooler:return_group_member(group_1, Pid, ok),
                             Type
                         end
                         || _I <- lists:seq(1, 50) ],
               Type_1_1 = [ X || "type-1-1" = X <- Types ],
               Type_1_2 = [ X || "type-1-2" = X <- Types ],
               ?assert(length(Type_1_1) > 0),
               ?assert(length(Type_1_2) > 0)
       end},

      {"take member from unknown group",
       fun() ->
               ?assertEqual({error_no_group, not_a_group},
                            pooler:take_group_member(not_a_group))
       end},

      {"return member to unknown group",
       fun() ->
               Pid = pooler:take_group_member(group_1),
               ?assertEqual(ok, pooler:return_group_member(no_such_group, Pid))
       end},

      {"return member to wrong group",
       fun() ->
               Pid = pooler:take_member(test_pool_3),
               ?assertEqual(ok, pooler:return_group_member(group_1, Pid))
       end},

      {"take member from empty group",
       fun() ->
               %% artificially empty group member list
               [ pg2:leave(group_1, M) || M <- pg2:get_members(group_1) ],
               ?assertEqual(error_no_members, pooler:take_group_member(group_1))
       end},

      {"return member to group, implied ok",
       fun() ->
               Pid = pooler:take_group_member(group_1),
               ?assertEqual(ok, pooler:return_group_member(group_1, Pid))
       end},

      {"return error_no_member to group",
       fun() ->
               ?assertEqual(ok, pooler:return_group_member(group_1, error_no_members))
       end},

      {"exhaust pools in group",
       fun() ->
               Pids = get_n_pids_group(group_1, 6, []),
               %% they should all be pids
               [ begin
                     {Type, _} = pooled_gs:get_id(P),
                     ?assertMatch("type-1" ++ _, Type),
                     ok
                 end || P <- Pids ],
               %% further attempts should be error
               [error_no_members,
                error_no_members,
                error_no_members] = [ pooler:take_group_member(group_1)
                                      || _I <- lists:seq(1, 3) ]
       end},

      {"rm_group with nonexisting group",
       fun() ->
               ?assertEqual(ok, pooler:rm_group(i_dont_exist))
       end},

      {"rm_group with existing empty group",
       fun() ->
               ?assertEqual(ok, pooler:rm_pool(test_pool_1)),
               ?assertEqual(ok, pooler:rm_pool(test_pool_2)),
               ?assertEqual(error_no_members, pooler:take_group_member(group_1)),
               ?assertEqual(ok, pooler:rm_group(group_1)),

               ?assertExit({noproc, _}, pooler:take_member(test_pool_1)),
               ?assertExit({noproc, _}, pooler:take_member(test_pool_2)),
               ?assertEqual({error_no_group, group_1},
                            pooler:take_group_member(group_1))
       end},

      {"rm_group with existing non-empty group",
       fun() ->
               %% Verify that group members exist
               MemberPid = pooler:take_group_member(group_1),
               ?assert(is_pid(MemberPid)),
               pooler:return_group_member(group_1, MemberPid),

               Pool1Pid = pooler:take_member(test_pool_1),
               ?assert(is_pid(Pool1Pid)),
               pooler:return_member(test_pool_1, Pool1Pid),

               Pool2Pid = pooler:take_member(test_pool_2),
               ?assert(is_pid(Pool2Pid)),
               pooler:return_member(test_pool_2, Pool2Pid),

               %% Delete and verify that group and pools are destroyed
               ?assertEqual(ok, pooler:rm_group(group_1)),

               ?assertExit({noproc, _}, pooler:take_member(test_pool_1)),
               ?assertExit({noproc, _}, pooler:take_member(test_pool_2)),
               ?assertEqual({error_no_group, group_1},
                            pooler:take_group_member(group_1))
       end}
     ]}}.

pooler_limit_failed_adds_test_() ->
    %% verify that pooler crashes completely if too many failures are
    %% encountered while trying to add pids.
    {setup,
     fun() ->
             Pools = [[{name, test_pool_1},
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
             ?assertEqual(error_no_members, pooler:take_member(test_pool_1)),
             ?assertEqual(error_no_members, pooler:take_member(test_pool_1))
     end}.

pooler_scheduled_cull_test_() ->
    {setup,
     fun() ->
             application:set_env(pooler, metrics_module, fake_metrics),
             fake_metrics:start_link(),
             Pools = [[{name, test_pool_1},
                       {max_count, 10},
                       {init_count, 2},
                       {start_mfa, {pooled_gs, start_link, [{"type-0"}]}},
                       {cull_interval, {200, ms}},
                       {max_age, {0, min}}]],
             application:set_env(pooler, pools, Pools),
             %% error_logger:delete_report_handler(error_logger_tty_h),
             application:start(pooler)
     end,
     fun(_X) ->
             fake_metrics:stop(),
             application:stop(pooler)
     end,
     [{"excess members are culled repeatedly",
       fun() ->
               %% take all members
               Pids1 = get_n_pids(test_pool_1, 10, []),
               %% return all
               [ pooler:return_member(test_pool_1, P) || P <- Pids1 ],
               ?assertEqual(10, length(pooler:pool_stats(test_pool_1))),
               %% wait for longer than cull delay
               timer:sleep(250),
               ?assertEqual(2, length(pooler:pool_stats(test_pool_1))),

               %% repeat the test to verify that culling gets rescheduled.
               Pids2 = get_n_pids(test_pool_1, 10, []),
               %% return all
               [ pooler:return_member(test_pool_1, P) || P <- Pids2 ],
               ?assertEqual(10, length(pooler:pool_stats(test_pool_1))),
               %% wait for longer than cull delay
               timer:sleep(250),
               ?assertEqual(2, length(pooler:pool_stats(test_pool_1)))
       end
      },

      {"non-excess members are not culled",
       fun() ->
               [P1, P2] = [pooler:take_member(test_pool_1) || _X <- [1, 2] ],
               [pooler:return_member(test_pool_1, P) || P <- [P1, P2] ],
               ?assertEqual(2, length(pooler:pool_stats(test_pool_1))),
               timer:sleep(250),
               ?assertEqual(2, length(pooler:pool_stats(test_pool_1)))
       end
      },

      {"in-use members are not culled",
       fun() ->
               %% take all members
               Pids = get_n_pids(test_pool_1, 10, []),
               %% don't return any
               ?assertEqual(10, length(pooler:pool_stats(test_pool_1))),
               %% wait for longer than cull delay
               timer:sleep(250),
               ?assertEqual(10, length(pooler:pool_stats(test_pool_1))),
               [ pooler:return_member(test_pool_1, P) || P <- Pids ]
       end},

      {"no cull when init_count matches max_count",
       %% not sure how to verify this. But this test at least
       %% exercises the code path.
       fun() ->
               Config = [{name, test_static_pool_1},
                         {max_count, 2},
                         {init_count, 2},
                         {start_mfa, {pooled_gs, start_link, [{"static-0"}]}},
                         {cull_interval, {200, ms}}], % ignored
               pooler:new_pool(Config),
               P = pooler:take_member(test_static_pool_1),
               ?assertMatch({"static-0", _}, pooled_gs:get_id(P)),
               pooler:return_member(test_static_pool_1, P),
               ok
       end}
     ]}.

random_message_test_() ->
    {setup,
     fun() ->
             Pools = [[{name, test_pool_1},
                       {max_count, 2},
                       {init_count, 1},
                       {start_mfa,
                        {pooled_gs, start_link, [{"type-0"}]}}]],
             application:set_env(pooler, pools, Pools),
             error_logger:delete_report_handler(error_logger_tty_h),
             application:start(pooler),
             %% now send some bogus messages
             %% do the call in a throw-away process to avoid timeout error
             spawn(fun() -> catch gen_server:call(test_pool_1, {unexpected_garbage_msg, 5}) end),
             gen_server:cast(test_pool_1, {unexpected_garbage_msg, 6}),
             whereis(test_pool_1) ! {unexpected_garbage_msg, 7},
             ok
     end,
     fun(_) ->
             application:stop(pooler)
     end,
    [
     fun() ->
             Pid = spawn(fun() -> ok end),
             MonMsg = {'DOWN', erlang:make_ref(), process, Pid, because},
             test_pool_1 ! MonMsg
     end,

     fun() ->
             Pid = pooler:take_member(test_pool_1),
             {Type, _} =  pooled_gs:get_id(Pid),
             ?assertEqual("type-0", Type)
     end,

     fun() ->
             RawPool = gen_server:call(test_pool_1, dump_pool),
             ?assertEqual(pool, element(1, RawPool))
     end
    ]}.

pooler_integration_test_() ->
    {foreach,
     % setup
     fun() ->
             Pools = [[{name, test_pool_1},
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

time_as_millis_test_() ->
    Zeros = [ {{0, U}, 0} || U <- [min, sec, ms, mu] ],
    Ones = [{{1, min}, 60000},
            {{1, sec}, 1000},
            {{1, ms}, 1},
            {{1, mu}, 0}],
    Misc = [{{3000, mu}, 3}],
    Tests = Zeros ++ Ones ++ Misc,
    [ ?_assertEqual(E, pooler:time_as_millis(I)) || {I, E} <- Tests ].

time_as_micros_test_() ->
    Zeros = [ {{0, U}, 0} || U <- [min, sec, ms, mu] ],
    Ones = [{{1, min}, 60000000},
            {{1, sec}, 1000000},
            {{1, ms}, 1000},
            {{1, mu}, 1}],
    Misc = [{{3000, mu}, 3000}],
    Tests = Zeros ++ Ones ++ Misc,
    [ ?_assertEqual(E, pooler:time_as_micros(I)) || {I, E} <- Tests ].

% testing crash recovery means race conditions when either pids
% haven't yet crashed or pooler hasn't recovered.  So this helper loops
% forver until N pids are obtained, ignoring error_no_members.
get_n_pids(N, Acc) ->
    get_n_pids(test_pool_1, N, Acc).

get_n_pids(_Pool, 0, Acc) ->
    Acc;
get_n_pids(Pool, N, Acc) ->
    case pooler:take_member(Pool) of
        error_no_members ->
            get_n_pids(Pool, N, Acc);
        Pid ->
            get_n_pids(Pool, N - 1, [Pid|Acc])
    end.

get_n_pids_group(_Group, 0, Acc) ->
    Acc;
get_n_pids_group(Group, N, Acc) ->
    case pooler:take_group_member(Group) of
        error_no_members ->
            get_n_pids_group(Group, N, Acc);
        Pid ->
            get_n_pids_group(Group, N - 1, [Pid|Acc])
    end.
