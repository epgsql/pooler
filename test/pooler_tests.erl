-module(pooler_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../src/pooler.hrl").

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

      {"it is ok to return a pid more than once",
       fun() ->
               M = pooler:take_member(test_pool_1),
               [ pooler:return_member(test_pool_1, M)
                 || _I <- lists:seq(1, 37) ],
               M1 = pooler:take_member(test_pool_1),
               M2 = pooler:take_member(test_pool_1),
               ?assert(M1 =/= M2),
               Pool1 = gen_server:call(test_pool_1, dump_pool),
               ?assertEqual(2, Pool1#pool.in_use_count),
               ?assertEqual(0, Pool1#pool.free_count),
               pooler:return_member(test_pool_1, M1),
               pooler:return_member(test_pool_1, M2),
               Pool2 = gen_server:call(test_pool_1, dump_pool),
               ?assertEqual(0, Pool2#pool.in_use_count),
               ?assertEqual(2, Pool2#pool.free_count),
               ok
       end},

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

      {"metrics have been called (no timeout/queue)",
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

      {"metrics have been called (with timeout/queue)",
       fun() ->
               %% exercise the API to ensure we have certain keys reported as metrics
               fake_metrics:reset_metrics(),
               %% pass a non-zero timeout here to exercise queueing
               Pids = [ pooler:take_member(test_pool_1, 1) || _I <- lists:seq(1, 10) ],
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
                                        <<"pooler.test_pool_1.take_rate">>,
                                        <<"pooler.test_pool_1.queue_count">>]),
               Metrics = fake_metrics:get_metrics(),
               GotKeys = lists:usort([ Name || {Name, _, _} <- Metrics ]),
               ?assertEqual(ExpectKeys, GotKeys)
       end},

      {"accept bad member is handled",
       fun() ->
               Bad = spawn(fun() -> ok end),
               FakeStarter = spawn(fun() -> starter end),
               ?assertEqual(ok, pooler:accept_member(test_pool_1, {FakeStarter, Bad}))
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
     [
       {foreach,
        fun() ->
                Pids = get_n_pids(test_pool_1, 10, []),
                ?assertEqual(10, length(pooler:pool_stats(test_pool_1))),
                ?assertEqual(10, length(Pids)),
                Pids
        end,
        fun(Pids) ->
                [ pooler:return_member(test_pool_1, P) || P <- Pids ]
        end,
        [
         fun(Pids) ->
                 {"excess members are culled run 1",
                  fun() ->
                          [ pooler:return_member(test_pool_1, P) || P <- Pids ],
                          %% wait for longer than cull delay
                          timer:sleep(250),
                          ?assertEqual(2, length(pooler:pool_stats(test_pool_1)))
                  end}
         end,

         fun(Pids) ->
                 {"excess members are culled run 2",
                  fun() ->
                          [ pooler:return_member(test_pool_1, P) || P <- Pids ],
                          %% wait for longer than cull delay
                          timer:sleep(250),
                          ?assertEqual(2, length(pooler:pool_stats(test_pool_1)))
                  end}
         end,

         fun(Pids) -> in_use_members_not_culled(Pids, 1) end,
         fun(Pids) -> in_use_members_not_culled(Pids, 2) end,
         fun(Pids) -> in_use_members_not_culled(Pids, 3) end,
         fun(Pids) -> in_use_members_not_culled(Pids, 4) end,
         fun(Pids) -> in_use_members_not_culled(Pids, 5) end,
         fun(Pids) -> in_use_members_not_culled(Pids, 6) end
        ]},

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

in_use_members_not_culled(Pids, N) ->
    {"in-use members are not culled " ++ erlang:integer_to_list(N),
     fun() ->
             %% wait for longer than cull delay
             timer:sleep(250),
             PidCount = length(Pids),
             ?assertEqual(PidCount,
                          length(pooler:pool_stats(test_pool_1))),
             Returns = lists:sublist(Pids, N),
             [ pooler:return_member(test_pool_1, P)
               || P <- Returns ],
             timer:sleep(250),

             ?assertEqual(PidCount - N,
                          length(pooler:pool_stats(test_pool_1)))
     end}.


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

pooler_integration_long_init_test_() ->
    {foreach,
     % setup
     fun() ->
             Pool = [{name, test_pool_1},
                       {max_count, 10},
                       {init_count, 0},
                       {member_start_timeout, {10, ms}},
                       {start_mfa,
                        {pooled_gs, start_link, [{"type-0", fun() -> timer:sleep(15) end}]}}],

             application:set_env(pooler, pools, [Pool]),
             application:start(pooler)
     end,
     % cleanup
     fun(_) ->
             application:stop(pooler)
     end,
     %
     [
      fun(_) ->
              % Test what happens when pool members take too long to start.
              % The pooler_starter should kill off stale members, there by
              % reducing the number of children of the member_sup. This
              % activity occurs both during take member and accept member.
              % Accordingly, the count should go to zero once all starters
              % check in.
              fun() ->
                      ?assertEqual(0, children_count(pooler_test_pool_1_member_sup)),
                      [begin
                           ?assertEqual(error_no_members, pooler:take_member(test_pool_1)),
                           ?assertEqual(1, starting_members(test_pool_1))
                       end
                               || _ <- lists:seq(1,10)],
                      ?assertEqual(10, children_count(pooler_test_pool_1_member_sup)),

                      timer:sleep(150),
                      ?assertEqual(0, children_count(pooler_test_pool_1_member_sup)),
                      ?assertEqual(0, starting_members(test_pool_1))
              end
      end
     ]
     }.

sleep_for_configured_timeout() ->
    SleepTime = case application:get_env(pooler, sleep_time) of
                    {ok, Val} ->
                        Val;
                    _  ->
                        0
                end,
    timer:sleep(SleepTime).    

pooler_integration_queueing_test_() ->
    {foreach,
     % setup
     fun() ->
             Pool = [{name, test_pool_1},
                     {max_count, 10},
                     {queue_max, 10},
                     {init_count, 0},
                     {metrics, fake_metrics},
                     {member_start_timeout, {5, sec}},
                     {start_mfa,
                      {pooled_gs, start_link, [
                                               {"type-0",
                                                fun pooler_tests:sleep_for_configured_timeout/0 }
                                              ]
                      }
                     }
                    ],

             application:set_env(pooler, pools, [Pool]),
             fake_metrics:start_link(),
             application:start(pooler)
     end,
     % cleanup
     fun(_) ->
             fake_metrics:stop(),
             application:stop(pooler)
     end,
     [
      fun(_) ->
              fun() ->
                      ?assertEqual(0, (dump_pool(test_pool_1))#pool.free_count),
                      Val = pooler:take_member(test_pool_1, 10),
                      ?assert(is_pid(Val)),
                      pooler:return_member(test_pool_1, Val)
              end
      end,
      fun(_) ->
              fun() ->
                      application:set_env(pooler, sleep_time, 1),
                      ?assertEqual(0, (dump_pool(test_pool_1))#pool.free_count),
                      Val = pooler:take_member(test_pool_1, 0),
                      ?assertEqual(error_no_members, Val),
                      timer:sleep(50),
                      %Next request should be available
                      Pid = pooler:take_member(test_pool_1, 0),
                      ?assert(is_pid(Pid)),
                      pooler:return_member(test_pool_1, Pid)
              end
      end,
      fun(_) ->
              fun() ->
                      application:set_env(pooler, sleep_time, 10),
                      ?assertEqual(0, (dump_pool(test_pool_1))#pool.free_count),
                      [
                       ?assertEqual(pooler:take_member(test_pool_1, 0), error_no_members) ||
                          _ <- lists:seq(1, (dump_pool(test_pool_1))#pool.max_count)],
                      timer:sleep(50),
                      %Next request should be available
                      Pid = pooler:take_member(test_pool_1, 0),
                      ?assert(is_pid(Pid)),
                      pooler:return_member(test_pool_1, Pid)
              end
      end,
      fun(_) ->
              fun() ->
                      % fill to queue_max, next request should return immediately with no_members
                      % Will return a if queue max is not enforced.
                      application:set_env(pooler, sleep_time, 100),
                      [ proc_lib:spawn(fun() ->
                                               Val = pooler:take_member(test_pool_1, 200),
                                               ?assert(is_pid(Val)),
                                               pooler:return_member(Val)
                                       end)
                        || _ <- lists:seq(1, (dump_pool(test_pool_1))#pool.max_count)
                      ],
                      timer:sleep(50),
                      ?assertEqual(10, queue:len((dump_pool(test_pool_1))#pool.queued_requestors)),
                      ?assertEqual(pooler:take_member(test_pool_1, 500), error_no_members),
                      ExpectKeys = lists:sort([<<"pooler.test_pool_1.error_no_members_count">>,
                                               <<"pooler.test_pool_1.events">>,
                                               <<"pooler.test_pool_1.take_rate">>,
                                               <<"pooler.test_pool_1.queue_count">>,
                                               <<"pooler.test_pool_1.queue_max_reached">>]),
                      Metrics = fake_metrics:get_metrics(),
                      GotKeys = lists:usort([ Name || {Name, _, _} <- Metrics ]),
                      ?assertEqual(ExpectKeys, GotKeys),

                      timer:sleep(100),
                      Val = pooler:take_member(test_pool_1, 500),
                      ?assert(is_pid(Val)),
                      pooler:return_member(test_pool_1, Val)
              end
      end
     ]
    }.
pooler_integration_queueing_return_member_test_() ->
    {foreach,
     % setup
     fun() ->
             Pool = [{name, test_pool_1},
                     {max_count, 10},
                     {queue_max, 10},
                     {init_count, 10},
                     {metrics, fake_metrics},
                     {member_start_timeout, {5, sec}},
                     {start_mfa,
                      {pooled_gs, start_link, [
                                               {"type-0",
                                                fun pooler_tests:sleep_for_configured_timeout/0 }
                                              ]
                      }
                     }
                    ],

             application:set_env(pooler, pools, [Pool]),
             fake_metrics:start_link(),
             application:start(pooler)
     end,
     % cleanup
     fun(_) ->
             fake_metrics:stop(),
             application:stop(pooler)
     end,
     [
      fun(_) ->
              fun() ->
                      application:set_env(pooler, sleep_time, 0),
                      Pids = [ proc_lib:spawn_link(fun() ->
                                               Val = pooler:take_member(test_pool_1, 200),
                                               ?assert(is_pid(Val)),
                                               receive
                                                   _ ->
                                                       pooler:return_member(test_pool_1, Val)
                                                   after
                                                       5000 ->
                                                           pooler:return_member(test_pool_1, Val)
                                               end
                                       end)
                        || _ <- lists:seq(1, (dump_pool(test_pool_1))#pool.max_count)
                      ],
                      timer:sleep(1),
                      Parent = self(),
                      proc_lib:spawn_link(fun() ->
                                             Val = pooler:take_member(test_pool_1, 200),
                                             Parent ! Val
                                     end),
                      [Pid ! return || Pid <- Pids],
                      receive
                          Result ->
                              ?assert(is_pid(Result)),
                              pooler:return_member(test_pool_1, Result)
                      end,
                      ?assertEqual((dump_pool(test_pool_1))#pool.max_count, length((dump_pool(test_pool_1))#pool.free_pids)),
                      ?assertEqual((dump_pool(test_pool_1))#pool.max_count, (dump_pool(test_pool_1))#pool.free_count)
              end
      end
      ]
     }.


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

pooler_auto_grow_disabled_by_default_test_() ->
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
                     {max_count, 5},
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
     [
      {"take one, and it should not auto-grow",
       fun() ->
               ?assertEqual(2, (dump_pool(test_pool_1))#pool.free_count),
               P = pooler:take_member(test_pool_1),
               ?assertMatch({"type-0", _Id}, pooled_gs:get_id(P)),
               timer:sleep(100),
               ?assertEqual(1, (dump_pool(test_pool_1))#pool.free_count),
               ok, pooler:return_member(test_pool_1, P)
       end}
     ]}}.

pooler_auto_grow_enabled_test_() ->
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
                     {max_count, 5},
                     {init_count, 2},
                     {auto_grow_threshold, 1},
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
     [
      {"take one, and it should grow by 2",
       fun() ->
               ?assertEqual(2, (dump_pool(test_pool_1))#pool.free_count),
               P = pooler:take_member(test_pool_1),
               ?assertMatch({"type-0", _Id}, pooled_gs:get_id(P)),
               timer:sleep(100),
               ?assertEqual(3, (dump_pool(test_pool_1))#pool.free_count),
               ok, pooler:return_member(test_pool_1, P)
       end}
     ]}}.

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

children_count(SupId) ->
    length(supervisor:which_children(SupId)).

starting_members(PoolName) ->
    length((dump_pool(PoolName))#pool.starting_members).

dump_pool(PoolName) ->
    gen_server:call(PoolName, dump_pool).
