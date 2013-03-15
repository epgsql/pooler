%% @doc basho_bench driver for pooler testing
-module(pooler_driver).

-export([
         new/1,
         pool_name/1,
         run/4
         ]).


-record(state, {
          %% integer id received from new/1
          id = 0,

          %% pid of consumer worker process
          consumer = undefined
         }).
          
new(ID) ->
    %% this is bogus, b/c called too many times.
    init_driver(),
    {ok, Consumer} = consumer_sup:new_consumer(),
    {ok, #state{id = ID, consumer = Consumer}}.

%% KeyGen can be a function that returns a pool name atom.
run(simple, PoolNameFun, _ValueGen, #state{consumer = _C} = State) ->
    PoolName = PoolNameFun(),
    case pooler:take_member(PoolName) of
        error_no_members ->
            {error, error_no_members, State};
        Pid ->
            pooler:return_member(PoolName, Pid),
            {ok, State}
    end;
run(fast, PoolNameFun, _ValueGen, #state{consumer = C} = State) ->
    PoolName = PoolNameFun(),
    ConsumerOpts = [{consumer_crash, false},
                    {member_crash, false},
                    {take_cycles, 1},
                    {think_time, 10},
                    {pool_name, PoolName}
                   ],
    consumer:run(C, ConsumerOpts),
    {ok, State};
run(slow, PoolNameFun, _ValueGen, #state{consumer = C} = State) ->
    PoolName = PoolNameFun(),
    ConsumerOpts = [{consumer_crash, false},
                    {member_crash, false},
                    {take_cycles, 1},
                    {think_time, 200},
                    {pool_name, PoolName}
                   ],
    consumer:run(C, ConsumerOpts),
    {ok, State}.



%% gets called as the PoolNameFun aka key_generator via basho_bench config
pool_name(_Id) ->
    fun() -> p1 end.

init_driver() ->
    consumer_sup:start_link(),
    member_sup:start_link(),
    application:start(pooler),
    Delay = 1000,
    PoolConfig = [{name, p1},
                  {max_count, 5},
                  {init_count, 2},
                  {start_mfa,
                   {member_sup, new_member, [Delay]}}],
    pooler:new_pool(PoolConfig),
    ok.
