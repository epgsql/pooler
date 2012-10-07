-define(DEFAULT_ADD_RETRY, 1).
-define(DEFAULT_CULL_INTERVAL, {0, min}).
-define(DEFAULT_MAX_AGE, {0, min}).

-type member_info() :: {string(), free | pid(), {_, _, _}}.
-type free_member_info() :: {string(), free, {_, _, _}}.
-type time_unit() :: min | sec | ms | mu.
-type time_spec() :: {non_neg_integer(), time_unit()}.

-record(pool, {
          name             :: atom(),
          group            :: atom(),
          max_count = 100  :: non_neg_integer(),
          init_count = 10  :: non_neg_integer(),
          start_mfa        :: {atom(), atom(), [term()]},
          free_pids = []   :: [pid()],
          in_use_count = 0 :: non_neg_integer(),
          free_count = 0   :: non_neg_integer(),
          %% The number times to attempt adding a pool member if the
          %% pool size is below max_count and there are no free
          %% members. After this many tries, error_no_members will be
          %% returned by a call to take_member. NOTE: this value
          %% should be >= 2 or else the pool will not grow on demand
          %% when max_count is larger than init_count.
          add_member_retry = ?DEFAULT_ADD_RETRY :: non_neg_integer(),

          %% The interval to schedule a cull message. Both
          %% 'cull_interval' and 'max_age' are specified using a
          %% `time_spec()' type.
          cull_interval = ?DEFAULT_CULL_INTERVAL :: time_spec(),
          %% The maximum age for members.
          max_age = ?DEFAULT_MAX_AGE             :: time_spec(),

          member_sup,
          all_members = dict:new()     :: dict(),
          consumer_to_pid = dict:new() :: dict()
         }).

-define(gv(X, Y), proplists:get_value(X, Y)).
-define(gv(X, Y, D), proplists:get_value(X, Y, D)).


