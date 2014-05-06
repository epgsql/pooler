-define(DEFAULT_ADD_RETRY, 1).
-define(DEFAULT_CULL_INTERVAL, {1, min}).
-define(DEFAULT_MAX_AGE, {30, sec}).
-define(DEFAULT_MEMBER_START_TIMEOUT, {1, min}).
-define(POOLER_GROUP_TABLE, pooler_group_table).

-type member_info() :: {string(), free | pid(), {_, _, _}}.
-type free_member_info() :: {string(), free, {_, _, _}}.
-type time_unit() :: min | sec | ms | mu.
-type time_spec() :: {non_neg_integer(), time_unit()}.

-ifdef(namespaced_types).
-type p_dict() :: dict:dict().
-else.
-type p_dict() :: dict().
-endif.

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

          %% The supervisor used to start new members
          member_sup :: atom() | pid(),

          %% The supervisor used to start starter servers that start
          %% new members. This is what enables async member starts.
          starter_sup :: atom() | pid(),

          %% Maps member pid to a tuple of the form:
          %% {MonitorRef, Status, Time},
          %% where MonitorRef is a monitor reference for the member,,
          %% Status is either 'free' or the consumer pid, and Time is
          %% an Erlang timestamp that records when the member became
          %% free.

          all_members = dict:new()     :: p_dict(),

          %% Maps consumer pid to a tuple of the form:
          %% {MonitorRef, MemberList} where MonitorRef is a monitor
          %% reference for the consumer and MemberList is a list of
          %% members being consumed.
          consumer_to_pid = dict:new() :: p_dict(),

          %% A list of `{References, Timestamp}' tuples representing
          %% new member start requests that are in-flight. The
          %% timestamp records when the start request was initiated
          %% and is used to implement start timeout.
          starting_members = [] :: [{reference(), erlang:timestamp()}],

          %% The maximum amount of time to allow for member start.
          member_start_timeout = ?DEFAULT_MEMBER_START_TIMEOUT :: time_spec(),

          %% The module to use for collecting metrics. If set to
          %% 'pooler_no_metrics', then metric sending calls do
          %% nothing. A typical value to actually capture metrics is
          %% folsom_metrics.
          metrics_mod = pooler_no_metrics :: atom()
         }).

-define(gv(X, Y), proplists:get_value(X, Y)).
-define(gv(X, Y, D), proplists:get_value(X, Y, D)).
