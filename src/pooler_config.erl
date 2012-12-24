%% @author Seth Falcon <seth@userprimary.net>
%% @copyright 2012 Seth Falcon
%% @doc Helper module to transform app config proplists into pool records

-module(pooler_config).

-export([list_to_pool/1]).

-include("pooler.hrl").

-spec list_to_pool([{atom(), term()}]) -> #pool{}.
list_to_pool(P) ->
    #pool{
       name              = ?gv(name, P),
       group             = ?gv(group, P),
       max_count         = ?gv(max_count, P),
       init_count        = ?gv(init_count, P),
       start_mfa         = ?gv(start_mfa, P),
       add_member_retry  = ?gv(add_member_retry, P, ?DEFAULT_ADD_RETRY),
       cull_interval     = ?gv(cull_interval, P, ?DEFAULT_CULL_INTERVAL),
       max_age           = ?gv(max_age, P, ?DEFAULT_MAX_AGE),
       metrics_mod       = ?gv(metrics_mod, P, pooler_no_metrics)}.

%% TODO: consider adding some type checking logic for parsing the
%% config to make config errors easier to track down.
