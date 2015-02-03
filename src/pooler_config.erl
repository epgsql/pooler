%% @author Seth Falcon <seth@userprimary.net>
%% @copyright 2012-2013 Seth Falcon
%% @doc Helper module to transform app config proplists into pool records

-module(pooler_config).

-export([list_to_pool/1]).

-include("pooler.hrl").

-spec list_to_pool([{atom(), term()}]) -> #pool{}.
list_to_pool(P) ->
    #pool{
       name              = req(name, P),
       group             = ?gv(group, P),
       max_count         = req(max_count, P),
       init_count        = req(init_count, P),
       start_mfa         = req(start_mfa, P),
       add_member_retry  = ?gv(add_member_retry, P, ?DEFAULT_ADD_RETRY),
       cull_interval     = ?gv(cull_interval, P, ?DEFAULT_CULL_INTERVAL),
       max_age           = ?gv(max_age, P, ?DEFAULT_MAX_AGE),
       member_start_timeout = ?gv(member_start_timeout, P, ?DEFAULT_MEMBER_START_TIMEOUT),
       auto_grow_threshold = ?gv(auto_grow_threshold, P, ?DEFAULT_AUTO_GROW_THRESHOLD),
       metrics_mod       = ?gv(metrics_mod, P, pooler_no_metrics),
       metrics_api       = ?gv(metrics_api, P, folsom),
       queue_max         = ?gv(queue_max, P, ?DEFAULT_POOLER_QUEUE_MAX)}.

%% Return `Value' for `Key' in proplist `P' or crashes with an
%% informative message if no value is found.
req(Key, P) ->
    case lists:keyfind(Key, 1, P) of
        false ->
            error({missing_required_config, Key, P});
        {Key, Value} ->
            Value
    end.
