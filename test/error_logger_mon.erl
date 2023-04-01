%%% A gen_server to check if we get any error_logger messages during test to see if
%%% any messages gets generated when they shouldn't

-module(error_logger_mon).

-behaviour(gen_server).
-define(SERVER, ?MODULE).

-record(state, {events = [] :: [logger:log_event()]}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0,
         report/1,
         get_msg_count/0,
         get_msgs/0,
         reset/0,
         stop/0,
         install_handler/0,
         install_handler/1,
         uninstall_handler/0
        ]).

%% OTP logger
-export([log/2]).

%% gen_server
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


%% Logger handler

log(Event, _) ->
    error_logger_mon:report(Event),
    ok.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

report(Event) ->
    gen_server:call(?SERVER, {report, Event}).

get_msg_count() ->
    gen_server:call(?SERVER, get_count).

get_msgs() ->
    gen_server:call(?SERVER, get_events).

reset() ->
    gen_server:call(?SERVER, reset).

stop() ->
    gen_server:call(?SERVER, stop).

install_handler() ->
    install_handler(error_logger).

install_handler(FilterName) ->
    logger:add_handler(
      ?MODULE,
      ?MODULE,
      #{level => all,
        filter_default => stop,
        filters => [{FilterName, filter(FilterName)}]}).

filter(error_logger) ->
    {fun error_logger_filter/2, []};
filter(pooler) ->
    {fun logger_filters:domain/2, {log, sub, [pooler]}}.


uninstall_handler() ->
    logger:remove_handler(?MODULE).

error_logger_filter(#{meta := #{error_logger := #{tag := _}}} = E, _) ->
    E;
error_logger_filter(_, _) ->
    ignore.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([]) ->
    {ok, #state{}}.

handle_call(get_count, _From, #state{events = E} = State) ->
    {reply, length(E), State};
handle_call(get_events, _From, #state{events = E} = State) ->
    {reply, E, State};
handle_call({report, Event}, _From, #state{events = E} = State) ->
    {reply, ok, State#state{events = [Event | E]}};
handle_call(reset, _From, State) ->
    {reply, ok, State#state{events = []}};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, error, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
