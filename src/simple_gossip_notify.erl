%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_notify).
-author("Peter Tihanyi").

-behaviour(gen_event).

-include("simple_gossip.hrl").

%% gen_event callbacks
-export([init/1, handle_event/2, terminate/2, handle_call/2, handle_info/2]).

-export([subscribe/1, unsubscribe/1]).

-define(SERVER, ?MODULE).

-record(state, {
  data_hash :: term(),
  subscribers = #{} :: #{pid() := reference()}
}).

-type state() :: #state{}.

-spec subscribe(pid()) -> ok.
subscribe(Pid) ->
  gen_event:call(simple_gossip_event, ?SERVER, {subscribe, Pid}).

-spec unsubscribe(pid()) -> ok.
unsubscribe(Pid) ->
  gen_event:call(simple_gossip_event, ?SERVER, {unsubscribe, Pid}).

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

%% @private
%% @doc Whenever a new event handler is added to an event manager,
%% this function is called to initialize the event handler.
-spec(init(InitArgs :: term()) ->
  {ok, State :: state()} |
  {ok, State :: state(), hibernate} |
  {error, Reason :: term()}).
init([]) ->
  {ok, #state{}}.

%% @private
%% @doc Whenever an event manager receives an event sent using
%% gen_event:notify/2 or gen_event:sync_notify/2, this function is
%% called for each installed event handler to handle the event.
-spec(handle_event(Event :: term(), State :: state()) ->
  {ok, NewState :: state()} |
  {ok, NewState :: state(), hibernate} |
  {swap_handler, Args1 :: term(), NewState :: state(),
   Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
  remove_handler).
handle_event({reconcile, #rumor{data = Data}}, #state{data_hash = Dh} = State) ->
  DataHash = erlang:phash2(Data),
  case DataHash =/= Dh of
    true ->
      [ Pid ! {data_changed, Data} || Pid <- maps:keys(State#state.subscribers)],
      {ok, State#state{data_hash = DataHash}};
    _ ->
      {ok, State}
  end;
handle_event(_, State = #state{}) ->
  {ok, State}.

-spec handle_call(Request :: term(), State :: state()) ->
  {ok, Reply :: term(), NewState :: state()} |
  {ok, Reply :: term(), NewState :: state(), hibernate} |
  {swap_handler, Reply :: term(), Args1 :: term(), NewState :: state(),
   Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
  {remove_handler, Reply :: term()}.
handle_call({subscribe, Pid}, #state{subscribers = Subscribers} = State) ->
  case maps:get(Pid, Subscribers, not_found) of
    not_found ->
      Ref = monitor(process, Pid),
      {ok, ok, State#state{subscribers = Subscribers#{Pid => Ref}}};
    _ ->
      {ok, ok, State}
  end;
handle_call({unsubscribe, Pid}, #state{subscribers = Subscribers} = State) ->
  case maps:get(Pid, Subscribers, not_found) of
    not_found ->
      {ok, ok, State};
    Ref ->
      demonitor(Ref, [flush]),
      {ok, ok, State#state{subscribers = maps:remove(Pid, Subscribers)}}
  end.

-spec handle_info(Info :: term(), State :: state()) ->
  {ok, NewState :: state()} |
  {ok, NewState :: state(), hibernate} |
  {swap_handler, Args1 :: term(), NewState :: state(),
   Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
  remove_handler.
handle_info({'DOWN', _, process, Pid, _Reason}, State = #state{subscribers = Subscribers}) ->
  {ok, State#state{subscribers = maps:remove(Pid, Subscribers)}}.

%% @private
%% @doc Whenever an event handler is deleted from an event manager, this
%% function is called. It should be the opposite of Module:init/1 and
%% do any necessary cleaning up.
-spec terminate(  Args :: (term() | {stop, Reason :: term()} | stop |
remove_handler | {error, {'EXIT', Reason :: term()}} |
{error, term()}), State :: state()) ->
                 ok.
terminate(_Arg, _State = #state{}) ->
  ok.
