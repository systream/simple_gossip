%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_event).

-behaviour(gen_server).

-include("simple_gossip.hrl").

%% API
-export([start_link/0,
         notify/1,
         subscribe/1,
         subscribe/2,
         unsubscribe/1
]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-define(SERVER, ?MODULE).

-record(state, {
    rumor_hash :: term(),
    data_hash :: term(),
    subscribers = #{} :: #{pid() := {data | rumor, reference()}}
}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================
-spec notify(rumor()) -> ok | timeout.
notify(Rumor) ->
  case get_mode() of
    async ->
      gen_server:cast(?SERVER, {notify, Rumor});
    sync ->
      case simple_gossip_call:call(?SERVER, {notify, Rumor}, 4000) of
        {ok, ok} ->
          ok;
        Else ->
          Else
      end
  end.

-spec subscribe(pid()) -> ok.
subscribe(Pid) ->
  subscribe(Pid, data).

-spec subscribe(pid(), Type :: data | rumor) -> ok.
subscribe(Pid, Type) when Type =:= data orelse Type =:= rumor ->
  gen_server:call(?SERVER, {subscribe, Pid, Type}).

-spec unsubscribe(pid()) -> ok.
unsubscribe(Pid) ->
  gen_server:call(?SERVER, {unsubscribe, Pid}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec init(term()) -> {ok, state()}.
init(_) ->
  {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
                  State :: state()) ->
                   {reply, Reply :: term(), NewState :: state()} |
                   {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
                   {noreply, NewState :: state()} |
                   {noreply, NewState :: state(), timeout() | hibernate} |
                   {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
                   {stop, Reason :: term(), NewState :: state()}).
handle_call({notify, Rumor}, _From, #state{} = State) ->
  {reply, ok, handle_notify(Rumor, State)};
handle_call({subscribe, Pid, Type}, _From, #state{subscribers = Subscribers} =
  State) ->
  check_mode(),
  case maps:get(Pid, Subscribers, not_found) of
    not_found ->
      Ref = monitor(process, Pid),
      {reply, ok, State#state{subscribers = Subscribers#{Pid => {Type, Ref}}}};
    _ ->
      {reply, ok, State}
  end;
handle_call({unsubscribe, Pid}, _From, #state{subscribers = Subscribers} = State) ->
  check_mode(),
  case maps:get(Pid, Subscribers, not_found) of
    not_found ->
      {reply, ok, State};
    {_Type, Ref} ->
      demonitor(Ref, [flush]),
      {reply, ok, State#state{subscribers = maps:remove(Pid, Subscribers)}}
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: state()) ->
  {noreply, NewState :: state()} |
  {noreply, NewState :: state(), timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: state()}).
handle_cast({notify, Rumor}, State) ->
  {noreply, handle_notify(Rumor, State)}.

-spec handle_info(Info :: term(), State :: state()) ->
  {noreply, NewState :: state()} |
  {noreply, NewState :: state(), hibernate}.
handle_info({'DOWN', _, process, Pid, _Reason},
            #state{subscribers = Subscribers} = State) ->
  check_mode(),
  {noreply, State#state{subscribers = maps:remove(Pid, Subscribers)}}.

-spec is_data_changed(term(), term()) -> {boolean(), term()}.
is_data_changed(Data, Hash) ->
  NewHash = erlang:phash2(Data),
  {NewHash =/= Hash, NewHash}.

-spec handle_notify(rumor(), state()) -> state().
handle_notify(Rumor, #state{rumor_hash = Rh, data_hash = Dh} = State) ->
  check_mode(),
  Data = Rumor#rumor.data,
  {IsRumorChanged, NewRh} = is_data_changed(Rumor, Rh),
  {IsDataChanged, NewDh} = is_data_changed(Rumor#rumor.data, Dh),

  NewState = State#state{rumor_hash = NewRh, data_hash = NewDh},

  case {IsDataChanged, IsRumorChanged} of
    {true, _} ->
      %% notify everybody
      maps:map(fun(Pid, {data, _}) ->
                    Pid ! {data_changed, Data};
                  (Pid, {rumor, _}) ->
                    Pid ! {rumor_changed, Rumor}
               end,
               State#state.subscribers);
    {false, true} ->
      % only rumor
      maps:map(fun(_Pid, {data, _}) ->
                    ok;
                  (Pid, {rumor, _}) ->
                    Pid ! {rumor_changed, Rumor}
               end,
               State#state.subscribers);
    _ ->
      % nobody
      ok
  end,
  NewState.

-spec set_mode(async | sync) -> ok.
set_mode(Mode) when Mode == async orelse Mode == sync ->
  persistent_term:put({?SERVER, mode}, Mode).

-spec get_mode() -> sync | async.
get_mode() ->
  persistent_term:get({?SERVER, mode}, async).

-spec check_mode() -> ok.
check_mode() ->
  {message_queue_len, MsgQueueLen} =
    erlang:process_info(self(), message_queue_len),
  case get_mode() of
    async when MsgQueueLen > 3 ->
      set_mode(sync);
    sync when MsgQueueLen =< 1 ->
      set_mode(async);
    _ ->
      ok
  end.
