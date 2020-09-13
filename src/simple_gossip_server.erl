%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_server).
-author("Peter Tihanyi").

-behaviour(gen_server).

%% API
-export([start_link/0,

         join/1,
         leave/1,

         set/1,
         get/0,

         status/0,

         subscribe/1,
         unsubscribe/1]).

%% gen_server callbacks
-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-define(SERVER, ?MODULE).

-define(RUMOR(State), State#state.rumor).

-record(rumor, {gossip_version = 1 :: pos_integer(),
                data :: any(),
                leader :: node(),
                nodes = [] :: [node()],
                max_gossip_per_period = 8 :: pos_integer(),
                gossip_period = 10000 :: pos_integer(),
                rumor_record_version = 1 :: pos_integer()
                }).

-type rumor() :: #rumor{}.

-record(state, {rumor = #rumor{} :: rumor,
                subscribers = [] :: list(pid())}).

%%%===================================================================
%%% API
%%%===================================================================
-spec set(Status | fun((Status) -> {change, Status} | no_change)) -> ok when
  Status :: term().
set(Data) ->
  gen_server:call(?SERVER, {set, Data}).

-spec get() -> term().
get() ->
  gen_server:call(?SERVER, get).

-spec subscribe(pid()) -> ok.
subscribe(Pid) ->
  gen_server:call(?SERVER, {subscribe, Pid}).

-spec unsubscribe(pid()) -> ok.
unsubscribe(Pid) ->
  gen_server:call(?SERVER, {unsubscribe, Pid}).

-spec join(node()) -> ok.
join(Node) ->
  gen_server:call(?SERVER, {join, Node}).

-spec leave(node()) -> ok.
leave(Node) ->
  gen_server:call(?SERVER, {leave, Node}).

-spec status() ->
  {ok, Vsn, Leader, Nodes} | {error, timeout} | mismatch when
  Vsn :: pos_integer(),
  Leader :: node(),
  Nodes :: [node()].
status() ->
  gen_server:call(?SERVER, status).

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
-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  Rumor = #rumor{leader = node(), nodes = [node()]},
  State = #state{rumor = Rumor},
  schedule_gossip(Rumor),
  {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call(get, _From, #state{rumor = #rumor{data = Data}} = State) ->
  {reply, Data, State};
handle_call({set, Data}, From, #state{rumor = #rumor{leader = Leader}} = State)
  when Leader =/= node() ->
  case catch gen_server:call({?MODULE, Leader}, {set, Data}) of
    ok ->
      {reply, ok, State};
    {'EXIT', {{nodedown, Leader}, _}} ->
      % It seems leader is unreachable elect this node as the leader
      Rumor = State#state.rumor,
      handle_call({set, Data}, From,
                  State#state{rumor = promote_myself_as_leader(Rumor)})
  end;
handle_call({set, ChangeFun}, _From,
            #state{rumor = #rumor{data = Data} = Rumor} = State)
  when is_function(ChangeFun) ->
  case ChangeFun(Data) of
    {change, NewData} ->
      NewRumor = increase_version(Rumor#rumor{data = NewData}),
      gossip(NewRumor),
      {reply, ok, State#state{rumor = NewRumor}};
    no_change ->
      {reply, ok, State}
  end;
handle_call({set, Data}, _From, #state{rumor = Rumor} = State) ->
  NewRumor = increase_version(Rumor#rumor{data = Data}),
  gossip(NewRumor),
  {reply, ok, State#state{rumor = NewRumor}};
handle_call({join, Node}, _From, #state{rumor = Rumor} = State) ->
  NewRumor = maybe_add_node(Node, Rumor,
    fun(NewRumor) ->
      net_kernel:connect_node(Node),
      NewRumor2 = increase_version(NewRumor),
      gen_server:cast({?MODULE, Node}, {join, node(), NewRumor2}),
      NewRumor2
    end),
  {reply, ok, State#state{rumor = NewRumor}};
handle_call({leave, Node}, _From, #state{rumor = Rumor} = State) ->
  Nodes = Rumor#rumor.nodes,
  NewRumor = Rumor#rumor{nodes = lists:delete(Node, Nodes)},
  NewRumor2 = maybe_promote_random_node_as_leader(NewRumor),
  gossip(NewRumor2),
  {reply, ok, State#state{rumor = NewRumor2}};


handle_call({subscribe, Pid}, _From, #state{subscribers = Subscribers} = State) ->
  case lists:member(Pid, Subscribers) of
    true ->
      {reply, ok, State};
    _ ->
      {reply, ok, State#state{subscribers = [Pid | Subscribers]}}
  end;
handle_call({unsubscribe, Pid}, _From, #state{subscribers = Subscribers} = State) ->
  {reply, ok, State#state{subscribers = lists:delete(Pid, Subscribers)}};

handle_call(status, From, #state{rumor = #rumor{nodes = Nodes,
                                               gossip_version = Version,
                                               leader = Leader}} = State) ->
  spawn(fun() ->
          Ref = make_ref(), Self = self(),
          gen_server:abcast(Nodes, ?MODULE, {get_gossip_version, Self, Ref}),
          Result = case receive_gossip_version(Ref, Nodes, {ok, Version}) of
                      {ok, Ver} ->
                        {ok, Ver, Leader, Nodes};
                      Else ->
                        Else
                    end,
          gen_server:reply(From, Result)
        end),
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast({reconcile,
             #rumor{gossip_version = InVersion} = InRumor},
             #state{rumor = #rumor{gossip_version = Version} = CRumor} = State)
  when InVersion > Version ->
  NewRumor = change_state(CRumor, InRumor),
  gossip(NewRumor),
  notify_subscribers(CRumor, InRumor, State#state.subscribers),
  {noreply, State#state{rumor = NewRumor}};
handle_cast({reconcile, _}, State) ->
  {noreply, State};

handle_cast({get_gossip_version, Requester, Ref},
            State = #state{rumor = #rumor{gossip_version = Vsn}}) ->
  Requester ! {gossip_vsn, Vsn, Ref, node()},
  {noreply, State};

handle_cast({join, _Node, #rumor{gossip_version = InGossipVsn} = InRumor},
            #state{rumor = #rumor{gossip_version = CurrentGossipVsn}} = State)
  when InGossipVsn > CurrentGossipVsn ->
  gossip(InRumor),
  {noreply, State#state{rumor = InRumor}};
handle_cast({join, Node, #rumor{gossip_version = InGossipVsn}},
            #state{rumor = #rumor{gossip_version = CurrentGossipVsn} = Rumor} =
              State)
  when InGossipVsn =< CurrentGossipVsn ->
  NewRumor = increase_version(maybe_add_node(Node, Rumor)),
  gossip(NewRumor, Node),
  {noreply, State#state{rumor = NewRumor}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info(tick, #state{rumor = Rumor} =State) ->
  gossip_random_node(Rumor),
  schedule_gossip(Rumor),
  {noreply, State};
handle_info({nodedown, Node},
            #state{rumor = #rumor{leader = Node} = Rumor} = State) ->
  % Panic: Leader node is down! Every node became leader for itself,
  % and when the next set comes the new leader will be that node
  {noreply, State#state{rumor = promote_myself_as_leader(Rumor)}};
handle_info({nodedown, _}, State) ->
  {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, #state{rumor = #rumor{nodes = Nodes, leader = Leader} =
  Rumor}) when Leader == node() ->
  NewRumor = maybe_promote_random_node_as_leader(
              Rumor#rumor{nodes = lists:delete(node(), Nodes)}),
  gossip(NewRumor),
  ok;
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
gossip(#rumor{nodes = []}) ->
  ok;
gossip(#rumor{nodes = Nodes, max_gossip_per_period = Max} = State) ->
  NewNodes = lists:delete(node(), Nodes),
  RandomNodes = pick_random_nodes(NewNodes, Max),
  [gossip(State, Node) || Node <- RandomNodes].

gossip_random_node(#rumor{nodes = []}) ->
  ok;
gossip_random_node(#rumor{nodes = Nodes} = State) ->
  NewNodes = lists:delete(node(), Nodes),
  RandomNodes = pick_random_nodes(NewNodes, 1),
  [gossip(State, Node) || Node <- RandomNodes],
  ok.

gossip(State, Node) ->
  gen_server:cast({?MODULE, Node}, {reconcile, State}).

pick_random_nodes(Nodes, Number) ->
  pick_random_nodes(Nodes, Number, []).

pick_random_nodes(Nodes, Number, Acc) when Nodes == [] orelse Number == 0 ->
  Acc;
pick_random_nodes(Nodes, Number, Acc) ->
  Node = lists:nth(rand:uniform(length(Nodes)), Nodes),
  pick_random_nodes(lists:delete(Node, Nodes), Number-1, [Node | Acc]).

schedule_gossip(#rumor{gossip_period = Period}) ->
  erlang:send_after(Period, ?SERVER, tick).

increase_version(#rumor{gossip_version = GossipVersion} = Rumor) ->
  Rumor#rumor{gossip_version = GossipVersion+1}.

receive_gossip_version(_, [], Vsn) ->
  Vsn;
receive_gossip_version(Ref, [Node | Nodes], {ok, Vsn}) ->
  receive
    {gossip_vsn, Vsn, Ref, Node} ->
      receive_gossip_version(Ref, Nodes, {ok, Vsn});
    {gossip_vsn, _, Ref, Node} ->
      receive_gossip_version(Ref, Nodes, mismatch)
  after 100 ->
    {error, timeout}
  end;
receive_gossip_version(_, _, Result) ->
  Result.


change_state(#rumor{leader = Leader},
             #rumor{leader = Leader} = NewRumor) ->
  NewRumor;
change_state(#rumor{leader = OldLeader},
             #rumor{leader = NewLeader} = NewRumor) ->
  erlang:monitor_node(NewLeader, true),
  erlang:monitor_node(OldLeader, false),
  NewRumor.

notify_subscribers(_, _, []) ->
  ok;
notify_subscribers(#rumor{data = Data}, #rumor{data = Data}, _) ->
  ok;
notify_subscribers(#rumor{data = _}, #rumor{data = NewData}, Subscribers) ->
  [ Pid ! {data_changed, NewData} || Pid <- Subscribers].

promote_myself_as_leader(Rumor) ->
  increase_version(Rumor#rumor{leader = node()}).

maybe_promote_random_node_as_leader(#rumor{nodes = Nodes} = Rumor)
  when Nodes =/= [] ->
  [NewLeader] = pick_random_nodes(Nodes, 1),
  increase_version(Rumor#rumor{leader = NewLeader});
maybe_promote_random_node_as_leader(Rumor) ->
  Rumor.

maybe_add_node(Node, State) ->
  maybe_add_node(Node, State, fun(NewState) -> NewState end).

maybe_add_node(Node, #rumor{nodes = Nodes} = Rumor, Fun) ->
  case lists:member(Node, Nodes) of
    true ->
      Rumor;
    _ ->
      Fun(Rumor#rumor{nodes = [Node | Nodes]})
  end.
