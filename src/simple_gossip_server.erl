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
                gossip_period = 10000 :: pos_integer()
                }).

-type rumor() :: #rumor{}.

-record(state, {rumor = #rumor{} :: rumor(),
                subscribers = #{} :: #{pid() := reference()},
                rumor_record_version = 1 :: pos_integer()
}).

-type set_fun() ::  fun((term()) -> {change, term()} | no_change).

%%%===================================================================
%%% API
%%%===================================================================
-spec set(term() | set_fun()) -> ok.
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
  {ok, Vsn, Leader, Nodes} | {error, {timeout, Nodes} | mismatch} when
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
  Rumor = init_rumor(),
  State = #state{rumor = Rumor},
  schedule_gossip(Rumor),
  {ok, State}.

-spec init_rumor() -> rumor().
init_rumor() ->
  init_rumor(1).

-spec init_rumor(pos_integer()) -> rumor().
init_rumor(GossipVsn) ->
  #rumor{leader = node(),
         nodes = [node()],
         gossip_version = GossipVsn}.

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
handle_call({set, Data}, From, State) ->
  proxy_call({set, Data}, From, State);
handle_call({subscribe, Pid}, _From, #state{subscribers = Subscribers} = State) ->
  case maps:get(Pid, Subscribers, not_found) of
    not_found ->
      Ref = monitor(process, Pid),
      {reply, ok, State#state{subscribers = Subscribers#{Pid => Ref}}};
    _ ->
      {reply, ok, State}
  end;
handle_call({unsubscribe, Pid}, _From, #state{subscribers = Subscribers} = State) ->
  case maps:get(Pid, Subscribers, not_found) of
    not_found ->
      {reply, ok, State};
    Ref ->
      demonitor(Ref, [flush]),
      {reply, ok, State#state{subscribers = maps:remove(Pid, Subscribers)}}
  end;

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
  {noreply, State};
handle_call(Command, From, State) ->
  proxy_call(Command, From, State).


-spec(handle_command(Request :: term(), From :: {pid(), Tag :: term()},
                  State :: #state{}) ->
                   {reply, Reply :: term(), NewState :: #state{}} |
                   {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
                   {noreply, NewState :: #state{}} |
                   {noreply, NewState :: #state{}, timeout() | hibernate} |
                   {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
                   {stop, Reason :: term(), NewState :: #state{}}).
handle_command({set, ChangeFun}, _From,
            #state{rumor = #rumor{data = Data} = Rumor} = State)
  when is_function(ChangeFun) ->
  case ChangeFun(Data) of
    {change, NewData} ->
      NewRumor = increase_version(Rumor#rumor{data = NewData}),
      gossip(NewRumor),
      notify_subscribers(Rumor, NewRumor, maps:keys(State#state.subscribers)),
      {reply, ok, State#state{rumor = NewRumor}};
    no_change ->
      {reply, ok, State}
  end;
handle_command({set, Data}, _From, #state{rumor = Rumor} = State) ->
  NewRumor = increase_version(Rumor#rumor{data = Data}),
  gossip(NewRumor),
  notify_subscribers(Rumor, NewRumor, maps:keys(State#state.subscribers)),
  {reply, ok, State#state{rumor = NewRumor}};

handle_command({join, Node}, _From, #state{rumor = Rumor} = State) ->
  NewRumor = maybe_add_node(Node, Rumor,
                            fun(NewRumor) ->
                              net_kernel:connect_node(Node),
                              NewRumor2 = increase_version(NewRumor),
                              gen_server:cast({?MODULE, Node}, {join, node(), NewRumor2}),
                              NewRumor2
                            end),
  {reply, ok, State#state{rumor = NewRumor}};



handle_command({leave, Node}, _From,
               #state{rumor = #rumor{leader = Node, nodes = Nodes} = Rumor} =
                 State) when Node == node() ->
  GlobalRumor = Rumor#rumor{nodes = lists:delete(Node, Nodes)},
  GlobalRumor2 = maybe_promote_random_node_as_leader(GlobalRumor),
  GlobalRumor3 = increase_version(GlobalRumor2),
  gossip(GlobalRumor3),
  MyRumor = increase_version(init_rumor(Rumor#rumor.gossip_version)),
  {reply, ok, State#state{rumor = MyRumor}};

handle_command({leave, Node}, _From,
               #state{rumor = #rumor{leader = Node, nodes = Nodes} = Rumor} =
                 State) when Node =/= node() ->
  NewRumor = Rumor#rumor{nodes = lists:delete(Node, Nodes)},
  NewRumor2 = increase_version(promote_myself_as_leader(NewRumor)),
  gossip(NewRumor2, Node), % send the new state to the leaving node
  gossip(NewRumor2), % normal gossip
  {reply, ok, State#state{rumor = NewRumor2}};

handle_command({leave, Node}, _From,
               #state{rumor = #rumor{nodes = Nodes} = Rumor} = State) ->
  NewRumor = increase_version(Rumor#rumor{nodes = lists:delete(Node, Nodes)}),
  gossip(NewRumor),
  {reply, ok, State#state{rumor = NewRumor}}.

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
             #rumor{gossip_version = InVersion} = InRumor,
             SenderNode},
             #state{rumor = #rumor{gossip_version = Version} = CRumor} = State)
  when InVersion > Version ->
  NewRumor = check_leader_change(CRumor, check_node_exclude(InRumor)),
  GossipNodes = lists:delete(SenderNode, lists:delete(node(), NewRumor#rumor.nodes)),
  gossip(NewRumor, GossipNodes),
  notify_subscribers(CRumor, NewRumor, maps:keys(State#state.subscribers)),
  {noreply, State#state{rumor = NewRumor}};
handle_cast({reconcile, _Rumor, _SenderNode}, State) ->
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
  % Panic: Leader node is down!
  NewRumor = maybe_promote_random_node_as_leader(Rumor),
  {noreply, State#state{rumor = increase_version(NewRumor)}};
handle_info({nodedown, _}, State) ->
  {noreply, State};
handle_info({'DOWN', _, process, Pid, _Reason},
            State = #state{subscribers = Subscribers}) ->
  {noreply, State#state{subscribers = maps:remove(Pid, Subscribers)}}.


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
terminate(_Reason, #state{rumor = #rumor{leader = Leader, nodes = Nodes} = Rumor})
  when Leader == node() ->
  NewRumor = Rumor#rumor{nodes = lists:delete(Leader, Nodes)},
  gossip(maybe_promote_random_node_as_leader(NewRumor)),
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
-spec gossip(rumor()) -> ok.
gossip(#rumor{nodes = Nodes} = Rumor) ->
  NewNodes = lists:delete(node(), Nodes),
  gossip(Rumor, NewNodes).

-spec gossip_random_node(rumor()) -> ok.
gossip_random_node(#rumor{nodes = []}) ->
  ok;
gossip_random_node(#rumor{nodes = Nodes} = Rumor) ->
  NewNodes = lists:delete(node(), Nodes),
  RandomNodes = pick_random_nodes(NewNodes, 1),
  gossip(Rumor, RandomNodes).

-spec gossip(rumor(), node() | [node()]) -> ok.
gossip(#rumor{max_gossip_per_period = Max} = Rumor, Nodes) when is_list(Nodes) ->
  RandomNodes = pick_random_nodes(Nodes, Max),
  [gossip(Rumor, Node) || Node <- RandomNodes],
  ok;
gossip(Rumor, Node) ->
  gen_server:cast({?MODULE, Node}, {reconcile, Rumor, node()}).

-spec pick_random_nodes([node()], non_neg_integer()) -> [node()].
pick_random_nodes(Nodes, Number) ->
  pick_random_nodes(Nodes, Number, []).

-spec pick_random_nodes([node()], non_neg_integer(), [node()]) -> [node()].
pick_random_nodes(Nodes, Number, Acc) when Nodes == [] orelse Number == 0 ->
  Acc;
pick_random_nodes(Nodes, Number, Acc) ->
  Node = lists:nth(rand:uniform(length(Nodes)), Nodes),
  pick_random_nodes(lists:delete(Node, Nodes), Number-1, [Node | Acc]).

-spec schedule_gossip(rumor()) -> reference().
schedule_gossip(#rumor{gossip_period = Period}) ->
  erlang:send_after(Period, ?SERVER, tick).

-spec increase_version(rumor()) -> rumor().
increase_version(#rumor{gossip_version = GossipVersion} = Rumor) ->
  Rumor#rumor{gossip_version = GossipVersion+1}.

-spec receive_gossip_version(reference(), [node()], Result) ->
  FinalResult when
  Version :: pos_integer(),
  Result :: {ok, Version},
  FinalResult :: Result | {error, {timeout, [node()]} | mismatch}.
receive_gossip_version(_, [], Vsn) ->
  Vsn;
receive_gossip_version(Ref, Nodes, {ok, Vsn}) ->
  receive
    {gossip_vsn, Vsn, Ref, Node} ->
      receive_gossip_version(Ref, lists:delete(Node, Nodes), {ok, Vsn});
    {gossip_vsn, _, Ref, _Node} ->
      {error, mismatch}
  after 1000 ->
    {error, {timeout, Nodes}}
  end.

-spec check_leader_change(Current :: rumor(), New :: rumor()) -> rumor().
check_leader_change(#rumor{leader = Leader},
                    #rumor{leader = Leader} = NewRumor) ->
  NewRumor;
check_leader_change(#rumor{leader = OldLeader},
                    #rumor{leader = NewLeader} = NewRumor) ->
  erlang:monitor_node(NewLeader, true),
  erlang:monitor_node(OldLeader, false),
  NewRumor.

-spec notify_subscribers(Current :: rumor(), New :: rumor(), [pid()]) -> ok.
notify_subscribers(_, _, []) ->
  ok;
notify_subscribers(#rumor{data = Data}, #rumor{data = Data}, _) ->
  ok;
notify_subscribers(#rumor{data = _}, #rumor{data = NewData}, Subscribers) ->
  [ Pid ! {data_changed, NewData} || Pid <- Subscribers],
  ok.

-spec promote_myself_as_leader(rumor()) -> rumor().
promote_myself_as_leader(Rumor) ->
  Rumor#rumor{leader = node()}.

-spec maybe_promote_random_node_as_leader(rumor()) -> rumor().
maybe_promote_random_node_as_leader(#rumor{nodes = Nodes} = Rumor)
  when Nodes =/= [] ->
  NewLeader = lists:nth(erlang:phash(Rumor, length(Nodes)), Nodes),
  Rumor#rumor{leader = NewLeader};
maybe_promote_random_node_as_leader(Rumor) ->
  Rumor.

-spec maybe_add_node(node(), rumor()) -> rumor().
maybe_add_node(Node, Rumor) ->
  maybe_add_node(Node, Rumor, fun(NewRumor) -> NewRumor end).

-spec maybe_add_node(node(), rumor(), set_fun()) -> rumor().
maybe_add_node(Node, #rumor{nodes = Nodes} = Rumor, Fun) ->
  case lists:member(Node, Nodes) of
    true ->
      Rumor;
    _ ->
      Fun(Rumor#rumor{nodes = [Node | Nodes]})
  end.

-spec check_node_exclude(rumor()) -> rumor().
check_node_exclude(#rumor{nodes = Nodes} = Rumor) ->
  case lists:member(node(), Nodes) of
    true ->
      Rumor;
    _ ->
      init_rumor(Rumor#rumor.gossip_version+1)
  end.

proxy_call(Command, From, #state{rumor = #rumor{leader = Leader}} = State)
  when Leader =/= node() ->
    case catch gen_server:call({?MODULE, Leader}, Command) of
      {'EXIT', {{nodedown, Leader}, _}} ->
        % It seems leader is unreachable elect this node as the leader
        Rumor = increase_version(promote_myself_as_leader(State#state.rumor)),
        gossip(Rumor),
        handle_command(Command, From, State#state{rumor = Rumor});
      {'EXIT', {noproc, _}} ->
        % It seems leader is unreachable elect this node as the leader
        Rumor = increase_version(promote_myself_as_leader(State#state.rumor)),
        gossip(Rumor),
        handle_command(Command, From, State#state{rumor = Rumor});
      Else ->
        {reply, Else, State}
    end;
proxy_call(Command, From, State) ->
  handle_command(Command, From, State).
