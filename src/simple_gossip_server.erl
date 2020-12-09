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

-include("simple_gossip.hrl").

-define(DEBUG(Msg, Args),
  get(debug_pid) ! {out, "~n * [~p] ~p " ++ Msg, [node(), os:timestamp() | Args]}).

%-define(DEBUG(Msg, Args), ok).

%% API
-export([start_link/0,

         join/1,
         leave/1,

         set/1,
         get/0,

         status/0, set_debug/1]).

%% gen_server callbacks
-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2]).

-define(SERVER, ?MODULE).

-define(RUMOR(State), State#state.rumor).

-record(state, {rumor = #rumor{} :: rumor(),
                rumor_record_version = 1 :: pos_integer()
}).

-type state() :: #state{}.

-type set_fun() ::  fun((term()) -> {change, term()} | no_change).

%%%===================================================================
%%% API
%%%===================================================================


set_debug(Pid) ->
  gen_server:call(?SERVER, {debug, Pid}).

-spec set(term() | set_fun()) -> ok.
set(Data) ->
  gen_server:call(?SERVER, {set, Data}).

-spec get() -> term().
get() ->
  gen_server:call(?SERVER, get).

-spec join(node()) -> ok.
join(Node) ->
  gen_server:call(?SERVER, {join, Node}).

-spec leave(node()) -> ok.
leave(Node) ->
  gen_server:call(?SERVER, {leave, Node}).

-spec status() -> {ok, Vsn, Leader, Nodes} |
                  {error, {timeout, Nodes} | mismatch, Leader, Nodes} when
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
  {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  put(debug_pid, user),
  Rumor =
    case simple_gossip_persist:get() of
      {ok, StoredRumor} ->
        ?DEBUG("Stored rumor read", []),
        StoredRumor;
      {error, undefined} ->
        ?DEBUG("No stored rumor", []),
        simple_gossip_rumor:new()
    end,
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
    State :: state()) ->
  {reply, Reply :: term(), NewState :: state()} |
  {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
  {noreply, NewState :: state()} |
  {noreply, NewState :: state(), timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
  {stop, Reason :: term(), NewState :: state()}).
handle_call({debug, Pid}, _From, #state{rumor = #rumor{data = Data}} = State) ->
  put(debug_pid, Pid),
  {reply, Data, State};
handle_call(get, _From, #state{rumor = #rumor{data = Data}} = State) ->
  {reply, Data, State};
handle_call({set, Data}, From, State) ->
  proxy_call({set, Data}, From, State);
handle_call(status, From, #state{rumor = #rumor{nodes = Nodes,
                                                gossip_version = Version,
                                                leader = Leader}} = State) ->
  spawn(fun() ->
          Ref = make_ref(),
          Self = self(),
          gen_server:abcast(Nodes, ?MODULE, {get_gossip_version, Self, Ref}),
          Result = receive_gossip_version(Ref, Nodes, Version, Leader),
          gen_server:reply(From, Result)
        end),
  {noreply, State};
handle_call({join_request, #rumor{gossip_version = InGossipVsn} = InRumor},
            _From,
            #state{rumor = #rumor{gossip_version = CurrentGossipVsn}} = State)
  when InGossipVsn >= CurrentGossipVsn ->
  {FromPid, _} = _From,
  ?DEBUG("Join request from ~p. Remote rumor (~p) is greater than the current one (~p), apply it",
         [node(FromPid), InGossipVsn, CurrentGossipVsn]),
  gossip(InRumor),
  simple_gossip_event:notify(InRumor),
  {reply, ok, State#state{rumor = InRumor}};
handle_call({join_request, #rumor{gossip_version = InGossipVsn}}, {FromPid, _},
             #state{rumor = #rumor{gossip_version = CurrentGossipVsn} = Rumor} =
              State) when InGossipVsn < CurrentGossipVsn ->
  Node = node(FromPid),
  NewRumor = simple_gossip_rumor:add_node(Rumor, Node),
  io:format("upgrade nodes: ~p~n", [NewRumor#rumor.nodes]),
  gossip(NewRumor, Node),
  ?DEBUG("Join request from ~p. Current rumor (~p) is greater than the remote (~p). Ask remote to upgrade",
         [Node, CurrentGossipVsn, InGossipVsn]),
  {reply, {upgrade, NewRumor}, State#state{rumor = NewRumor}};

handle_call(Command, From, State) ->
  proxy_call(Command, From, State).

-spec(handle_command(Request :: term(), From :: {pid(), Tag :: term()},
                  State :: state()) ->
                   {reply, Reply :: term(), NewState :: state()} |
                   {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
                   {noreply, NewState :: state()} |
                   {noreply, NewState :: state(), timeout() | hibernate} |
                   {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
                   {stop, Reason :: term(), NewState :: state()}).
handle_command({set, ChangeFun}, _From, #state{rumor = #rumor{data = Data} = Rumor} = State)
  when is_function(ChangeFun) ->
  case ChangeFun(Data) of
    {change, NewData} ->
      NewRumor = simple_gossip_rumor:set_data(Rumor, NewData),
      ?DEBUG("Set data: ~p vsn: ~p", [erlang:phash2(NewData, 9999),
                                      NewRumor#rumor.gossip_version]),
      gossip(NewRumor),
      simple_gossip_event:notify(NewRumor),
      {reply, ok, State#state{rumor = NewRumor}};
    no_change ->
      {reply, ok, State}
  end;
handle_command({set, Data}, _From, #state{rumor = Rumor} = State) ->
  NewRumor = simple_gossip_rumor:set_data(Rumor, Data),
  ?DEBUG("Set data: ~p vsn: ~p", [erlang:phash2(Data, 9999),
                                  NewRumor#rumor.gossip_version]),
  gossip(NewRumor),
  simple_gossip_event:notify(NewRumor),
  {reply, ok, State#state{rumor = NewRumor}};

handle_command({join, Node}, _From, #state{rumor = Rumor} = State) ->
  NewRumor = simple_gossip_rumor:if_not_member(Rumor, Node,
                                               fun() -> join_node(Rumor, Node) end),
  gossip(NewRumor),
  {reply, ok, State#state{rumor = NewRumor}};

handle_command({leave, Node}, _From,
               #state{rumor = #rumor{nodes = [Node]}} = State) ->
  {reply, ok, State};

% leader node leaves which is the current node
handle_command({leave, Node}, _From, #state{rumor = #rumor{leader = Node} = Rumor} = State)
  when Node == node() ->
  ?DEBUG("Leaving ~p Leader: ~p", [Node, Node]),
  ?DEBUG("Nodes in rumor: ~p", [Rumor#rumor.nodes]),
  OthersRumor = simple_gossip_rumor:remove_node(Rumor, Node),
  gossip(OthersRumor),
  ?DEBUG("New rumor's Nodes in rumor: ~p Leader: ~p",
            [OthersRumor#rumor.nodes, OthersRumor#rumor.leader]),
  MyRumor = simple_gossip_rumor:new(),
  simple_gossip_event:notify(MyRumor),
  {reply, ok, State#state{rumor = MyRumor}};

% leader node leaves but, not this node
handle_command({leave, Node}, _From, #state{rumor = #rumor{leader = Node} = Rumor} = State)
  when Node =/= node() ->
  ?DEBUG("Leaving ~p Leader: ~p~n", [Node, Node]),
  NewRumor = simple_gossip_rumor:remove_node(Rumor, Node),
  %simple_gossip_rumor:if_member(Rumor, Node, fun() -> gossip(NewRumor, Node), NewRumor end),
  gossip(NewRumor),
  {reply, ok, State#state{rumor = NewRumor}};

handle_command({leave, Node}, _From,
               #state{rumor = Rumor} = State) ->
  ?DEBUG("Leaving ~p Leader: ~p~n", [Node, State#state.rumor#rumor.leader]),
  NewRumor = simple_gossip_rumor:remove_node(Rumor, Node),
  simple_gossip_rumor:if_member(Rumor, Node,
                                fun() -> gossip(NewRumor, Node), NewRumor end),
  gossip(NewRumor),
  {reply, ok, State#state{rumor = NewRumor}}.

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
handle_cast({reconcile,
             #rumor{gossip_version = InVersion} = InRumor,
             SenderNode},
             #state{rumor = #rumor{gossip_version = Version} = CRumor} = State)
  when InVersion > Version ->
  case simple_gossip_rumor:check_vector_clocks(InRumor, CRumor) of
    true ->
      NewRumor = check_leader_change(CRumor, simple_gossip_rumor:check_node_exclude(InRumor)),
      GossipNodes = lists:delete(SenderNode, lists:delete(node(), NewRumor#rumor.nodes)),
      gossip(NewRumor, GossipNodes),
      simple_gossip_event:notify(NewRumor),
      ?DEBUG("New gossip ~p From ~p", [NewRumor#rumor.gossip_version, SenderNode]),
      {noreply, State#state{rumor = NewRumor}};
    _ ->
      ?DEBUG("Dropped newer gossip due to vclock check", []),
      {noreply, State}
  end;
handle_cast({reconcile, _Rumor, _SenderNode}, State) ->
  {noreply, State};

handle_cast({get_gossip_version, Requester, Ref},
            State = #state{rumor = #rumor{gossip_version = Vsn}}) ->
  Requester ! {gossip_vsn, Vsn, Ref, node()},
  {noreply, State}.

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
-spec(handle_info(Info :: timeout() | term(), State :: state()) ->
  {noreply, NewState :: state()} |
  {noreply, NewState :: state(), timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: state()}).
handle_info(tick, #state{rumor = Rumor} =State) ->
  gossip_random_node(Rumor),
  schedule_gossip(Rumor),
  {noreply, State};
handle_info({nodedown, Node},
            #state{rumor = #rumor{leader = Node} = Rumor} = State) ->
  % Panic: Leader node is down!
  ?DEBUG("Leader node is down ~p", [Node]),
  {noreply, State#state{rumor = simple_gossip_rumor:change_leader(Rumor)}};
handle_info({nodedown, _Node}, State) ->
  ?DEBUG("Node is down ~p", [_Node]),
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
    State :: state()) -> term()).
terminate(_Reason, #state{rumor = #rumor{leader = Leader} = Rumor})
  when Leader == node() ->
  gossip(simple_gossip_rumor:remove_node(Rumor, Leader)),
  ok;
terminate(_Reason, _State) ->
  ok.

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
  RandomNodes = simple_gossip_rumor:pick_random_nodes(NewNodes, 1),
  gossip(Rumor, RandomNodes).

-spec gossip(rumor(), node() | [node()]) -> ok.
gossip(#rumor{max_gossip_per_period = Max} = Rumor, Nodes) when is_list(Nodes) ->
  RandomNodes = simple_gossip_rumor:pick_random_nodes(Nodes, Max),
  [gossip(Rumor, Node) || Node <- RandomNodes],
  ok;
gossip(Rumor, Node) ->
  gen_server:cast({?MODULE, Node}, {reconcile, Rumor, node()}).

-spec schedule_gossip(rumor()) -> reference().
schedule_gossip(#rumor{gossip_period = Period}) ->
  erlang:send_after(Period, ?SERVER, tick).

-spec receive_gossip_version(reference(), [node()], pos_integer(), node()) ->
  {ok, pos_integer(), node(), [node()]} | {error, term(), node(), [node()]}.
receive_gossip_version(Ref, Nodes, Version, Leader) ->
  case receive_gossip_version(Ref, Nodes, Version) of
    {ok, Ver} ->
      {ok, Ver, Leader, Nodes};
    {error, Else} ->
      {error, Else, Leader, Nodes}
  end.

-spec receive_gossip_version(reference(), [node()], Result) ->
  FinalResult when
  Version :: pos_integer(),
  Result :: {ok, Version},
  FinalResult :: Result | {error, {timeout, [node()]} | mismatch}.
receive_gossip_version(_, [], Vsn) ->
  {ok, Vsn};
receive_gossip_version(Ref, Nodes, Vsn) ->
  receive
    {gossip_vsn, Vsn, Ref, Node} ->
      receive_gossip_version(Ref, lists:delete(Node, Nodes), Vsn);
    {gossip_vsn, _MismatchVsn, Ref, _Node} ->
      {error, mismatch}
  after 1024 ->
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

proxy_call(Command, From, #state{rumor = #rumor{leader = Leader} = Rumor} = State)
  when Leader =/= node() ->
    case call({?MODULE, Leader}, Command, 4000) of
      {ok, {'EXIT', {{nodedown, Leader}, _}}} ->
        ?DEBUG("Proxy call nodedown ~p", [Leader]),
        proxy_call(Command, From, State#state{rumor = simple_gossip_rumor:change_leader(Rumor)});
      {ok, {'EXIT', {noproc, _}}} ->
        ?DEBUG("Proxy call noproc ~p", [Leader]),
        proxy_call(Command, From, State#state{rumor = simple_gossip_rumor:change_leader(Rumor)});
      {ok, Else} ->
        {reply, Else, State};
      timeout ->
        {reply, timeout, State}
    end;
proxy_call(Command, From, State) ->
  handle_command(Command, From, State).

-spec join_node(rumor(), node()) -> rumor().
join_node(Rumor, Node) ->
  NewRumor = simple_gossip_rumor:add_node(Rumor, Node),
  net_kernel:connect_node(Node),

  case call({?MODULE, Node}, {join_request, NewRumor}, 3000) of
    {ok, {upgrade, UpgradeRumor}} ->
      ?DEBUG("Rumor upgraded from remote ~p ~p", [Node, UpgradeRumor#rumor.gossip_version]),
      UpgradeRumor;
    _ ->
      NewRumor
  end.

% prevent late responses
-spec call(ServerRef, term(), pos_integer()) -> {ok, term()} | timeout when
  ServerRef ::
    Name | {Name, node()} | {global, Name} | {via, module(), Name} | pid(),
  Name :: term().
call(Name, Request, Timeout) when Timeout > 10 ->
  Self = self(),
  Key = '$_prevent_late_msgs$',
  Pid = spawn(fun() ->
                CallTimeout = Timeout-10,
                Self ! {Key, self(), catch gen_server:call(Name, Request, CallTimeout)}
              end),
  receive {Key, Pid, Msg} -> {ok, Msg}
  after Timeout ->
    exit(Pid, kill),
    receive
      {Key, Pid, Msg} ->
        {ok, Msg}
    after 10 ->
      timeout
    end
  end.