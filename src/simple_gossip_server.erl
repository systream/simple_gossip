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
-include_lib("kernel/include/logger.hrl").

-define(STATUS_RECEIVE_TIMEOUT, 1024).
-define(PROXY_CALL_TIMEOUT, 2000).

%% API
-export([start_link/0,

         join/1,
         leave/1,

         set/1,
         get/0,

         status/0,

         set_gossip_period/1]).

%% gen_server callbacks
-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2]).

-define(SERVER, ?MODULE).

-define(RUMOR(State), State#state.rumor).

-record(state, {rumor = #rumor{} :: rumor(),
                rumor_record_version = 1 :: pos_integer(),
                timer_ref :: reference() | undefined
}).

-type state() :: #state{}.

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

-spec join(node()) -> ok.
join(Node) ->
  gen_server:call(?SERVER, {join, Node}).

-spec leave(node()) -> ok.
leave(Node) ->
  gen_server:call(?SERVER, {leave, Node}).

-spec status() -> {ok, Vsn, Leader, Nodes} |
                  {error, {timeout, Nodes} | gossip_vsn_mismatch, Leader, Nodes}
              when
  Vsn :: pos_integer(),
  Leader :: node(),
  Nodes :: [node()].
status() ->
  gen_server:call(?SERVER, status).

-spec set_gossip_period(pos_integer()) -> ok.
set_gossip_period(Period) ->
  gen_server:call(?SERVER, {change_period, Period}).

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
  process_flag(trap_exit, true),
  ?LOG_INFO("Starting gossip service", []),
  Rumor =
    case simple_gossip_persist:get() of
      {ok, StoredRumor} ->
        ?LOG_DEBUG("Read rumor from file", []),
        StoredRumor;
      {error, undefined} ->
        ?LOG_DEBUG("No rumor file found", []),
        simple_gossip_rumor:new()
    end,
  State = #state{rumor = Rumor},
  {ok, reschedule_gossip(State)}.

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
  {_FromPid, _} = _From,
  ?LOG_INFO(
    "Join request from ~p. Remote rumor (~p) is greater than the current (~p)" ++
    " applying it", [node(_FromPid), InGossipVsn, CurrentGossipVsn]),
  gossip(InRumor),
  simple_gossip_event:notify(InRumor),
  {reply, ok, reschedule_gossip(State#state{rumor = InRumor})};
handle_call({join_request, #rumor{gossip_version = InGossipVsn}}, {FromPid, _},
             #state{rumor = #rumor{gossip_version = CurrentGossipVsn} = Rumor} =
              State) when InGossipVsn < CurrentGossipVsn ->
  Node = node(FromPid),
  NewRumor = simple_gossip_rumor:add_node(Rumor, Node),
  gossip(NewRumor, Node),
  ?LOG_INFO(
    "Join request from ~p. Current rumor (~p) is greater than the remote (~p)." ++
    " Asking remote to upgrade", [Node, CurrentGossipVsn, InGossipVsn]),
  simple_gossip_event:notify(NewRumor),
  {reply, {upgrade, NewRumor}, reschedule_gossip(State#state{rumor = NewRumor})};

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
      ?LOG_DEBUG("Set data: ~p vsn: ~p",
                 [erlang:phash2(NewData, 9999), NewRumor#rumor.gossip_version]),
      gossip(NewRumor),
      simple_gossip_event:notify(NewRumor),
      {reply, ok, reschedule_gossip(State#state{rumor = NewRumor})};
    no_change ->
      {reply, ok, State}
  end;
handle_command({set, Data}, _From, #state{rumor = Rumor} = State) ->
  NewRumor = simple_gossip_rumor:set_data(Rumor, Data),
  ?LOG_DEBUG("Set data: ~p vsn: ~p",
             [erlang:phash2(Data, 9999), NewRumor#rumor.gossip_version]),
  gossip(NewRumor),
  simple_gossip_event:notify(NewRumor),
  {reply, ok, reschedule_gossip(State#state{rumor = NewRumor})};

handle_command({join, Node}, _From, #state{rumor = Rumor} = State) ->
  NewRumor = simple_gossip_rumor:if_not_member(Rumor, Node,
                                               fun() -> join_node(Rumor, Node) end),
  gossip(NewRumor),
  {reply, ok, reschedule_gossip(State#state{rumor = NewRumor})};

handle_command({leave, Node}, _From,
               #state{rumor = #rumor{nodes = [Node]}} = State) ->
  {reply, ok, State};

% leader node leaves which is the current node
handle_command({leave, Node}, _From, #state{rumor = #rumor{leader = Node} = Rumor} = State)
  when Node == node() ->
  ?LOG_INFO("Node ~p leaving. Leader: ~p", [Node, Node]),
  OthersRumor = simple_gossip_rumor:remove_node(Rumor, Node),
  gossip(OthersRumor),
  MyRumor = simple_gossip_rumor:new(),
  simple_gossip_event:notify(MyRumor),
  {reply, ok, reschedule_gossip(State#state{rumor = MyRumor})};

% leader node leaves but, not this node
handle_command({leave, Node}, _From, #state{rumor = #rumor{leader = Node} = Rumor} = State)
  when Node =/= node() ->
  ?LOG_INFO("Node ~p leaving. Leader: ~p~n", [Node, Node]),
  NewRumor = simple_gossip_rumor:remove_node(Rumor, Node),
  gossip(NewRumor),
  {reply, ok, reschedule_gossip(State#state{rumor = NewRumor})};

handle_command({leave, Node}, _From,
               #state{rumor = Rumor} = State) ->
  ?LOG_INFO("Node ~p leaving. Leader: ~p~n",
                          [Node, State#state.rumor#rumor.leader]),
  NewRumor = simple_gossip_rumor:remove_node(Rumor, Node),
  simple_gossip_rumor:if_member(Rumor, Node,
                                fun() -> gossip(NewRumor, Node), NewRumor end),
  gossip(NewRumor),
  {reply, ok, reschedule_gossip(State#state{rumor = NewRumor})};

handle_command({change_period, Period}, _From,
               #state{rumor = Rumor} = State) ->
  NewRumor = simple_gossip_rumor:change_gossip_period(Rumor, Period),
  gossip(NewRumor),
  {reply, ok, reschedule_gossip(State#state{rumor = NewRumor})}.



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
      NewRumor0 = simple_gossip_rumor:check_node_exclude(InRumor),
      NewRumor1 = check_leader_change(CRumor, NewRumor0),
      GossipNodes = lists:delete(SenderNode,
                                 lists:delete(node(), NewRumor1#rumor.nodes)),
      gossip(NewRumor1, GossipNodes),
      simple_gossip_event:notify(NewRumor1),
      ?LOG_DEBUG("New gossip ~p From ~p",
                [NewRumor1#rumor.gossip_version, SenderNode]),
      {noreply, State#state{rumor = NewRumor1}};
    _ ->
      ?LOG_WARNING("Dropped newer gossip due to vclock check", []),
      {noreply, State}
  end;
handle_cast({reconcile, Rumor, _SenderNode}, State) ->
  ?LOG_DEBUG("Gossip ~p dropped due to lower version, From ~p",
             [Rumor#rumor.gossip_version, _SenderNode]),
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
  {noreply, reschedule_gossip(State)};
handle_info({nodedown, Node},
            #state{rumor = #rumor{leader = Node} = Rumor} = State) ->
  ?LOG_WARNING("Leader node is down ~p", [Node]),
  {noreply, State#state{rumor = simple_gossip_rumor:change_leader(Rumor)}};
handle_info({nodedown, _Node}, State) ->
  ?LOG_WARNING("Node is down ~p", [_Node]),
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
  NewRumor = simple_gossip_rumor:change_leader(Rumor),
  gossip(NewRumor),
  simple_gossip_event:notify(NewRumor),
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
  erlang:send_after(Period+rand:uniform(10), ?SERVER, tick).

-spec reschedule_gossip(state()) -> state().
reschedule_gossip(#state{rumor = Rumor} = State) ->
  cancel_timer(State),
  State#state{timer_ref = schedule_gossip(Rumor)}.

-spec cancel_timer(state()) -> ok.
cancel_timer(#state{timer_ref = undefined}) ->
  ok;
cancel_timer(#state{timer_ref = TimerRef}) ->
  erlang:cancel_timer(TimerRef),
  ok.

-spec receive_gossip_version(reference(), [node()], pos_integer(), node()) ->
  {ok, pos_integer(), node(), [node()]} | {error, term(), node(), [node()]}.
receive_gossip_version(Ref, Nodes, Version, Leader) ->
  case receive_gossip_version(Ref, Nodes, Version) of
    {ok, Ver} ->
      {ok, Ver, Leader, Nodes};
    {error, Else} ->
      {error, Else, Leader, Nodes}
  end.

-spec receive_gossip_version(reference(), [node()], Version) ->
  FinalResult when
  Version :: pos_integer(),
  FinalResult :: {ok, Version} | {error, {timeout, [node()]} | gossip_vsn_mismatch}.
receive_gossip_version(_, [], Vsn) ->
  {ok, Vsn};
receive_gossip_version(Ref, Nodes, Vsn) ->
  receive
    {gossip_vsn, Vsn, Ref, Node} ->
      receive_gossip_version(Ref, lists:delete(Node, Nodes), Vsn);
    {gossip_vsn, _MismatchVsn, Ref, _Node} ->
      {error, gossip_vsn_mismatch}
  after ?STATUS_RECEIVE_TIMEOUT ->
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
    case call({?MODULE, Leader}, Command, ?PROXY_CALL_TIMEOUT) of
      {ok, {'EXIT', {{nodedown, Leader}, _}}} ->
        ?LOG_DEBUG("Proxy call nodedown ~p", [Leader]),
        NewLeader = simple_gossip_rumor:calculate_new_leader(Rumor),
        NewRumor = Rumor#rumor{leader = NewLeader},
        proxy_call(Command, From, State#state{rumor = NewRumor});
      {ok, {'EXIT', {noproc, _}}} ->
        ?LOG_DEBUG("Proxy call noproc ~p", [Leader]),
        NewLeader = simple_gossip_rumor:calculate_new_leader(Rumor),
        NewRumor = Rumor#rumor{leader = NewLeader},
        proxy_call(Command, From, State#state{rumor = NewRumor});
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

  case call({?MODULE, Node}, {join_request, NewRumor}, ?PROXY_CALL_TIMEOUT) of
    {ok, {upgrade, UpgradeRumor}} ->
      ?LOG_DEBUG("Rumor upgraded from remote ~p ~p",
                              [Node, UpgradeRumor#rumor.gossip_version]),
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
  Key = '$_prevent_late_msgs_$',
  Pid = spawn(fun() ->
                CallTimeout = Timeout-10,
                Self !
                  {Key, self(), catch gen_server:call(Name, Request, CallTimeout)}
              end),
  receive
    {Key, Pid, Msg} ->
      {ok, Msg}
  after Timeout ->
    exit(Pid, kill),
    receive
      {Key, Pid, Msg} ->
        {ok, Msg}
    after 10 ->
      timeout
    end
  end.