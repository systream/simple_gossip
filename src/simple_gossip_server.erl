%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_server).

-behaviour(gen_server).

-include("simple_gossip.hrl").
-include_lib("kernel/include/logger.hrl").

-define(STATUS_RECEIVE_TIMEOUT, 1024).
-define(PROXY_CALL_TIMEOUT, 3000).

%% API
-export([start_link/0,

          join/1,
          leave/1,

          set/1,
          get/0,

          status/0,

          get_gossip/0,

          set_max_gossip_per_period/1,
          set_gossip_interval/1,

          wait_to_propagate/3, wait_to_propagate/1]).

%% gen_server callbacks
-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2]).

-ifdef(TEST).
-export([build_tree/2]).
-endif.

-define(SERVER, ?MODULE).

-define(RUMOR(State), State#state.rumor).

-record(state, {rumor = #rumor{} :: rumor(),
                rumor_record_version = 1 :: pos_integer(),
                timer_ref :: reference() | undefined
}).

-opaque state() :: #state{}.
-export_type([state/0, rumor/0, set_fun/0]).

-type set_fun() ::  fun((term()) -> {change, term()} | no_change).

%%%===================================================================
%%% API
%%%===================================================================
-spec set(term() | set_fun()) -> {ok, rumor()}.
set(Data) ->
  {ok, _Rumor} = gen_server:call(?SERVER, {set, Data}).

-spec get() -> term().
get() ->
  gen_server:call(?SERVER, get).

-spec wait_to_propagate(rumor()) -> ok | {error, {timeout, node()} | term()}.
wait_to_propagate(#rumor{nodes = Nodes, gossip_version = Vsn}) ->
  Caller = self(),
  {Pid, Ref} = spawn_opt(?MODULE, wait_to_propagate, [Caller, Vsn, Nodes], [{monitor, [{'alias', 'demonitor'}]}]),
  receive
    {'DOWN', Ref, process, _Pid, Reason} ->
      {error, Reason};
    {result, Pid, Result} ->
      erlang:demonitor(Ref, [flush]),
      Result
  end.

-spec wait_to_propagate(pid(), pos_integer(), [node()]) -> term().
wait_to_propagate(Caller, Vsn, Nodes) ->
  wait_to_propagate(Caller, Vsn, Nodes, 10).

-spec wait_to_propagate(pid(), pos_integer(), [node()], pos_integer()) -> term().
wait_to_propagate(Caller, TargetVsn, Nodes, RetryCount) ->
  Ref = make_ref(),
  gen_server:abcast(Nodes, ?MODULE, {get_gossip_version, self(), Ref}),
  case receive_gossip_version(Ref, Nodes) of
    {ok, Versions} ->
      case lists:all(fun(AggregatedVersion) -> AggregatedVersion >= TargetVsn end, Versions) of
        true ->
          Caller ! {result, self(), ok};
        _ ->
          timer:sleep(200),
          wait_to_propagate(Caller, TargetVsn, Nodes, RetryCount - 1)
      end;
    {error, {timeout, _Node}} when RetryCount > 0 ->
      timer:sleep(200),
      wait_to_propagate(Caller, TargetVsn, Nodes, RetryCount - 1);
    Result ->
      Caller ! {result, self(), Result}
  end.

-spec join(node()) -> ok | {error, term()}.
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

-spec set_max_gossip_per_period(pos_integer()) -> ok.
set_max_gossip_per_period(Period) ->
  gen_server:call(?SERVER, {change_max_gossip_per_period, Period}).

-spec set_gossip_interval(pos_integer()) -> ok.
set_gossip_interval(Interval) ->
  gen_server:call(?SERVER, {change_interval, Interval}).

-spec get_gossip() -> {ok, rumor()}.
get_gossip() ->
  gen_server:call(?SERVER, whisper_your_gossip, 5000).

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
        pre_sync(StoredRumor);
      {error, undefined} ->
        ?LOG_DEBUG("No rumor file found", []),
        simple_gossip_rumor:new()
    end,
  State = #state{rumor = Rumor},
  {ok, reschedule_gossip(State)}.

-spec pre_sync(rumor()) -> rumor().
pre_sync(StoredRumor) ->
  fetch_gossip(remove_current_node(simple_gossip_rumor:nodes(StoredRumor)), StoredRumor).

-spec fetch_gossip([node()], rumor()) -> rumor().
fetch_gossip(Nodes, StoredRumor) ->
  case simple_gossip_rumor:pick_random_nodes(Nodes, 1) of
    [Node] ->
      case catch rpc:call(Node, ?MODULE, get_gossip, []) of
        {ok, NewRumor} when NewRumor#rumor.gossip_version > StoredRumor#rumor.gossip_version ->
          ?LOG_INFO("PreSync rumor from ~p gossip vsn: ~p", [Node, NewRumor#rumor.gossip_version]),
          simple_gossip_rumor:check_node_exclude(NewRumor);
        {ok, NewRumor} when NewRumor#rumor.gossip_version =< StoredRumor#rumor.gossip_version ->
          ?LOG_INFO("Cannot get fresh gossip From ~p Reason: older gossip vsn ~p > ~p",
                      [Node, NewRumor#rumor.gossip_version, StoredRumor#rumor.gossip_version]),
          maybe_fetch_gossip(Nodes -- [Node], StoredRumor);
        Else ->
          ?LOG_WARNING("Cannot get fresh gossip From ~p Reason: ~p", [Node, Else]),
          maybe_fetch_gossip(Nodes -- [Node], StoredRumor)
      end;
    _ ->
      % from nowhere to sync
      StoredRumor
  end.

-spec maybe_fetch_gossip([node()], rumor()) -> rumor().
maybe_fetch_gossip([], StoredRumor) ->
  ?LOG_WARNING("No more nodes available for pre syncing rumor, using localy stored"),
  StoredRumor;
maybe_fetch_gossip(NewNodes, StoredRumor) ->
  fetch_gossip(NewNodes, StoredRumor).

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
handle_call(get, _From, #state{rumor = Rumor} = State) ->
  {reply, simple_gossip_rumor:data(Rumor), State};
handle_call({set, Data}, From, State) ->
  case proxy_call({set, Data}, From, State) of
    {Leader, {reply, {ok, Rumor}, State}} -> % Let's update the rumor locally too
      {noreply, NewState} = handle_cast({reconcile, Rumor, Leader}, State),
      {reply, {ok, Rumor}, NewState};
    {_Leader, Else} ->
      Else
  end;
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
            {FromPid, _},
            #state{rumor = #rumor{gossip_version = CurrentGossipVsn}} = State)
  when InGossipVsn >= CurrentGossipVsn ->
  ?LOG_INFO(
    "Join request from ~p. Remote rumor (~p) is greater than the current (~p)" ++
    " applying it", [node(FromPid), InGossipVsn, CurrentGossipVsn]),
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
handle_call(reconcile, {FromPid, _FromRef}, #state{rumor = Rumor} = State) ->
  gossip(Rumor, node(FromPid)),
  {reply, ok, State};

handle_call(Command, From, State) ->
  {_Leader, Result} = proxy_call(Command, From, State),
  Result.

-spec(handle_command(Request :: term(), From :: {pid(), Tag :: term()},
                  State :: state()) ->
                   {reply, Reply :: term(), NewState :: state()} |
                   {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
                   {noreply, NewState :: state()} |
                   {noreply, NewState :: state(), timeout() | hibernate} |
                   {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
                   {stop, Reason :: term(), NewState :: state()}).
handle_command({set, ChangeFun}, From, #state{rumor = Rumor} = State)
  when is_function(ChangeFun) ->
  case ChangeFun(simple_gossip_rumor:data(Rumor)) of
    {change, NewData} ->
      handle_command({set, NewData}, From, State);
    no_change ->
      {reply, {ok, Rumor}, State}
  end;
handle_command({set, Data}, _From, #state{rumor = Rumor} = State) ->
  NewRumor = simple_gossip_rumor:set_data(Rumor, Data),
  ?LOG_DEBUG("Set data: ~p vsn: ~p",
             [erlang:phash2(Data, 9999), NewRumor#rumor.gossip_version]),
  gossip(NewRumor),
  simple_gossip_event:notify(NewRumor),
  {reply, {ok, NewRumor}, reschedule_gossip(State#state{rumor = NewRumor})};

handle_command({join, Node}, _From, #state{rumor = Rumor} = State) ->
  case net_kernel:connect_node(Node) of
    true ->
      NewRumor = simple_gossip_rumor:if_not_member(Rumor, Node, fun() ->
                                                                  join_node(Rumor, Node)
                                                                end),
      gossip(NewRumor),
      simple_gossip_event:notify(NewRumor),
      {reply, ok, reschedule_gossip(State#state{rumor = NewRumor})};
    _ ->
      {reply, {error, {could_not_connect_to_node, Node}}, State}
  end;

handle_command({leave, Node}, _From,
               #state{rumor = #rumor{nodes = [Node]}} = State) ->
  {reply, ok, State};

% leader node leaves which is the current node
handle_command({leave, Node}, _From, #state{rumor = #rumor{leader = Node} = Rumor} = State)
  when Node == node() ->
  ?LOG_INFO("Node ~p leaving. Leader: ~p", [Node, Node]),
  OthersRumor = simple_gossip_rumor:remove_node(Rumor, Node),
  gossip(OthersRumor),
  MyRumor = simple_gossip_rumor:new(Rumor),
  simple_gossip_event:notify(MyRumor),
  {reply, ok, reschedule_gossip(State#state{rumor = MyRumor})};

% leader node leaves but, not this node
handle_command({leave, Node}, _From, #state{rumor = #rumor{leader = Node} = Rumor} = State)
  when Node =/= node() ->
  ?LOG_INFO("Node ~p leaving. Leader: ~p~n", [Node, Node]),
  NewRumor = simple_gossip_rumor:remove_node(Rumor, Node),
  gossip(NewRumor),
  simple_gossip_event:notify(NewRumor),
  {reply, ok, reschedule_gossip(State#state{rumor = NewRumor})};

handle_command({leave, Node}, _From,
               #state{rumor = Rumor} = State) ->
  ?LOG_INFO("Node ~p leaving. Leader: ~p~n",
                          [Node, State#state.rumor#rumor.leader]),
  NewRumor = simple_gossip_rumor:remove_node(Rumor, Node),
  simple_gossip_rumor:if_member(Rumor, Node,
                                fun() -> gossip(NewRumor, Node),
                                         simple_gossip_event:notify(NewRumor),
                                         NewRumor
                                end),
  gossip(NewRumor),
  {reply, ok, reschedule_gossip(State#state{rumor = NewRumor})};

handle_command({change_max_gossip_per_period, Period}, _From,
               #state{rumor = Rumor} = State) ->
  NewRumor = simple_gossip_rumor:change_max_gossip_per_period(Rumor, Period),
  gossip(NewRumor),
  simple_gossip_event:notify(NewRumor),
  {reply, ok, reschedule_gossip(State#state{rumor = NewRumor})};
handle_command({change_interval, Interval}, _From,
               #state{rumor = Rumor} = State) ->
  NewRumor = simple_gossip_rumor:change_gossip_interval(Rumor, Interval),
  gossip(NewRumor),
  simple_gossip_event:notify(NewRumor),
  {reply, ok, reschedule_gossip(State#state{rumor = NewRumor})};
handle_command(whisper_your_gossip, _From,
               #state{rumor = Rumor} = State) ->
  {reply, {ok, Rumor}, State}.

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
handle_cast({reconcile, #rumor_head{gossip_version = InVersion}, SenderNode},
             #state{rumor = #rumor{gossip_version = Version}} = State)
  when InVersion > Version ->
  gen_server:call({?SERVER, SenderNode}, reconcile),
  {noreply, State};
handle_cast({reconcile,
             #rumor{gossip_version = InVersion} = InRumor,
             SenderNode},
             #state{rumor = #rumor{gossip_version = Version} = CRumor} = State)
  when InVersion > Version ->
  case simple_gossip_rumor:check_vector_clocks(InRumor, CRumor) of
    true ->
      NewRumor1 = simple_gossip_rumor:check_node_exclude(InRumor),
      GossipNodes = lists:delete(SenderNode, gossip_neighbours(node(), NewRumor1)),

      gossip(NewRumor1, GossipNodes),
      simple_gossip_event:notify(NewRumor1),
      ?LOG_DEBUG("New gossip ~p From ~p",
                [NewRumor1#rumor.gossip_version, SenderNode]),
      {noreply, State#state{rumor = NewRumor1}};
    _ ->
      ?LOG_WARNING("Dropped newer gossip (~p) due to vclock check", [InVersion]),
      {noreply, State}
  end;
handle_cast({reconcile, #rumor_head{gossip_version = InVersion}, _SenderNode},
            #state{rumor = #rumor{gossip_version = Version}} = State)
  when InVersion =:= Version ->
  {noreply, State};
handle_cast({reconcile, #rumor{gossip_version = InVersion}, _SenderNode},
    #state{rumor = #rumor{gossip_version = Version}} = State)
  when InVersion =:= Version ->
  {noreply, State};
handle_cast({reconcile, InRumor, SenderNode}, #state{rumor = Rumor} = State) ->
  ?LOG_DEBUG("Gossip ~p dropped due to lower version, From ~p",
             [simple_gossip_rumor:version(InRumor), SenderNode]),
  gossip(Rumor, SenderNode),
  {noreply, State};

handle_cast({get_gossip_version, Requester, Ref},
            #state{rumor = #rumor{gossip_version = Vsn}} = State) ->
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
  {noreply, reschedule_gossip(State)}.

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
gossip(Rumor) ->
  gossip(Rumor, gossip_neighbours(node(), Rumor)).

-spec gossip_random_node(rumor()) -> ok.
gossip_random_node(#rumor{nodes = []}) ->
  ok;
gossip_random_node(Rumor) ->
  NewNodes = remove_current_node(simple_gossip_rumor:nodes(Rumor)),
  case simple_gossip_rumor:pick_random_nodes(NewNodes, 1) of
    [] -> ok;
    [Node] -> gossip_head(Rumor, Node)
  end.

-spec remove_current_node([node()]) -> [node()].
remove_current_node(Nodes) ->
  lists:delete(node(), Nodes).

-spec gossip(rumor(), node() | [node()]) -> ok.
gossip(Rumor, Nodes) when is_list(Nodes) ->
  [gossip(Rumor, Node) || Node <- Nodes],
  ok;
gossip(Rumor, Node) ->
  gen_server:cast({?MODULE, Node}, {reconcile, Rumor, node()}).

-spec gossip_head(rumor(), node()) -> ok.
gossip_head(Rumor, Node) ->
  gen_server:cast({?MODULE, Node}, {reconcile, simple_gossip_rumor:head(Rumor), node()}).

-spec schedule_gossip(rumor()) -> reference().
schedule_gossip(#rumor{gossip_period = Period}) ->
  erlang:send_after(Period + rand:uniform(100), ?SERVER, tick).

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
  case receive_gossip_version(Ref, Nodes) of
    {ok, Versions} ->
      case lists:all(fun (AggregatedVersions) -> AggregatedVersions =:= Version end, Versions) of
        true ->
          {ok, Version, Leader, Nodes};
        false ->
          {error, gossip_vsn_mismatch}
      end;
    {error, Else} ->
      {error, Else, Leader, Nodes}
  end.

-spec receive_gossip_version(reference(), [node()]) ->
  FinalResult when
  Version :: pos_integer(),
  FinalResult :: {ok, [Version]} | {error, {timeout, [node()]}}.
receive_gossip_version(Ref, Nodes) ->
  receive_gossip_version(Ref, Nodes, []).

-spec receive_gossip_version(reference(), [node()], [Version]) ->
  FinalResult when
  Version :: pos_integer(),
  FinalResult :: {ok, [Version]} | {error, {timeout, [node()]}}.
receive_gossip_version(_, [], VsnAcc) ->
  {ok, VsnAcc};
receive_gossip_version(Ref, Nodes, VsnAcc) ->
  receive
    {gossip_vsn, Vsn, Ref, Node} ->
      receive_gossip_version(Ref, lists:delete(Node, Nodes), [Vsn | VsnAcc])
  after ?STATUS_RECEIVE_TIMEOUT ->
    {error, {timeout, Nodes}}
  end.

-spec proxy_call(term(), {pid(), Tag :: term()}, state()) ->
  {node(), {reply, term(), state()} | {noreply, state()}}.
proxy_call(Command, From, State) ->
  proxy_call(Command, From, State, []).

-spec proxy_call(term(), {pid(), Tag :: term()}, state(), [node()]) ->
  {node(), {reply, term(), state()} | {noreply, state()}}.
proxy_call(Command, From,
           #state{rumor = #rumor{leader = Leader} = Rumor} = State,
           DownNodes) when Leader =/= node() ->
    case call(Leader, Command) of
      {'EXIT', {Error, _}} when {nodedown, Leader} == Error orelse
                                 noproc == Error ->
        ?LOG_DEBUG("Proxy call nodedown ~p", [Leader]),
        NewDownNodes = [Leader | DownNodes],
        NewLeader = simple_gossip_rumor:calculate_new_leader(Rumor, NewDownNodes),
        NewRumor = Rumor#rumor{leader = NewLeader},
        proxy_call(Command, From, State#state{rumor = NewRumor}, NewDownNodes);
      {'EXIT', {timeout, _}} ->
        {Leader, {reply, timeout, State}};
      Else ->
        {Leader, {reply, Else, State}}
    end;
proxy_call(Command, From, State, _DownNodes) ->
  {node(), handle_command(Command, From, State)}.

call(Leader, Command) ->
  call(Leader, Command, 1).

call(Leader, Command, Retry) ->
  case catch gen_server:call({?MODULE, Leader}, Command, ?PROXY_CALL_TIMEOUT) of
    {'EXIT', {Error, _}}
      when ({nodedown, Leader} == Error orelse noproc == Error) andalso Retry >= 1 ->
      call(Leader, Command, Retry - 1);
    Else ->
      Else
  end.

-spec join_node(rumor(), node()) -> rumor().
join_node(Rumor, Node) ->
  NewRumor = simple_gossip_rumor:add_node(Rumor, Node),
  case gen_server:call({?MODULE, Node}, {join_request, NewRumor}, ?PROXY_CALL_TIMEOUT) of
    {upgrade, UpgradeRumor} ->
      ?LOG_DEBUG("Rumor upgraded from remote ~p ~p", [Node, UpgradeRumor#rumor.gossip_version]),
      UpgradeRumor;
    ok ->
      NewRumor
  end.

-spec gossip_neighbours(node(), rumor()) -> [node()].
gossip_neighbours(Node, #rumor{nodes = Nodes, max_gossip_per_period = N}) ->
  maps:get(Node, build_tree(N, Nodes), []).

-spec build_tree(N :: integer(), Nodes :: [term()]) -> #{node() => [node()]}.
build_tree(_N, []) -> #{};
build_tree(_N, [_]) -> #{};
build_tree(N, Nodes) ->
  NodeCount = length(Nodes),
  Len = erlang:min(N, NodeCount - 1),
  Expand = lists:flatten(lists:duplicate(Len + 1, Nodes)),
  {Tree, _} = lists:foldl(fun(CNode, {Result, CNodes}) ->
                            {Neighbours, Rest} = lists:split(Len, CNodes),
                            {Neighbours1, Rest1} =
                              case lists:member(CNode, Neighbours) of
                              true ->
                                [NextNode | RRest] = Rest,
                                {[NextNode | lists:delete(CNode, Neighbours)], RRest};
                              _ ->
                                {Neighbours, Rest}
                            end,
                            {Result#{CNode => Neighbours1}, Rest1}
                          end, {#{}, tl(Expand)}, Nodes),
  Tree.