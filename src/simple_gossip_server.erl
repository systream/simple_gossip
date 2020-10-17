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

-include_lib("simple_gossip.hrl").

%% API
-export([start_link/0,

         join/1,
         leave/1,

         set/1,
         get/0,

         status/0]).

%% gen_server callbacks
-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

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
  {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  gen_event:add_handler(simple_gossip_event, simple_gossip_notify, []),
  %gen_event:add_handler(simple_gossip_event, simple_gossip_throttle, []),
  Rumor = simple_gossip_rumor:new(),
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
handle_call(get, _From, #state{rumor = #rumor{data = Data}} = State) ->
  {reply, Data, State};
handle_call({set, Data}, From, State) ->
  proxy_call({set, Data}, From, State);
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
handle_call({join_request, #rumor{gossip_version = InGossipVsn} = InRumor},
            _From,
            #state{rumor = #rumor{gossip_version = CurrentGossipVsn}} = State)
  when InGossipVsn >= CurrentGossipVsn ->
  gossip(InRumor),
  {reply, ok, State#state{rumor = InRumor}};
handle_call({join_request, #rumor{gossip_version = InGossipVsn}}, {FromPid, _},
             #state{rumor = #rumor{gossip_version = CurrentGossipVsn} = Rumor} =
              State) when InGossipVsn < CurrentGossipVsn ->
  Node = node(FromPid),
  NewRumor = simple_gossip_rumor:add_node(Rumor, Node),
  gossip(NewRumor, Node),
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
      gossip(NewRumor),
      notify_subscribers(NewRumor),
      {reply, ok, State#state{rumor = NewRumor}};
    no_change ->
      {reply, ok, State}
  end;
handle_command({set, Data}, _From, #state{rumor = Rumor} = State) ->
  NewRumor = simple_gossip_rumor:set_data(Rumor, Data),
  gossip(NewRumor),
  notify_subscribers(NewRumor),
  {reply, ok, State#state{rumor = NewRumor}};

handle_command({join, Node}, _From, #state{rumor = Rumor} = State) ->
  NewRumor =
    simple_gossip_rumor:if_not_member(Rumor, Node, fun() -> join_node(Rumor, Node) end),
  gossip(NewRumor),
  {reply, ok, State#state{rumor = NewRumor}};

handle_command({leave, Node}, _From,
               #state{rumor = #rumor{nodes = [Node]}} = State) ->
  {reply, ok, State};

handle_command({leave, Node}, _From, #state{rumor = #rumor{leader = Node} = Rumor} = State)
  when Node == node() ->
  gossip(simple_gossip_rumor:remove_node(Rumor, Node)),
  MyRumor = simple_gossip_rumor:new(),
  notify_subscribers(MyRumor),
  {reply, ok, State#state{rumor = MyRumor}};

handle_command({leave, Node}, _From, #state{rumor = #rumor{leader = Node} = Rumor} = State)
  when Node =/= node() ->
  NewRumor = simple_gossip_rumor:remove_node(Rumor, Node),
  simple_gossip_rumor:if_member(Rumor, Node, fun() -> gossip(NewRumor, Node), NewRumor end),
  gossip(NewRumor), % normal gossip
  {reply, ok, State#state{rumor = NewRumor}};

handle_command({leave, Node}, _From,
               #state{rumor = Rumor} = State) ->
  NewRumor = simple_gossip_rumor:remove_node(Rumor, Node),
  simple_gossip_rumor:if_member(Rumor, Node, fun() -> gossip(NewRumor, Node), NewRumor end),
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
  NewRumor = check_leader_change(CRumor, simple_gossip_rumor:check_node_exclude(InRumor)),
  GossipNodes = lists:delete(SenderNode, lists:delete(node(), NewRumor#rumor.nodes)),
  gossip(NewRumor, GossipNodes),
  notify_subscribers(NewRumor),
  {noreply, State#state{rumor = NewRumor}};
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
  {noreply, State#state{rumor = simple_gossip_rumor:change_leader(Rumor)}};
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
    State :: state()) -> term()).
terminate(_Reason, #state{rumor = #rumor{leader = Leader} = Rumor})
  when Leader == node() ->
  gossip(simple_gossip_rumor:remove_node(Rumor, Leader)),
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
-spec(code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) ->
  {ok, NewState :: state()} | {error, Reason :: term()}).
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
    case catch gen_server:call({?MODULE, Leader}, Command) of
      {'EXIT', {{nodedown, Leader}, _}} ->
        proxy_call(Command, From, State#state{rumor = simple_gossip_rumor:change_leader(Rumor)});
      {'EXIT', {noproc, _}} ->
        proxy_call(Command, From, State#state{rumor = simple_gossip_rumor:change_leader(Rumor)});
      Else ->
        {reply, Else, State}
    end;
proxy_call(Command, From, State) ->
  handle_command(Command, From, State).

-spec join_node(rumor(), node()) -> rumor().
join_node(Rumor, Node) ->
  NewRumor = simple_gossip_rumor:add_node(Rumor, Node),
  net_kernel:connect_node(Node),
  case catch gen_server:call({?MODULE, Node}, {join_request, NewRumor}, 1000) of
    {upgrade, UpgradeRumor} ->
      UpgradeRumor;
    _ ->
      NewRumor
  end.

-spec notify_subscribers(rumor()) -> ok.
notify_subscribers(Rumor) ->
  gen_event:notify(simple_gossip_event, {reconcile, Rumor}).