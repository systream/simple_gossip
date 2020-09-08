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
-export([start_link/0, set/1, join/1, leave/1, get/0, status/0]).

%% gen_server callbacks
-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {gossip_version = 1 :: pos_integer(),
                data :: any(),
                claimant :: node(),
                nodes = [] :: [node()],
                max_gossip_per_period = 8 :: pos_integer(),
                gossip_period = 10000 :: pos_integer()
                }).

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

-spec join(node()) -> ok.
join(Node) ->
  gen_server:call(?SERVER, {join, Node}).

-spec leave(node()) -> ok.
leave(Node) ->
  gen_server:call(?SERVER, {leave, Node}).

-spec status() ->
  {ok, Vsn, Claimant, Nodes} | {error, timeout} | mismatch when
  Vsn :: pos_integer(),
  Claimant :: node(),
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
  State = #state{claimant = node(), nodes = [node()]},
  schedule_gossip(State),
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
handle_call(get, _From, #state{data = Data} = State) ->
  {reply, Data, State};
handle_call({set, Data}, _From, #state{claimant = Claimant} = State) when Claimant =/= node() ->
  {reply, gen_server:call({?MODULE, Claimant}, {set, Data}), State};
handle_call({set, ChangeFun}, _From, #state{data = Data} = State) when is_function(ChangeFun) ->
  case ChangeFun(Data) of
    {change, NewData} ->
      NewState = increase_version(State#state{data = NewData}),
      gossip(NewState),
      {reply, ok, NewState};
    no_change ->
      {reply, ok, State}
  end;
handle_call({set, Data}, _From, State) ->
  NewState = increase_version(State#state{data = Data}),
  gossip(NewState),
  {reply, ok, NewState};
handle_call({join, Node}, _From, #state{nodes = Nodes} = State) ->
  case lists:member(Node, Nodes) of
    true ->
      {reply, ok, State};
    _ ->
      net_kernel:connect_node(Node),
      NewState = increase_version(State#state{nodes = [Node | Nodes]}),
      gen_server:cast({?MODULE, Node}, {join, node(), NewState}),
      {reply, ok, NewState}
  end;
handle_call({leave, Node}, _From, #state{nodes = Nodes} = State) ->
  NewState = increase_version(State#state{nodes = lists:delete(Node, Nodes)}),
  gossip(NewState),
  {reply, ok, NewState};

handle_call(status, From, #state{nodes = Nodes, gossip_version = Version, claimant = Claimant} = State) ->
  spawn(fun() ->
        Ref = make_ref(), Self = self(),
        gen_server:abcast(Nodes, ?MODULE, {get_gossip_version, Self, Ref}),
        Result = case receive_gossip_version(Ref, Nodes, {ok, Version}) of
                    {ok, Ver} ->
                      {ok, Ver, Claimant, Nodes};
                    Else ->
                      Else
                  end,
        gen_server:reply(From, Result)
         end),
  {noreply, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

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
handle_cast({reconcile, #state{gossip_version = InVersion} = InState},
                        #state{gossip_version = Version} = CurrentState) when InVersion > Version ->
  NewState = change_state(CurrentState, InState),
  gossip(NewState),
  {noreply, NewState};
handle_cast({reconcile, _}, State) ->
  {noreply, State};

handle_cast({get_gossip_version, Requester, Ref}, State = #state{gossip_version = Vsn}) ->
  Requester ! {gossip_vsn, Vsn, Ref, node()},
  {noreply, State};

handle_cast({join, _Node, #state{gossip_version = InGossipVsn} = InState},
            #state{gossip_version = CurrentGossipVsn}) when InGossipVsn > CurrentGossipVsn ->
  gossip(InState),
  {noreply, InState};
handle_cast({join, Node, #state{gossip_version = InGossipVsn, nodes = Nodes}},
            #state{gossip_version = CurrentGossipVsn} = State) when InGossipVsn =< CurrentGossipVsn ->
  NewState = increase_version(State#state{nodes = [Node | Nodes]}),
  gossip(NewState, Node),
  {noreply, NewState}.

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
handle_info(tick, State) ->
  gossip_random_node(State),
  schedule_gossip(State),
  {noreply, State};
handle_info({nodedown, Node}, #state{claimant = Node} = State) ->
  % panic: claimant node is down, every node became claimant for it self,
  % and when the next set comes the new claimant will be that node
  NewState = increase_version(State#state{claimant = node()}),
  {noreply, NewState};
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
gossip(#state{nodes = []}) ->
  ok;
gossip(#state{nodes = Nodes, max_gossip_per_period = Max} = State) ->
  NewNodes = lists:delete(node(), Nodes),
  RandomNodes = pick_random_nodes(NewNodes, Max),
  [gossip(State, Node) || Node <- RandomNodes].

gossip_random_node(#state{nodes = []}) ->
  ok;
gossip_random_node(#state{nodes = Nodes} = State) ->
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

schedule_gossip(#state{gossip_period = Period}) ->
  erlang:send_after(Period, ?SERVER, tick).

increase_version(#state{gossip_version = GossipVersion} = State) ->
  State#state{gossip_version = GossipVersion+1}.

receive_gossip_version(_, [], Vsn) ->
  Vsn;
receive_gossip_version(Ref, [Node | Nodes], {ok, Vsn}) ->
  receive
    {gossip_vsn, Vsn, Ref, Node} ->
      receive_gossip_version(Ref, Nodes, {ok, Vsn});
    {gossip_vsn, _, Ref, Node} ->
      receive_gossip_version(Ref, Nodes, mismatch)
  after 1000 ->
    {error, timeout}
  end;
receive_gossip_version(_, _, Result) ->
  Result.


change_state(#state{claimant = Claimant},
             #state{claimant = Claimant} = NewState) ->
  NewState;
change_state(#state{claimant = OldClaimant},
             #state{claimant = NewClaimant} = NewState) ->
  erlang:monitor_node(NewClaimant, true),
  erlang:monitor_node(OldClaimant, false),
  NewState.