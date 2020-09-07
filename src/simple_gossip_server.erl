%%%-------------------------------------------------------------------
%%% @author tihanyipeter
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 05. Sep 2020 20:41
%%%-------------------------------------------------------------------
-module(simple_gossip_server).
-author("tihanyipeter").

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
                nodes = [] :: [node()],
                data :: any(),
                max_gossip_per_period = 8 :: pos_integer(),
                gossip_period = 10000 :: pos_integer(),
                claimant :: node()
                }).

%%%===================================================================
%%% API
%%%===================================================================

set(Data) ->
  gen_server:call(?SERVER, {set, Data}).

get() ->
  gen_server:call(?SERVER, get).

join(Node) ->
  gen_server:call(?SERVER, {join, Node}).

leave(Node) ->
  gen_server:call(?SERVER, {leave, Node}).

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
  State = #state{claimant = node()},
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
handle_call({set, Data}, _From, #state{claimant = Claimant} = State) when Claimant == node() ->
  NewState = increase_version(State#state{data = Data}),
  gossip(NewState),
  {reply, ok, NewState};
handle_call({set, Data}, _From, #state{claimant = Claimant} = State) ->
  {reply, gen_server:call({?MODULE, Claimant}, {set, Data}), State};
handle_call({join, Node}, _From, #state{nodes = Nodes} = State) ->
  net_kernel:connect_node(Node),
  {reply, ok, increase_version(State#state{nodes = [Node | Nodes]})};
handle_call({leave, Node}, _From, #state{nodes = Nodes} = State) ->
  {reply, ok, increase_version(State#state{nodes = lists:delete(Node, Nodes)})};
handle_call(status, From, #state{nodes = Nodes, gossip_version = Version, claimant = Claimant} = State) ->
  spawn(fun() ->
        Ref = make_ref(),
        Self = self(),
        [gen_server:cast({?MODULE, Node}, {get_gossip_version, Self, Ref}) || Node <- Nodes],
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
                        #state{gossip_version = Version}) when InVersion > Version ->
  io:format("Reconcile accepted: ~p ~n", [InVersion]),
  gossip(InState),
  {noreply, InState};
handle_cast({reconcile, _}, State) ->
  %io:format("Reconcile dropped~n", []),
  {noreply, State};
handle_cast({get_gossip_version, Requester, Ref}, State = #state{gossip_version = Vsn}) ->
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
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info(tick, State) ->
  gossip_random_node(State),
  schedule_gossip(State),
  %io:format("tick~n", []),
  {noreply, State};
handle_info(_Info, State) ->
  io:format("info: ~p~n", [_Info]),
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
  CountOfNodes =
    case rand:uniform(length(NewNodes)) of
      Count when Count < Max ->
        Count;
      _ ->
        Max
    end,
  RandomNodes = pick_random_nodes(NewNodes, CountOfNodes),
  [gossip(State, Node) || Node <- RandomNodes].

gossip_random_node(#state{nodes = []}) ->
  ok;
gossip_random_node(#state{nodes = Nodes} = State) ->
  NewNodes = lists:delete(node(), Nodes),
  RandomNodes = pick_random_nodes(NewNodes, 1),
  [gossip(State, Node) || Node <- RandomNodes],
  ok.

gossip(State, Node) ->
  io:format("sending reconcile to ~p version: ~p~n", [Node, State#state.gossip_version]),
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


% [simple_gossip_server:join(list_to_atom(integer_to_list(I) ++ "@Tihanyi-MacBook-Pro-2")) || I <- lists:seq(1, 30)].
% [simple_gossip_server:join(list_to_atom(integer_to_list(I) ++ "@Tihanyi-MacBook-Pro-2")) || I <- lists:seq(1, 3)].