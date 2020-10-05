-module(prop_model).
-include_lib("proper/include/proper.hrl").

%% Model Callbacks
-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3, subscribe_on_node/1]).

-define(RPC(Node, Function, Args),
        {call, rpc, call, [Node, simple_gossip, Function, Args]}).

%%%%%%%%%%%%%%%%%%
%%% PROPERTIES %%%
%%%%%%%%%%%%%%%%%%
prop_test() ->
  simple_gossip_test_tools:stop_cluster(),
  simple_gossip_test_tools:init_netkernel(),
  application:ensure_all_started(simple_gossip),
  SupChildren = supervisor:which_children(simple_gossip_sup),
  {_, ServerPid, _, _} = lists:keyfind(simple_gossip_server, 1, SupChildren),
  % restart the server to loose it's state
  exit(ServerPid, kill),
    ?FORALL(Cmds, commands(?MODULE),
            begin
                simple_gossip:set(1),
                {History, State, Result} = run_commands(?MODULE, Cmds),
                ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
                                    [History,State,Result]),
                          aggregate(cmd_names(Cmds), Result =:= ok))
            end).


cmd_names({Cmds, L}) ->
  lists:flatten([cmd_names(Cmds)|[ cmd_names(Cs) || Cs <- L ]]);
cmd_names(Cmds) ->
  [cm({M, F, Args}) || {set, _Var, {call, M, F, Args}} <- Cmds].

cm({rpc, call, [_, M, F, A]}) ->
  cm({M, F, A});
cm({M, F, Args}) ->
  {M, F, length(Args)}.


%%%%%%%%%%%%%
%%% MODEL %%%
%%%%%%%%%%%%%
%% @doc Initial model value at system start. Should be deterministic.
initial_state() ->
  Nodes = simple_gossip_test_tools:start_nodes(['g1', 'g2', 'g3', 'g4', 'g5']),

  [simple_gossip:join(Node) || Node <- Nodes],

  #{nodes => Nodes,
    data => 1,
    subscribers => [],
    in_cluster => Nodes}.

%% @doc List of possible commands to run against the system
command(#{nodes := Nodes, in_cluster := InCluster, subscribers := Subscribers}) ->
  RandomNode = oneof(Nodes),
  RpcNode = oneof(InCluster),
  OneOfSubscribers = oneof([skip | Subscribers]),
    frequency([
      {15, ?RPC(RpcNode, set, [resize(150, term())])},
      {25, ?RPC(RpcNode, get, [])},
      {13, ?RPC(RandomNode, join, [RandomNode])},
      {10, ?RPC(RandomNode, leave, [RandomNode])},

      {5, {call, ?MODULE, subscribe_on_node, [RandomNode]}},
      {4, ?RPC(RpcNode, unsubscribe, [OneOfSubscribers])},
      {3, {call, erlang, exit, [OneOfSubscribers, kill]}}
  ]).

%% @doc Determines whether a command should be valid under the
%% current state.
precondition(_, {call, rpc, call, [Node, _, get, []]}) ->
  simple_gossip_test_tools:wait_to_reconcile(Node, 200),
  true;
precondition(#{in_cluster := ClusterNodes},
             {call, rpc, call, [Node, _, join, [ToNode]]}) ->
  lists:member(Node, ClusterNodes) orelse lists:member(ToNode, ClusterNodes);
precondition(_, {call, rpc, call, [Node, _, leave, [FromNode]]}) ->
  Node =/= FromNode;

precondition(_, {call, rpc, call, [_Node, _, unsubscribe, [skip]]}) ->
  false;
precondition(_, {call, erlang, exit, [skip, kill]}) ->
  false;

precondition(_State, {call, _Mod, _Fun, _Args}) ->
  true.

%% @doc Given the state `State' *prior* to the call
%% `{call, Mod, Fun, Args}', determine whether the result
%% `Res' (coming from the actual system) makes sense.
postcondition(_State, {call, _Mod, _Fun, [_Node, _, join, [_JoinNode]]}, Res) ->
  Res == ok;
postcondition(_State, {call, _Mod, _Fun, [_Node, _, leave, [_LeaveNode]]}, Res) ->
  Res == ok;
postcondition(_State, {call, _Mod, _Fun, [_Node, _, set, [_Data]]}, Res) ->
  Res == ok;
postcondition(_, {call, rpc, call, [_, _, get, []]}, undefined) ->
  true;
postcondition(#{data := Data},
              {call, rpc, call, [_Node, _, get, []]}, Res) ->
  case Data /= Res of
    true ->
      io:format("Mismatch: ~p - ~p ~n States: ~n", [Data, Res]),
      ok;
    _ ->
      ok
  end,
  Data == Res;
postcondition(_State, {call, _Mod, _Fun, _Args} = _A, _Res) ->
  true.

%% @doc Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(State, _Res,
           {call, rpc, call, [_Node, _, set, [SetData]] = _Args}) ->
  State#{data => SetData};
next_state(#{in_cluster := ClusterNodes} = State, _Res,
           {call, rpc, call, [FromNode, _, join, [ToNode]] = _Args}) ->
  case {lists:member(FromNode, ClusterNodes), lists:member(ToNode, ClusterNodes)} of
    {false, false} ->
      State;
    _ ->
      State#{in_cluster => lists:usort([ FromNode, ToNode | ClusterNodes])}
  end;
next_state(#{in_cluster := ClusterNodes} = State, _Res,
           {call, rpc, call, [FromNode, _, leave, [ToNode]] = _Args}) ->
  case lists:member(FromNode, ClusterNodes) of
    false ->
      State;
    _ ->
      State#{in_cluster => lists:delete(ToNode, ClusterNodes) }
  end;

next_state(#{subscribers := Subscribers} = State, Pid,
           {call, ?MODULE, subscribe_on_node, [_]}) ->
  State#{subscribers => [Pid | Subscribers]};

next_state(State, _Res, {call, _Mod, _Fun, _Args}) ->
  State.


subscribe_on_node(Node) ->
  Pid = spawn(fun() -> subscribe_loop(undefined) end),
  ok = rpc:call(Node, simple_gossip, subscribe, [Pid]),
  Pid.

subscribe_loop(Data) ->
  receive
    {data_changed, Data} ->
      subscribe_loop(Data);
    {get_data, Ref, Pid} ->
      Pid ! {data, Ref, Data}
  end.
