-module(prop_model).
-include_lib("proper/include/proper.hrl").
-include("../src/simple_gossip.hrl").

%% Model Callbacks
-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3, subscribe_on_node/1]).

-define(RPC(Node, Function, Args),
        {call, rpc, call, [Node, simple_gossip, Function, Args]}).

%%%%%%%%%%%%%%%%%%
%%% PROPERTIES %%%
%%%%%%%%%%%%%%%%%%

f() ->
  receive
    {out, Format, Args} ->
      io:format(user, Format, Args),
      f()
  end.

prop_test() ->
  OutServer = spawn(fun f/0),
  put(debug_pid, OutServer),
  simple_gossip_test_tools:stop_cluster(),
  simple_gossip_test_tools:init_netkernel(),
  application:ensure_all_started(simple_gossip),
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

  DebugPid = get(debug_pid),
  [rpc:call(Node, simple_gossip_server, set_debug, [DebugPid]) || Node <- Nodes],

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
      {10, ?RPC(RpcNode, set, [resize(150, term())])},
      {15, ?RPC(RpcNode, get, [])},
      {10, ?RPC(RandomNode, join, [RandomNode])},
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
postcondition(#{data := Data, in_cluster := ClusterNodes},
              {call, rpc, call, [_Node, _, get, []]}, Res) ->
  case Data /= Res of
    true ->
      dump_server_states(ClusterNodes),
      io:format("Different data -~n OnNode: ~p~n In cluster: ~p~n Expected data hash: ~p~n Got Data hash: ~p~n",
                [_Node, ClusterNodes, erlang:phash2(Data, 9999), erlang:phash2(Res, 9999)]),
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
  %io:format("Not handled commands: ~p~n", [{call, _Mod, _Fun, _Args}]),
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

dump_server_states(Nodes) ->
  {Pids, []} = rpc:multicall(Nodes, erlang, whereis, [simple_gossip_server]),
  [format_server_state(Pid) || Pid <- Pids].

format_server_state(Pid) ->
  {state, Rumor, _} = sys:get_state(Pid),
  io:format("~n============================================================~n"),
  io:format("Node:        ~p~n", [node(Pid)]),
  io:format("Leader:      ~p~n", [Rumor#rumor.leader]),
  io:format("GossipVsn:   ~p~n", [Rumor#rumor.gossip_version]),
  io:format("Datahash:    ~p~n", [erlang:phash2(Rumor#rumor.data, 9999)]),
  io:format("VectorClock: ~p~n", [Rumor#rumor.vector_clock]),
  io:format("Nodes:       ~p~n", [Rumor#rumor.nodes]).
