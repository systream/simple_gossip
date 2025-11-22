%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, Systream Ltd
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(prop_simple_gossip).
-author("Peter Tihanyi").

-include_lib("proper/include/proper.hrl").

prop_set_get() ->
  simple_gossip_test_tools:init_netkernel(),
  simple_gossip_test_tools:stop_cluster(),
  application:stop(simple_gossip),
  simple_gossip_persist:delete_file(),
  application:ensure_all_started(simple_gossip),

  simple_gossip_test_tools:wait_to_reconcile(),
  simple_gossip_server:set_gossip_interval(5000),
  simple_gossip_server:set_max_gossip_per_period(25),
  ?FORALL(Payload,
          term(),
          begin
            simple_gossip:set(Payload),
            simple_gossip:get() == Payload
          end).

prop_set_get_with_fun() ->
  simple_gossip_test_tools:init_netkernel(),
  simple_gossip_test_tools:stop_cluster(),
  application:stop(simple_gossip),
  simple_gossip_persist:delete_file(),
  application:ensure_all_started(simple_gossip),
  simple_gossip_test_tools:wait_to_reconcile(),
  simple_gossip_server:set_gossip_interval(5000),
  simple_gossip_server:set_max_gossip_per_period(25),
  ?FORALL(Payload,
    term(),
    begin
      simple_gossip:set(fun(_) -> {change, Payload} end),
      simple_gossip:get() == Payload
    end).

prop_cluster() ->
  application:stop(simple_gossip),
  simple_gossip_persist:delete_file(),
  simple_gossip_test_tools:stop_cluster(),

  [Node1 | _] = Nodes = start_cluster(),

  application:ensure_all_started(simple_gossip),
  ok = simple_gossip_test_tools:wait_to_reconcile(Node1, 5000),
  [ok = rpc:call(Node1, simple_gossip, join, [Node], 5000) || Node <- Nodes, Node =/= Node1],
  ok = simple_gossip_test_tools:wait_to_reconcile(Node1, 15000),
  rpc:call(Node1, simple_gossip_server, set_gossip_interval, [2000], 5000),
  rpc:call(Node1, simple_gossip_server, set_max_gossip_per_period, [25], 5000),
  ?FORALL({Payload, Node, GetNode},
    {resize(500, term()), oneof(Nodes), oneof(Nodes)},
    begin
      rpc:call(Node, simple_gossip, set, [Payload]),
      simple_gossip_test_tools:wait_to_reconcile(Node, 200),
      RpcResult = rpc:call(GetNode, simple_gossip, get, []),
      R = Payload == RpcResult,
      case R of
        false ->
          io:format("Payload: ~p ~nRpc: ~p ~n", [Payload, RpcResult]),
          io:format("Setnode: ~p ~nGetNode: ~p ~n", [Node, GetNode]),
          io:format("status: ~p~n", [rpc:call(GetNode, simple_gossip, status, [])]),
          false;
        true ->
          true
      end
    end).

prop_cluster_stop_leader() ->
  simple_gossip_test_tools:stop_cluster(),
  [Node1 | _] = Nodes = start_cluster(),
  [ok = rpc:call(Node1, simple_gossip, join, [Node]) || Node <- Nodes],

  ok = simple_gossip_test_tools:wait_to_reconcile(Node1, 15000),
  {ok, _, Leader, _} = simple_gossip:status(),
  AvailableNodes = Nodes -- [Leader],
  rpc:call(Leader, application, stop, [simple_gossip]),
  ct_slave:stop(Leader),
  [OneNode | _] = AvailableNodes,
  rpc:call(OneNode, simple_gossip_server, set_gossip_interval, [2000], 5000),
  rpc:call(OneNode, simple_gossip_server, set_max_gossip_per_period, [25], 5000),

  ?FORALL({Payload, Node, GetNode},
    {term(), oneof(AvailableNodes), oneof(AvailableNodes)},
    begin
      Result = rpc:call(Node, simple_gossip, set, [Payload]),
      simple_gossip_test_tools:wait_to_reconcile(Node, 100),
      GNPayload = rpc:call(GetNode, simple_gossip, get, []),
      Result =:= ok andalso Payload =:= GNPayload
    end).

prop_cluster_kill_leader() ->
  simple_gossip_test_tools:stop_cluster(),
  simple_gossip_persist:delete_file(),
  application:ensure_all_started(simple_gossip),
  [Node1 | _] = Nodes = start_cluster(),
  [ok = rpc:call(Node1, simple_gossip, join, [Node]) || Node <- Nodes],

  % kill the leader,
  ok = simple_gossip_test_tools:wait_to_reconcile(Node1, 5000),
  {ok, _, Leader, _} = simple_gossip:status(),
  AvailableNodes = Nodes -- [Leader],
  rpc:call(Leader, erlang, exit, [whereis(simple_gossip_server), kill]),
  ct_slave:stop(Leader),
  [OneNode | _] = AvailableNodes,
  rpc:call(OneNode, simple_gossip_server, set_gossip_interval, [5000], 5000),
  rpc:call(OneNode, simple_gossip_server, set_max_gossip_per_period, [25], 5000),
  ?FORALL({Payload, TargetNode, GetNode},
    {term(), oneof(AvailableNodes), oneof(AvailableNodes)},
    begin
      Result = rpc:call(TargetNode, simple_gossip, set, [Payload]),
      Result == ok andalso Payload == rpc:call(GetNode, simple_gossip, get, [])
    end).

start_cluster() ->
  simple_gossip_test_tools:start_nodes(['prop1', 'prop2']).