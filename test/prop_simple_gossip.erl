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
  simple_gossip_test_tools:stop_cluster(),
  simple_gossip_test_tools:init_netkernel(),
  application:ensure_all_started(simple_gossip),
  ?FORALL(Payload,
          term(),
          begin
            simple_gossip:set(Payload),
            simple_gossip:get() == Payload
          end).

prop_set_get_with_fun() ->
  application:ensure_all_started(simple_gossip),
  ?FORALL(Payload,
    term(),
    begin
      simple_gossip:set(fun(_) -> {change, Payload} end),
      simple_gossip:get() == Payload
    end).

prop_cluster() ->
  application:stop(simple_gossip),
  [Node1 | _] = Nodes = start_cluster(),
  application:ensure_all_started(simple_gossip),

  [rpc:call(Node1, simple_gossip, join, [Node]) || Node <- Nodes],
  ok = simple_gossip_test_tools:wait_to_reconcile(),

  ?FORALL({Payload, Node, GetNode},
    {resize(500, term()), oneof(Nodes), oneof(Nodes)},
    begin
      rpc:call(Node, simple_gossip, set, [Payload]),
      simple_gossip_test_tools:wait_to_reconcile(),
      Payload == rpc:call(GetNode, simple_gossip, get, [])
    end).

prop_cluster_stop_leader() ->
  [Node1 | _] = Nodes = start_cluster(),

  [rpc:call(Node1, simple_gossip, join, [Node]) || Node <- Nodes],

  % kill the leader,
  {ok, _, Leader, _} = simple_gossip:status(),
  AvailableNodes = Nodes -- [Leader],
  rpc:call(Leader, application, stop, [simple_gossip]),
  ct_slave:stop(Leader),
  ?FORALL({Payload, Node},
    {term(), oneof(AvailableNodes)},
    begin
      Result = rpc:call(Node, simple_gossip, set, [Payload]),
      Result == ok andalso Payload == simple_gossip:get()
    end).

prop_cluster_kill_leader() ->
  [Node1 | _] = Nodes = start_cluster(),

  [rpc:call(Node1, simple_gossip, join, [Node]) || Node <- Nodes],

  % kill the leader,
  {ok, _, Leader, _} = simple_gossip:status(),
  AvailableNodes = Nodes -- [Leader],
  rpc:call(Leader, erlang, exit, [whereis(simple_gossip_server), kill]),
  ct_slave:stop(Leader),
  ?FORALL({Payload, Node},
    {term(), oneof(AvailableNodes)},
    begin
      Result = rpc:call(Node, simple_gossip, set, [Payload]),
      Result == ok andalso Payload == simple_gossip:get()
    end).

start_cluster() ->
  simple_gossip_test_tools:start_nodes(['prop1', 'prop2', 'prop3', 'prop4']).