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
  wait_until_cluster_state(),

  ?FORALL({Payload, Node},
    {term(), oneof(Nodes)},
    begin
      rpc:call(Node, simple_gossip, set, [Payload]),
      Payload == simple_gossip:get()
    end).

prop_cluster_stop_leader() ->
  [Node1 | _] = Nodes = start_cluster(),

  [rpc:call(Node1, simple_gossip, join, [Node]) || Node <- Nodes],

  % kill the leader,
  {ok, _, Leader, _} = simple_gossip:status(),
  AvailableNodes = Nodes -- [Leader],
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


wait_until_cluster_state() ->
  wait_until_cluster_state(10).

wait_until_cluster_state(N) ->
  case simple_gossip:status() of
    {ok, _, _, _} ->
      ok;
    Else when N == 0 ->
      Else;
    _ ->
      timer:sleep(10),
      wait_until_cluster_state(N-1)
  end.

start_cluster() ->
  case net_kernel:start([proper, shortnames]) of
    {error, {already_started, _}} ->
      ok;
    {ok, _} ->
      application:stop(simple_gossip),
      application:ensure_all_started(simple_gossip)
  end,

  Node1 = simple_gossip_SUITE:start_slave_node('prop1'),
  Node2 = simple_gossip_SUITE:start_slave_node('prop2'),
  Node3 = simple_gossip_SUITE:start_slave_node('prop3'),
  Node4 = simple_gossip_SUITE:start_slave_node('prop4'),

  [Node1, Node2, Node3, Node4, node()].