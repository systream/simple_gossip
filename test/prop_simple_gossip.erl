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

disabled_prop_cluster() ->
  application:stop(simple_gossip),
  net_kernel:start([proper, shortnames]),
  application:ensure_all_started(simple_gossip),
  Node1 = simple_gossip_SUITE:start_slave_node('prop1'),
  Node2 = simple_gossip_SUITE:start_slave_node('prop2'),

  Nodes = [Node1, Node2],
  [simple_gossip:join(Node) || Node <- Nodes],

  timer:sleep(100),
  wait_until_cluster_state(),
  io:format("Status: ~p~n", [simple_gossip:status()]),

  ?FORALL({Payload, Node},
    {term(), oneof([node() | Nodes])},
    begin
      rpc:call(Node, simple_gossip, set, [Payload]),
      Payload == simple_gossip:get()
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
      timer:sleep(1000),
      wait_until_cluster_state(N-1)
  end.