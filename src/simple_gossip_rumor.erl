%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_rumor).

-compile({no_auto_import, [nodes/1]}).

-include("simple_gossip.hrl").
-include_lib("kernel/include/logger.hrl").

%% API
-export([new/0,
  add_node/2,
  change_leader/1,
  remove_node/2,
  nodes/1,
  pick_random_nodes/2,
  check_node_exclude/1,
  if_member/3,
  if_not_member/3,
  set_data/2,
  data/1,
  descendant/2,
  change_max_gossip_per_period/2,
  change_gossip_interval/2,
  calculate_new_leader/1,
  calculate_new_leader/2, vsn/1, descendant_cluster/2, cluster_vsn/1]).

-type manage_node_fun() ::  fun(() -> rumor()).
-type if_leader_node_fun() ::  fun((node()) -> rumor()).

-export_type([manage_node_fun/0]).

-spec new() -> rumor().
new() ->
  #rumor{leader = node(),
         nodes = [node()],
         cluster_vclock = simple_gossip_vclock:new(),
         data_vclock = simple_gossip_vclock:new()}.

-spec change_max_gossip_per_period(rumor(), pos_integer()) -> rumor().
change_max_gossip_per_period(#rumor{} = Rumor, Period) ->
  increase_cluster_version(Rumor#rumor{max_gossip_per_period = Period}).

-spec change_gossip_interval(rumor(), pos_integer()) -> rumor().
change_gossip_interval(#rumor{} = Rumor, Interval) ->
  increase_cluster_version(Rumor#rumor{gossip_period = Interval}).

-spec increase_version(rumor()) -> rumor().
increase_version(#rumor{data_vclock = VectorClock} = Rumor) ->
  Rumor#rumor{data_vclock = simple_gossip_vclock:increment(VectorClock)}.

-spec increase_cluster_version(rumor()) -> rumor().
increase_cluster_version(#rumor{cluster_vclock = VectorClock} = Rumor) ->
  Rumor#rumor{cluster_vclock = simple_gossip_vclock:increment(VectorClock)}.

-spec add_node(rumor(), node()) -> rumor().
add_node(#rumor{nodes = Nodes} = Rumor, Node) ->
  if_not_member(Rumor, Node,
                fun() ->
                  increase_cluster_version(Rumor#rumor{nodes = [Node | Nodes]})
                end
  ).

-spec nodes(rumor()) -> [node()].
nodes(#rumor{nodes = Nodes}) ->
  Nodes.

-spec set_data(rumor(), term()) -> rumor().
set_data(#rumor{} = Rumor, Data) ->
  increase_version(Rumor#rumor{data = Data}).

-spec data(rumor()) -> term().
data(#rumor{data = Data}) ->
  Data.

-spec remove_node(rumor(), node()) -> rumor().
remove_node(#rumor{nodes = Nodes} = Rumor, Node) ->
  if_member(Rumor, Node,
            fun() ->
              NewRumor = increase_cluster_version(
                Rumor#rumor{nodes = lists:delete(Node, Nodes)}
              ),
              if_leader(NewRumor, Node, fun change_leader/1)
            end).

-spec check_node_exclude(rumor()) -> rumor().
check_node_exclude(#rumor{} = Rumor) ->
  if_not_member(Rumor, node(),
                fun() ->
                  ?LOG_INFO("New rumor created because"
                            "the incoming rumor not contains the current node"),
                  increase_cluster_version(Rumor)
                end).

-spec pick_random_nodes([node()], non_neg_integer()) -> [node()].
pick_random_nodes(Nodes, Number) ->
  pick_random_nodes(Nodes, Number, []).

-spec pick_random_nodes([node()], non_neg_integer(), [node()]) -> [node()].
pick_random_nodes(Nodes, Number, Acc) when Nodes == [] orelse Number == 0 ->
  Acc;
pick_random_nodes(Nodes, Number, Acc) ->
  Node = lists:nth(rand:uniform(length(Nodes)), Nodes),
  pick_random_nodes(lists:delete(Node, Nodes), Number - 1, [Node | Acc]).

-spec change_leader(rumor()) -> rumor().
change_leader(#rumor{nodes = Nodes, leader = Leader} = Rumor)
  when Nodes =/= [] andalso Nodes =/= [Leader] ->
  NewLeader = calculate_new_leader(Rumor),
  increase_cluster_version(Rumor#rumor{leader = NewLeader});
change_leader(Rumor) ->
  Rumor.

-spec calculate_new_leader(rumor()) -> node().
calculate_new_leader(Rumor) ->
  calculate_new_leader(Rumor, []).

-spec calculate_new_leader(rumor(), [node()]) -> node().
calculate_new_leader(#rumor{nodes = Nodes, leader = Leader}, ExcludeNodes) when
  is_list(ExcludeNodes) ->
  [_ | _] = ONodes = lists:usort((Nodes -- [Leader]) -- ExcludeNodes),
  lists:nth(erlang:phash2(ONodes, length(ONodes)) + 1, ONodes).

-spec if_not_member(rumor(), node(), manage_node_fun()) -> rumor().
if_not_member(Rumor, Node, Fun) ->
  if_member(Rumor, Node, Fun, false).

-spec if_member(rumor(), node(), manage_node_fun()) -> rumor().
if_member(Rumor, Node, Fun) ->
  if_member(Rumor, Node, Fun, true).

-spec if_member(rumor(), node(), manage_node_fun(), boolean()) -> rumor().
if_member(#rumor{nodes = Nodes} = Rumor, Node, Fun, Type) ->
  case lists:member(Node, Nodes) of
    Type ->
      Fun();
    _ ->
      Rumor
  end.

-spec if_leader(rumor(), node(), if_leader_node_fun()) -> rumor().
if_leader(#rumor{leader = Leader} = Rumor, Leader, Fun) ->
  Fun(Rumor);
if_leader(Rumor, _, _Fun) ->
  Rumor.

-spec descendant(In :: rumor(), Current :: rumor()) -> boolean().
descendant(#rumor{data_vclock = InVectorClocks},
           #rumor{data_vclock = CurrentVectorClocks}) ->
  simple_gossip_vclock:descendant(InVectorClocks, CurrentVectorClocks).

-spec descendant_cluster(In :: rumor(), Current :: rumor()) -> boolean().
descendant_cluster(#rumor{cluster_vclock = InVectorClocks},
                   #rumor{cluster_vclock = CurrentVectorClocks}) ->
  simple_gossip_vclock:descendant(InVectorClocks, CurrentVectorClocks).

-spec vsn(rumor()) -> non_neg_integer().
vsn(#rumor{data_vclock = VectorClocks} = Rumor) ->
  <<(integer_to_binary(cluster_vsn(Rumor)))/binary, ".",
    (integer_to_binary(simple_gossip_vclock:vsn(VectorClocks)))/binary>>.

-spec cluster_vsn(rumor()) -> non_neg_integer().
cluster_vsn(#rumor{cluster_vclock = VectorClocks}) ->
  simple_gossip_vclock:vsn(VectorClocks).

