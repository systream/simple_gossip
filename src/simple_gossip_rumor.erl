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
  new/1,
  head/1,
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
  check_vector_clocks/2,
  change_max_gossip_per_period/2,
  change_gossip_interval/2,
  calculate_new_leader/1,
  calculate_new_leader/2, version/1]).

-type manage_node_fun() ::  fun(() -> rumor()).
-type if_leader_node_fun() ::  fun((node()) -> rumor()).

-export_type([manage_node_fun/0]).

-spec new() -> rumor().
new() ->
  #rumor{leader = node(),
         nodes = [node()],
         gossip_version = 1,
         vector_clock = simple_gossip_vclock:new()}.

-spec new(rumor()) -> rumor().
new(#rumor{vector_clock = VectorClocks}) ->
  #rumor{leader = node(),
         nodes = [node()],
         gossip_version = 1,
         vector_clock = VectorClocks}.

-spec head(rumor()) -> rumor_head().
head(#rumor{gossip_version = GossipVersion, vector_clock = VectorClock}) ->
  #rumor_head{gossip_version = GossipVersion, vector_clock = VectorClock}.

-spec change_max_gossip_per_period(rumor(), pos_integer()) -> rumor().
change_max_gossip_per_period(#rumor{} = Rumor, Period) ->
  increase_version(Rumor#rumor{max_gossip_per_period = Period}).

-spec change_gossip_interval(rumor(), pos_integer()) -> rumor().
change_gossip_interval(#rumor{} = Rumor, Interval) ->
  increase_version(Rumor#rumor{gossip_period = Interval}).

-spec increase_gossip_version(rumor()) -> rumor().
increase_gossip_version(#rumor{gossip_version = GossipVersion} = Rumor) ->
  Rumor#rumor{gossip_version = GossipVersion + 1}.

-spec increase_vector_clock(rumor()) -> rumor().
increase_vector_clock(#rumor{vector_clock = VectorClock} = Rumor) ->
  Rumor#rumor{vector_clock = simple_gossip_vclock:increment(VectorClock)}.

-spec increase_version(rumor()) -> rumor().
increase_version(#rumor{} = Rumor) ->
  increase_vector_clock(increase_gossip_version(Rumor)).

-spec add_node(rumor(), node()) -> rumor().
add_node(#rumor{nodes = Nodes} = Rumor, Node) ->
  if_not_member(Rumor, Node,
                fun() ->
                  increase_version(Rumor#rumor{nodes = [Node | Nodes]})
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

-spec version(rumor_head() | rumor()) -> pos_integer().
version(#rumor{gossip_version = Version}) ->
  Version;
version(#rumor_head{gossip_version = Version}) ->
  Version.

-spec remove_node(rumor(), node()) -> rumor().
remove_node(#rumor{nodes = Nodes} = Rumor, Node) ->
  if_member(Rumor, Node,
            fun() ->
              NewRumor = increase_version(
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
                  increase_vector_clock(new(Rumor))
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
  increase_version(Rumor#rumor{leader = NewLeader});
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

-spec check_vector_clocks(In :: rumor(), Current :: rumor()) -> boolean().
check_vector_clocks(#rumor{vector_clock = InVectorClocks},
                    #rumor{vector_clock = CurrentVectorClocks}) ->
  simple_gossip_vclock:descendant(InVectorClocks, CurrentVectorClocks).
