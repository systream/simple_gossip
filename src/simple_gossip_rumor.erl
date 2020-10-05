%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_rumor).
-author("Peter Tihanyi").

-include_lib("simple_gossip.hrl").

%% API
-export([new/0,
         increase_version/1,
         add_node/2,
         change_leader/1,
         remove_node/2,
         pick_random_nodes/2,
         check_node_exclude/1,
         if_member/3,
         if_not_member/3,
         set_data/2]).

-type manage_node_fun() ::  fun(() -> rumor()).
-type if_leader_node_fun() ::  fun((node()) -> rumor()).


-spec new() -> rumor().
new() ->
  #rumor{leader = node(),
         nodes = [node()],
         gossip_version = 1}.

-spec increase_version(rumor()) -> rumor().
increase_version(#rumor{gossip_version = GossipVersion} = Rumor) ->
  Rumor#rumor{gossip_version = GossipVersion+1}.

-spec add_node(rumor(), node()) -> rumor().
add_node(#rumor{nodes = Nodes} = Rumor, Node) ->
  if_not_member(Rumor, Node, fun() -> Rumor#rumor{nodes = [Node | Nodes]} end).

-spec set_data(rumor(), term()) -> rumor().
set_data(Rumor, Data) ->
  increase_version(Rumor#rumor{data = Data}).

-spec remove_node(rumor(), node()) -> rumor().
remove_node(#rumor{nodes = Nodes} = Rumor, Node) ->
  if_member(Rumor, Node,
            fun() ->
              NewRumor = Rumor#rumor{nodes = lists:delete(Node, Nodes)},
              if_leader(NewRumor, Node, fun promote_random_leader/1)
            end).

-spec check_node_exclude(rumor()) -> rumor().
check_node_exclude(Rumor) ->
  if_not_member(Rumor, node(), fun() -> new() end).

-spec pick_random_nodes([node()], non_neg_integer()) -> [node()].
pick_random_nodes(Nodes, Number) ->
  pick_random_nodes(Nodes, Number, []).

-spec pick_random_nodes([node()], non_neg_integer(), [node()]) -> [node()].
pick_random_nodes(Nodes, Number, Acc) when Nodes == [] orelse Number == 0 ->
  Acc;
pick_random_nodes(Nodes, Number, Acc) ->
  Node = lists:nth(rand:uniform(length(Nodes)), Nodes),
  pick_random_nodes(lists:delete(Node, Nodes), Number-1, [Node | Acc]).

-spec promote_random_leader(rumor()) -> rumor().
promote_random_leader(#rumor{nodes = Nodes} = Rumor)
  when Nodes =/= [] ->
  NewLeader = lists:nth(erlang:phash(Rumor, length(Nodes)), Nodes),
  increase_version(Rumor#rumor{leader = NewLeader});
promote_random_leader(Rumor) ->
  Rumor.

-spec change_leader(rumor()) -> rumor().
change_leader(#rumor{leader = Leader, nodes = Nodes} = Rumor) ->
  NewRumor = Rumor#rumor{nodes = lists:delete(Leader, Nodes)},
  promote_random_leader(NewRumor).

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
      increase_version(Fun());
    _ ->
      Rumor
  end.

-spec if_leader(rumor(), node(), if_leader_node_fun()) -> rumor().
if_leader(#rumor{leader = Leader} = Rumor, Leader, Fun) ->
  Fun(Rumor);
if_leader(Rumor, _, _Fun) ->
  Rumor.

