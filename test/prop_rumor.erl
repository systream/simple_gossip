%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, Systream Ltd
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(prop_rumor).
-author("Peter Tihanyi").

-include_lib("proper/include/proper.hrl").
-include("../src/simple_gossip.hrl").

prop_change_leader() ->
  ?FORALL(Nodes,
          rumor_nodes(),
          begin
            Rumor = simple_gossip_rumor:new(),
            FullNodes = [node() | Nodes],
            NewRumor1 =
              Rumor#rumor{nodes = FullNodes, leader = node()},
            NewRumor2 =
              Rumor#rumor{nodes = random_list(FullNodes), leader = node()},

            NewLeader1 = simple_gossip_rumor:calculate_new_leader(NewRumor1),
            NewLeader2 = simple_gossip_rumor:calculate_new_leader(NewRumor2),

            lists:member(NewLeader1, FullNodes) andalso
            lists:member(NewLeader2, FullNodes) andalso
            NewLeader1 == NewLeader2 andalso
            NewLeader1 =/= node()
          end).

rumor_nodes() ->
  ?SUCHTHAT(Nodes, resize(100, list(atom())), length(Nodes) > 1).

random_list(List) ->
  [X || {_,X} <- lists:sort([{rand:uniform(), N} || N <- List])].
