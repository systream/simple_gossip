%%%-------------------------------------------------------------------
%%% @author tihanyipeter
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip).
-author("tihanyipeter").

%% API
-export([set/1, get/0, join/1, leave/1, status/0]).


-spec set(term()) -> ok.
set(Data) ->
  simple_gossip_server:set(Data).

-spec get() -> term().
get() ->
  simple_gossip_server:get().

-spec join(node()) -> ok.
join(Node) ->
  simple_gossip_server:join(Node).

-spec leave(node()) -> ok.
leave(Node) ->
  simple_gossip_server:leave(Node).

-spec status() -> {ok, Vsn :: pos_integer(), Claimant :: node(), Nodes :: [node()]} | {error, timeout} | mismatch.
status() ->
  simple_gossip_server:status().
