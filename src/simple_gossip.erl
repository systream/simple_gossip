%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip).
-author("Peter Tihanyi").

%% API
-export([set/1, get/0, join/1, leave/1, status/0]).

-spec set(Status | fun((Status) -> {change, Status} | no_change)) -> ok when
  Status :: term().
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

-spec status() ->
  {ok, Vsn, Claimant, Nodes} | {error, timeout} | mismatch when
  Vsn :: pos_integer(),
  Claimant :: node(),
  Nodes :: [node()].
status() ->
  simple_gossip_server:status().
