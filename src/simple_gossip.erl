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
-export([set/1, get/0, join/1, leave/1, status/0, subscribe/1, unsubscribe/1]).

-type set_fun() ::  fun((term()) -> {change, term()} | no_change).

-export_type([set_fun/0]).

%% @doc Set new rumor
-spec set(Status | fun((Status) -> {change, Status} | no_change)) -> ok when
  Status :: term().
set(Data) ->
  simple_gossip_server:set(Data).

%% @doc Get rumor
-spec get() -> term().
get() ->
  simple_gossip_server:get().

%% @doc Subscribe to rumor changes
-spec subscribe(pid()) -> ok.
subscribe(Pid) ->
  simple_gossip_notify:subscribe(Pid).

%% @doc Unsubscribe from rumor changes
-spec unsubscribe(pid()) -> ok.
unsubscribe(Pid) ->
  simple_gossip_notify:unsubscribe(Pid).

%% @doc Join node to cluster
-spec join(node()) -> ok.
join(Node) ->
  simple_gossip_server:join(Node).

%% @doc Leave cluster
-spec leave(node()) -> ok.
leave(Node) ->
  simple_gossip_server:leave(Node).

%% @doc Check cluster status
-spec status() ->
  {ok, Vsn, Leader, Nodes} | {error, {timeout, Nodes} | mismatch} when
  Vsn :: pos_integer(),
  Leader :: node(),
  Nodes :: [node()].
status() ->
  simple_gossip_server:status().
