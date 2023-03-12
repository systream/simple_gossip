%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip).
-author("Peter Tihanyi").
-include("simple_gossip.hrl").

%% API
-export([set/1, get/0, join/1, leave/1, status/0, subscribe/1, unsubscribe/1]).

-export([subscribe/2]).

-type set_fun() :: fun((term()) -> {change, term()} | no_change).

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

%% @doc Subscribe to data changes
-spec subscribe(pid()) -> ok.
subscribe(Pid) ->
  simple_gossip_event:subscribe(Pid).

%% @doc Subscribe to data or rumor changes
-spec subscribe(pid(), Type :: data | rumor) -> ok.
subscribe(Pid, Type) ->
  simple_gossip_event:subscribe(Pid, Type).

%% @doc Unsubscribe from rumor changes
-spec unsubscribe(pid()) -> ok.
unsubscribe(Pid) ->
  simple_gossip_event:unsubscribe(Pid).

%% @doc Join node to cluster
-spec join(node()) -> ok.
join(Node) ->
  simple_gossip_server:join(Node).

%% @doc Leave cluster
-spec leave(node()) -> ok.
leave(Node) ->
  simple_gossip_server:leave(Node).

%% @doc Check cluster status
-spec status() -> {ok, Vsn, Leader, Nodes} |
                  {error, {timeout, Nodes} | gossip_vsn_mismatch, Leader, Nodes}
              when
  Vsn :: pos_integer(),
  Leader :: node(),
  Nodes :: [node()].
status() ->
  simple_gossip_server:status().
