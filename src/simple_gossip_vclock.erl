%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_vclock).

%% API
-export([new/0, increment/1, descendant/2, increment/2]).

-type vclock() :: map().

-export_type([vclock/0]).

-spec new() -> vclock().
new() ->
  #{}.

-spec increment(vclock()) -> vclock().
increment(VClock) ->
  increment(VClock, node(), timestamp()).

-spec increment(vclock(), node()) -> vclock().
increment(VClock, Node) ->
  increment(VClock, Node, timestamp()).

-spec increment(vclock(), node(), pos_integer()) -> vclock().
increment(VClock, Node, Timestamp) ->
  case maps:get(Node, VClock, not_found) of
    not_found -> VClock#{Node => {1, Timestamp}};
    {Counter, _OldTs} -> VClock#{Node => {Counter + 1, Timestamp}}
  end.

-spec descendant(vclock(), vclock() | none | {node(), {pos_integer(), pos_integer()}, term()}) ->
  boolean().
descendant(VClockA, {Node, {CounterB, TSB}, Iterator}) ->
  case maps:get(Node, VClockA, not_found) of
    not_found -> false;
    {CounterA, _TSA} when CounterA > CounterB ->
      descendant(VClockA, maps:next(Iterator));
    {CounterA, TSA} when CounterA =:= CounterB andalso TSA =:= TSB ->
      descendant(VClockA, maps:next(Iterator));
    _ ->
      false
  end;
descendant(_VClockA, none) ->
  true;
descendant(VClockA, VClockB) when is_map(VClockB) ->
  descendant(VClockA, maps:next(maps:iterator(VClockB))).

-spec timestamp() -> pos_integer().
timestamp() ->
  erlang:system_time(nanosecond).