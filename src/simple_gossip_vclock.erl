%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_vclock).

%% API
-export([new/0, increment/1, descendant/2, increment/2, vsn/1]).

-type clocks() :: #{node() => {pos_integer(), pos_integer()}}.

-record(vclock, {
  vsn = 0 :: non_neg_integer(),
  clocks = #{} :: clocks()
}).

-type vclock() :: #vclock{}.

-export_type([vclock/0]).

-spec new() -> vclock().
new() ->
  #vclock{}.

-spec increment(vclock()) -> vclock().
increment(VClock) ->
  increment(VClock, node(), timestamp()).

-spec increment(vclock(), node()) -> vclock().
increment(VClock, Node) ->
  increment(VClock, Node, timestamp()).

-spec increment(vclock(), node(), pos_integer()) -> vclock().
increment(#vclock{clocks = Clock, vsn = Vsn} = VClock, Node, Timestamp) ->
  NewClocks = case maps:get(Node, Clock, not_found) of
                not_found -> Clock#{Node => {1, Timestamp}};
                {Counter, _OldTs} -> Clock#{Node => {Counter+1, Timestamp}}
              end,
  maybe_shrink(VClock#vclock{clocks = NewClocks, vsn = Vsn+1}).

-spec descendant(vclock() | clocks(), vclock() | clocks() | none | {node(), {pos_integer(), pos_integer()}, term()}) ->
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
descendant(#vclock{clocks = VClockA}, #vclock{clocks = VClockB}) ->
  descendant(VClockA, maps:next(maps:iterator(VClockB))).

-spec timestamp() -> pos_integer().
timestamp() ->
  erlang:system_time(nanosecond).

-spec vsn(vclock()) -> non_neg_integer().
vsn(#vclock{vsn = Vsn}) ->
  Vsn.

-spec maybe_shrink(vclock()) -> vclock().
maybe_shrink(#vclock{vsn = Vsn} = VClock) when Vsn rem 100 =:= 0 ->
  shrink(VClock, application:get_env(simple_gossip, shrink_time_threshold, timer:minutes(5)));
maybe_shrink(VClock) ->
  VClock.

-spec shrink(vclock(), pos_integer()) -> vclock().
shrink(#vclock{clocks = Clocks} = VClock, TimeThreshold) ->
  Time = timestamp() - (TimeThreshold*1000),
  VClock#vclock{clocks = maps:filter(fun(_, {_, Ts}) -> Ts >= Time end, Clocks)}.
