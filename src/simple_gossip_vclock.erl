%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2023, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_vclock).

%% API
-export([new/0, increment/1, descendant/2, increment/2, clean/2]).

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
    case maps:get(Node, VClock, deleted) of
        {Counter, OldTs} -> VClock#{Node => {Counter + 1, max(OldTs, Timestamp) + 1}};
        deleted -> VClock#{Node => {1, Timestamp}};
        {Counter, OldTs, deleted} -> VClock#{Node => {Counter + 1, max(OldTs, Timestamp) + 1}}
    end.

-spec descendant(vclock(), vclock() | none | {node(), {pos_integer(), pos_integer()}, term()}) ->
    boolean().
descendant(VClockA, {Node, {CounterB, TSB}, Iterator}) ->
    case maps:get(Node, VClockA, not_found) of
        not_found ->
            false;
        {CounterA, _TSA} when CounterA > CounterB ->
            descendant(VClockA, maps:next(Iterator));
        {CounterA, TSA} when CounterA =:= CounterB andalso TSA =:= TSB ->
            descendant(VClockA, maps:next(Iterator));
        {_CounterA, _TSA, deleted} ->
            descendant(VClockA, maps:next(Iterator));
        _ ->
            false
    end;
descendant(VClockA, {_Node, {_CounterB, _TSB, deleted}, Iterator}) ->
    descendant(VClockA, maps:next(Iterator));
descendant(_VClockA, none) ->
    true;
descendant(VClockA, VClockB) when is_map(VClockB) ->
    descendant(VClockA, maps:next(maps:iterator(VClockB))).

-spec timestamp() -> pos_integer().
timestamp() ->
    erlang:system_time(nanosecond).

-spec clean(vclock(), [node()]) -> vclock().
clean(VClock, NodesList) ->
    % 5 minutes
    DropNodesAfter = timestamp() - 300_000_000_000,
    % We need to leave a deleted vclock as a thumb stone, at least some time
    maps:fold(fun(VClockNode, {Counter, TS, deleted}, VClockAcc) ->
                    case TS > DropNodesAfter of
                        true -> VClockAcc;
                        _ -> VClockAcc#{VClockNode => {Counter, TS, deleted}}
                    end;
                (VClockNode, {Counter, TS}, VClockAcc) ->
                    case lists:member(VClockNode, NodesList) of
                        true -> VClockAcc#{VClockNode => {Counter, TS}};
                        _ -> VClockAcc#{VClockNode => {Counter, TS, deleted}}
                    end
            end, #{}, VClock
    ).