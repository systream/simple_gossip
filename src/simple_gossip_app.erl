%%%-------------------------------------------------------------------
%% @doc simple_gossip public API
%% @end
%%%-------------------------------------------------------------------

-module(simple_gossip_app).

-behaviour(application).

-export([start/2, stop/1]).

-spec start(StartType :: application:start_type(), StartArgs :: term()) ->
    {'ok', pid()}.
start(_StartType, _StartArgs) ->
    simple_gossip_sup:start_link().

-spec stop(State :: term()) -> ok.
stop(_State) ->
    ok.

%% internal functions