-module(prop_vclock).
-include_lib("proper/include/proper.hrl").
-include("../src/simple_gossip.hrl").

%% Model Callbacks
-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3]).

%%%%%%%%%%%%%%%%%%
%%% PROPERTIES %%%
%%%%%%%%%%%%%%%%%%

prop_test() ->
  ?FORALL(Cmds, parallel_commands(?MODULE),
          begin
              {History, PHistory, Result} = run_parallel_commands(?MODULE, Cmds),
              ?WHENFAIL(io:format("History: ~p\nParalell History: ~p\nResult: ~p\n",
                                  [History, PHistory, Result]),
                        aggregate(command_names(Cmds), Result =:= ok))
          end).

%%%%%%%%%%%%%
%%% MODEL %%%
%%%%%%%%%%%%%
%% @doc Initial model value at system start. Should be deterministic.
initial_state() ->
  simple_gossip_vclock:new().

%% @doc List of possible commands to run against the system
command(VClock) ->
    frequency([
      {10, {call, simple_gossip_vclock, increment, [VClock, random_node()]}},
      {1, {call, simple_gossip_vclock, clean, [VClock, random_nodes()]}}
  ]).

%% @doc Determines whether a command should be valid under the
%% current state.
precondition(_State, {call, _Mod, _Fun, _Args}) ->
  true.

%% @doc Given the state `State' *prior* to the call
%% `{call, Mod, Fun, Args}', determine whether the result
%% `Res' (coming from the actual system) makes sense.
postcondition(VClock, {call, _Mod, _Fun, _Args} = _A, NewClock) ->
  simple_gossip_vclock:descendant(NewClock, VClock).

%% @doc Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(_VClock, Res, {call, simple_gossip_vclock, increment, [_, _]}) ->
  Res;
next_state(_VClock, Res, {call, simple_gossip_vclock, clean, [_, _]}) ->
  Res;

next_state(State, _Res, {call, _Mod, _Fun, _Args}) ->
  %io:format("Not handled command: ~p~n", [{call, _Mod, _Fun, _Args}]),
  State.

random_node() ->
  atom().

random_nodes() ->
  ?SUCHTHAT(Nodes, resize(10, list(random_node())), length(Nodes) > 1).