%%%-------------------------------------------------------------------
%%% @author tihanyipeter
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_SUITE).
-author("tihanyipeter").

%% API
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% Function: suite() -> Info
%% Info = [tuple()]
%%--------------------------------------------------------------------
suite() ->
  [{timetrap,{seconds,30}}].

%%--------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------
init_per_suite(Config) ->
  application:ensure_all_started(simple_gossip),
  net_kernel:set_net_ticktime(5),
  Config.

%%--------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> term() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
  application:stop(simple_gossip),
  ok.

%%--------------------------------------------------------------------
%% Function: init_per_group(GroupName, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------
init_per_group(_GroupName, Config) ->
  Config.

%%--------------------------------------------------------------------
%% Function: end_per_group(GroupName, Config0) ->
%%               term() | {save_config,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%%--------------------------------------------------------------------
end_per_group(_GroupName, _Config) ->
  ok.

%%--------------------------------------------------------------------
%% Function: init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
  Config.

%%--------------------------------------------------------------------
%% Function: end_per_testcase(TestCase, Config0) ->
%%               term() | {save_config,Config1} | {fail,Reason}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
  ok.

%%--------------------------------------------------------------------
%% Function: groups() -> [Group]
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%% Shuffle = shuffle | {shuffle,{integer(),integer(),integer()}}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%% N = integer() | forever
%%--------------------------------------------------------------------
groups() ->
  [].

%%--------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases | {skip,Reason}
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%% TestCase = atom()
%% Reason = term()
%%--------------------------------------------------------------------
all() ->
  [ set_get,
    set_get_via_fun,
    set_get_via_fun_no_change,
    join_node,
    sync_between_nodes,
    sync_connecting_node
  ].

%%--------------------------------------------------------------------
%% Function: TestCase(Config0) ->
%%               ok | exit() | {skip,Reason} | {comment,Comment} |
%%               {save_config,Config1} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% Comment = term()
%%--------------------------------------------------------------------
set_get(_Config) ->
  Test = <<"test">>,
  simple_gossip:set(Test),
  ?assertEqual(Test, simple_gossip:get()).

set_get_via_fun(_Config) ->
  simple_gossip:set("Prev"),
  simple_gossip:set(fun(State) ->
    ?assertEqual("Prev", State),
    {change, "Next"}
                    end),
  ?assertEqual("Next", simple_gossip:get()).

set_get_via_fun_no_change(_Config) ->
  application:ensure_all_started(simple_gossip),
  simple_gossip:set("Prev"),
  simple_gossip:set(fun(State) ->
    ?assertEqual("Prev", State),
    no_change
                    end),
  ?assertEqual("Prev", simple_gossip:get()).

join_node(_Config) ->
  simple_gossip:set("test_11"),
  NodeName = 'test1',
  Host = start_slave_node(NodeName),
  simple_gossip:join(Host),
  ct:sleep(10), % wait a bit to new host process the message
  ?assertEqual("test_11", rpc:call(Host, simple_gossip, get, [])),
  simple_gossip:leave(Host),
  ct_slave:stop(Host).


sync_between_nodes(_Config) ->
  simple_gossip:set("default"),
  Node1 = start_slave_node('sync_1'),
  Node2 = start_slave_node('sync_2'),
  Node3 = start_slave_node('sync_3'),
  simple_gossip:join(Node1),
  simple_gossip:join(Node2),
  simple_gossip:join(Node3),

  simple_gossip:set("main"),
  ct:sleep(10), % wait a bit to new hosts process the messag

  ?assertEqual("main", rpc:call(Node1, simple_gossip, get, [])),
  ?assertEqual("main", rpc:call(Node2, simple_gossip, get, [])),
  ?assertEqual("main", rpc:call(Node3, simple_gossip, get, [])),

  rpc:call(Node1, simple_gossip, set, ["main1"]),
  ct:sleep(10), % wait a bit to new hosts process the messag

  ?assertEqual("main1", simple_gossip:get()),
  ?assertEqual("main1", rpc:call(Node2, simple_gossip, get, [])),
  ?assertEqual("main1", rpc:call(Node3, simple_gossip, get, [])),

  rpc:call(Node2, simple_gossip, set, ["main2"]),
  ct:sleep(10), % wait a bit to new hosts process the messag

  ?assertEqual("main2", simple_gossip:get()),
  ?assertEqual("main2", rpc:call(Node1, simple_gossip, get, [])),
  ?assertEqual("main2", rpc:call(Node3, simple_gossip, get, [])),

  simple_gossip:leave(Node1),
  simple_gossip:leave(Node2),
  simple_gossip:leave(Node3),

  ct_slave:stop(Node1),
  ct_slave:stop(Node2),
  ct_slave:stop(Node3).


sync_connecting_node(_Config) ->
  simple_gossip:set("default"),
  Node1 = start_slave_node('sync_1'),

  rpc:call(Node1, simple_gossip, join, [node()]),

  ?assertEqual("default", simple_gossip:get()),

  simple_gossip:set("main"),
  ct:sleep(10), % wait a bit to new hosts process the messag

  ?assertEqual("main", simple_gossip:get()),
  ?assertEqual("main", rpc:call(Node1, simple_gossip, get, [])),

  simple_gossip:leave(Node1),

  ct_slave:stop(Node1).


start_slave_node(NodeName) ->
  ErlFlags = "-pa ../../../../_build/test/lib/*/ebin ",
  {ok, HostNode} = ct_slave:start(NodeName,
    [ {kill_if_fail, true},
      {monitor_master, true},
      {init_timeout, 5},
      {boot_timeout, 5},
      {startup_timeout, 5},
      {startup_functions, [{application, ensure_all_started, [simple_gossip]}]},
      {erl_flags, ErlFlags}]),
  HostNode.

