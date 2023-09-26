%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_SUITE).
-author("Pete Tihanyi").

%% API
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../src/simple_gossip.hrl").

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
  application:ensure_all_started(simple_gossip),
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
    notify_change,
    notify_change_not_on_leader,
    join_node,
    leave_self,
    leave_by_another_node,
    sync_between_nodes,
    sync_connecting_node,
    leave_during_nodedown,
    status,
    status_timeout,
    stop_node,
    stop_leader_node,
    start_from_persist_data,
    serve_persist_from_memory,
    vclock
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

notify_change(_Config) ->
  application:ensure_all_started(simple_gossip),
  Ref = make_ref(),
  ok = simple_gossip:subscribe(self()),
  ok = simple_gossip:subscribe(self()), % check double subscribe -> should no effect
  simple_gossip:set({"new", Ref}),
  ?assertMatch({ok, _}, receive_notify(Ref)),

  simple_gossip:unsubscribe(self()),

  Ref2 = make_ref(),
  simple_gossip:set({"new2", Ref2}),
  ?assertMatch(timeout, receive_notify(Ref)),
  simple_gossip:unsubscribe(self()),

  simple_gossip:subscribe(self(), rumor),
  simple_gossip:set(test),
  ?assertMatch({ok, #rumor{data = test}}, receive_rumor_notification()),
  simple_gossip:unsubscribe(self()).

notify_change_not_on_leader(_Config) ->
  NodeName = 'notify1',
  Host = simple_gossip_test_tools:start_slave_node(NodeName),
  ok = simple_gossip:join(Host),
  simple_gossip_test_tools:wait_to_reconcile(),
  ok = rpc:call(Host, simple_gossip, subscribe, [self()]),
  ok = rpc:call(Host, simple_gossip, subscribe, [self()]),

  spawn(fun() ->
          timer:sleep(10),
          simple_gossip:subscribe(self())
        end),

  spawn(fun() ->
          timer:sleep(10),
          ok = rpc:call(Host, simple_gossip, subscribe, [self()])
        end),

  ok = simple_gossip:subscribe(self()),

  Ref = make_ref(),
  simple_gossip:set({"test", Ref}),
  ?assertMatch({ok, _}, receive_notify(Ref)),
  ?assertMatch({ok, _}, receive_notify(Ref)),

  simple_gossip:leave(Host),
  ct_slave:stop(Host),
  ok.

join_node(_Config) ->
  simple_gossip:set("test_11"),
  NodeName = 'test1',
  Host = simple_gossip_test_tools:start_slave_node(NodeName),
  ok = simple_gossip:join(Host),
  simple_gossip_test_tools:wait_to_reconcile(),
  ?assertEqual("test_11", rpc:call(Host, simple_gossip, get, [])),
  simple_gossip:leave(Host),
  ct_slave:stop(Host),

  {error, {could_not_connect_to_node, 'not_existing@host'}} =
    simple_gossip:join('not_existing@host').

leave_self(_Config) ->
  ?assertEqual(ok, simple_gossip:leave(node())),
  Node = node(),
  ?assertMatch({ok, _, Node, [Node]}, simple_gossip:status()).

leave_by_another_node(_Config) ->
  NodeName = 'test_leave_test',
  Node = simple_gossip_test_tools:start_slave_node(NodeName),
  ok = simple_gossip:join(Node),
  simple_gossip_test_tools:wait_to_reconcile(),
  SelfNode = node(),
  ok = rpc:call(Node, simple_gossip, leave, [SelfNode]),
  simple_gossip_test_tools:wait_to_reconcile(),
  ?assertMatch({ok, _, SelfNode, [SelfNode]}, simple_gossip:status()),
  simple_gossip:leave(Node),
  ct_slave:stop(Node).

sync_between_nodes(_Config) ->
  simple_gossip:set("default"),
  Node1 = simple_gossip_test_tools:start_slave_node('sync_1'),
  Node2 = simple_gossip_test_tools:start_slave_node('sync_2'),
  ok = simple_gossip:join(Node1),
  ok = simple_gossip:join(Node2),

  simple_gossip:set("main"),
  simple_gossip_test_tools:wait_to_reconcile(),

  ?assertEqual("main", rpc:call(Node1, simple_gossip, get, [])),
  ?assertEqual("main", rpc:call(Node2, simple_gossip, get, [])),

  ok = rpc:call(Node1, simple_gossip, set, ["main1"]),
  simple_gossip_test_tools:wait_to_reconcile(),

  ?assertEqual("main1", simple_gossip:get()),
  ?assertEqual("main1", rpc:call(Node2, simple_gossip, get, [])),

  ok = rpc:call(Node2, simple_gossip, set, ["main2"]),
  simple_gossip_test_tools:wait_to_reconcile(),

  ?assertEqual("main2", simple_gossip:get()),
  ?assertEqual("main2", rpc:call(Node1, simple_gossip, get, [])),

  ct:pal("leave: ~p~n", [simple_gossip_server:status()]),
  simple_gossip:leave(Node1),
  ct:pal("leave: ~p~n", [simple_gossip_server:status()]),
  simple_gossip:leave(Node2),
  ct:pal("leave: ~p~n", [simple_gossip_server:status()]),

  ct_slave:stop(Node1),
  ct_slave:stop(Node2).


status(_Config) ->
  simple_gossip:set("default"),
  Node1 = simple_gossip_test_tools:start_slave_node('status_1'),
  ok = simple_gossip:join(Node1),
  simple_gossip:set("a"),
  simple_gossip_test_tools:wait_to_reconcile(),
  Node = node(),
  ?assertMatch({ok, _, Node, _}, simple_gossip:status()),
  simple_gossip:leave(Node1),
  ct_slave:stop(Node1).

status_timeout(_Config) ->
  simple_gossip:set("default"),
  Node = simple_gossip_test_tools:start_slave_node('status_timeout_node'),
  ok = simple_gossip:join(Node),
  simple_gossip_test_tools:stop_cluster([Node]),
  CurrentNode = node(),
  ?assertMatch({error, {timeout, [Node]}, CurrentNode, [Node, CurrentNode]},
               simple_gossip:status()),
  simple_gossip:leave(Node).

stop_node(_Config) ->
  simple_gossip:set("default"),
  Node1 = simple_gossip_test_tools:start_slave_node('stop_1'),
  ok = simple_gossip:join(Node1),
  simple_gossip_test_tools:wait_to_reconcile(),
  simple_gossip:set(test),
  Node = node(),
  ct_slave:stop(Node1),
  timer:sleep(10),
  simple_gossip:leave(Node1),
  simple_gossip_test_tools:wait_to_reconcile(),
  ?assertMatch({ok, _, Node, _}, simple_gossip:status()).


stop_leader_node(_Config) ->
  simple_gossip:set("default"),
  Node1 = simple_gossip_test_tools:start_slave_node('stop_2'),
  ok = simple_gossip:join(Node1),
  simple_gossip_test_tools:wait_to_reconcile(),
  simple_gossip:set(test),
  Node = node(),

  application:stop(simple_gossip),
  timer:sleep(100),
  ok = rpc:call(Node1, simple_gossip, leave, [Node]),
  timer:sleep(100),
  ?assertMatch({ok, _, Node1, _}, rpc:call(Node1, simple_gossip, status, [])),
  ct_slave:stop(Node1).


sync_connecting_node(_Config) ->
  simple_gossip:set("default"),
  Node1 = simple_gossip_test_tools:start_slave_node('sync_1'),

  ok = rpc:call(Node1, simple_gossip, join, [node()]),

  ?assertEqual("default", simple_gossip:get()),

  simple_gossip:set("main"),
  simple_gossip_test_tools:wait_to_reconcile(),

  ?assertEqual("main", simple_gossip:get()),
  ?assertEqual("main", rpc:call(Node1, simple_gossip, get, [])),

  simple_gossip:leave(Node1),

  ct_slave:stop(Node1).

leave_during_nodedown(_Config) ->
  Node1 = simple_gossip_test_tools:start_slave_node('split_1'),

  ok = rpc:call(Node1, simple_gossip, join, [node()]),

  simple_gossip:set("synctest"),
  simple_gossip_test_tools:wait_to_reconcile(),

  Nodes = get_nodes(simple_gossip:status()),
  ?assertEqual(Nodes, get_nodes(rpc:call(Node1, simple_gossip, status, []))),
  rpc:call(Node1, application, stop, [simple_gossip]),
  simple_gossip:leave(Node1),

  CurrentNode = node(),
  ?assertEqual([CurrentNode], get_nodes(simple_gossip:status())),
  rpc:call(Node1, application, ensure_all_started, [simple_gossip]),

  ct:sleep(100),

  ?assertEqual([Node1], get_nodes(rpc:call(Node1, simple_gossip, status, []))),

  ct_slave:stop(Node1).

get_nodes({ok, _Vsn, _Leader, Nodes}) ->
  Nodes;
get_nodes({error, _Reason, _Leader, Nodes}) ->
  Nodes.



start_from_persist_data(_Config) ->
  simple_gossip:set("from file test"),
  simple_gossip_test_tools:wait_to_reconcile(),
  [ok = simple_gossip:leave(Node) || Node <- nodes()],
  simple_gossip_test_tools:stop_cluster(),
  application:stop(simple_gossip),
  application:ensure_all_started(simple_gossip),
  ?assertEqual("from file test", simple_gossip:get()).

serve_persist_from_memory(_Config) ->
  ct:sleep(2000), % make sure data persisted
  simple_gossip:set("test_from_not_persisted_data"),
  ct:sleep(10), % make sure the event reach the persister service
  ?assertMatch({ok, {rumor, _, _, "test_from_not_persisted_data", _, _, _, _}},
               simple_gossip_persist:get()),
  % after persist data
  ?assertMatch({ok, {rumor, _, _, "test_from_not_persisted_data", _, _, _, _}},
               simple_gossip_persist:get()).

vclock(_Config) ->
  A = simple_gossip_vclock:new(),
  ?assertEqual(0, simple_gossip_vclock:vsn(A)),
  B = simple_gossip_vclock:new(),
  ?assert(simple_gossip_vclock:descendant(A, B)),
  ?assert(simple_gossip_vclock:descendant(B, A)),
  A1_1 = simple_gossip_vclock:increment(A, node_a),
  A1_2 = simple_gossip_vclock:increment(A, node_b),
  ?assert(simple_gossip_vclock:descendant(A1_1, A)),
  ?assert(simple_gossip_vclock:descendant(A1_2, A)),
  ?assertNot(simple_gossip_vclock:descendant(A1_2, A1_1)),
  ?assertNot(simple_gossip_vclock:descendant(A1_1, A1_2)),
  A2 = simple_gossip_vclock:increment(A1_1, node_b),
  ?assert(simple_gossip_vclock:descendant(A2, A1_1)),
  ?assert(simple_gossip_vclock:descendant(A2, A)),
  ?assertNot(simple_gossip_vclock:descendant(A2, A1_2)),
  ?assertEqual(2, simple_gossip_vclock:vsn(A2)),

  Shrink = lists:foldl(fun(I, Vc) ->
                  simple_gossip_vclock:increment(Vc, I rem 5)
                end, simple_gossip_vclock:new(), lists:seq(1, 199)),
  application:set_env(simple_gossip, shrink_time_threshold, 10),
  timer:sleep(10),
  ?assert(byte_size(term_to_binary(simple_gossip_vclock:increment(Shrink))) < byte_size(term_to_binary(Shrink))),
  ok.

receive_notify(Ref) ->
  receive
    {data_changed, {Data, Ref}} ->
      {ok, Data}
  after 1000 ->
    timeout
  end.

receive_rumor_notification() ->
  receive
    {rumor_changed, Rumor} ->
      {ok, Rumor}
  after 1000 ->
    timeout
  end.

