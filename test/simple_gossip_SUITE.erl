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
    status,
    status_timeout,
    stop_node,
    stop_leader_node
    %%status_mismatch
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
  ok =simple_gossip:subscribe(self()),
  ok = simple_gossip:subscribe(self()), % check double subscribe -> should no effect
  simple_gossip:set({"new", Ref}),
  ?assertMatch({ok, _}, receive_notify(Ref)),

  simple_gossip:unsubscribe(self()),

  Ref2 = make_ref(),
  simple_gossip:set({"new2", Ref2}),
  ?assertMatch(timeout, receive_notify(Ref)),
  simple_gossip:unsubscribe(self()).

notify_change_not_on_leader(_Config) ->
  NodeName = 'notify1',
  Host = start_slave_node(NodeName),
  simple_gossip:join(Host),
  wait_to_reconcile(),
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
  Host = start_slave_node(NodeName),
  simple_gossip:join(Host),
  wait_to_reconcile(),
  ?assertEqual("test_11", rpc:call(Host, simple_gossip, get, [])),
  simple_gossip:leave(Host),
  ct_slave:stop(Host).

leave_self(_Config) ->
  ?assertEqual(ok, simple_gossip:leave(node())),
  Node = node(),
  ?assertMatch({ok, _, Node, [Node]}, simple_gossip:status()).

leave_by_another_node(_Config) ->
  NodeName = 'test_leave_test',
  Node = start_slave_node(NodeName),
  simple_gossip:join(Node),
  wait_to_reconcile(),
  SelfNode = node(),
  ok = rpc:call(Node, simple_gossip, leave, [SelfNode]),
  wait_to_reconcile(),
  ?assertMatch({ok, _, SelfNode, [SelfNode]}, simple_gossip:status()),
  simple_gossip:leave(Node),
  ct_slave:stop(Node).

sync_between_nodes(_Config) ->
  simple_gossip:set("default"),
  Node1 = start_slave_node('sync_1'),
  Node2 = start_slave_node('sync_2'),
  simple_gossip:join(Node1),
  simple_gossip:join(Node2),

  simple_gossip:set("main"),
  wait_to_reconcile(),

  ?assertEqual("main", rpc:call(Node1, simple_gossip, get, [])),
  ?assertEqual("main", rpc:call(Node2, simple_gossip, get, [])),

  ok = rpc:call(Node1, simple_gossip, set, ["main1"]),
  wait_to_reconcile(),

  ?assertEqual("main1", simple_gossip:get()),
  ?assertEqual("main1", rpc:call(Node2, simple_gossip, get, [])),

  ok = rpc:call(Node2, simple_gossip, set, ["main2"]),
  wait_to_reconcile(),

  ?assertEqual("main2", simple_gossip:get()),
  ?assertEqual("main2", rpc:call(Node1, simple_gossip, get, [])),

  simple_gossip:leave(Node1),
  simple_gossip:leave(Node2),

  ct_slave:stop(Node1),
  ct_slave:stop(Node2).


status(_Config) ->
  simple_gossip:set("default"),
  Node1 = start_slave_node('status_1'),
  simple_gossip:join(Node1),
  simple_gossip:set("a"),
  wait_to_reconcile(),
  Node = node(),
  ?assertMatch({ok, _, Node, _}, simple_gossip:status()),
  simple_gossip:leave(Node1),
  ct_slave:stop(Node1).

status_timeout(_Config) ->
  simple_gossip:set("default"),
  Node = 'test_no_node@local',
  simple_gossip:join(Node),
  ?assertMatch({error, {timeout, [Node]}}, simple_gossip:status()),
  simple_gossip:leave(Node).

stop_node(_Config) ->
  simple_gossip:set("default"),
  Node1 = start_slave_node('stop_1'),
  simple_gossip:join(Node1),
  wait_to_reconcile(),
  simple_gossip:set(test),
  Node = node(),
  ct_slave:stop(Node1),
  timer:sleep(10),
  simple_gossip:leave(Node1),
  wait_to_reconcile(),
  ?assertMatch({ok, _, Node, _}, simple_gossip:status()).


stop_leader_node(_Config) ->
  simple_gossip:set("default"),
  Node1 = start_slave_node('stop_2'),
  simple_gossip:join(Node1),
  wait_to_reconcile(),
  simple_gossip:set(test),
  Node = node(),

  application:stop(simple_gossip),
  timer:sleep(100),
  ok = rpc:call(Node1, simple_gossip, leave, [Node]),
  timer:sleep(100),
  ?assertMatch({ok, _, Node1, _}, rpc:call(Node1, simple_gossip, status, [])),
  ct_slave:stop(Node1).


%%status_mismatch(_Config) ->
%%  simple_gossip:set("default"),
%%  Node = start_slave_node('status_2'),
%%  simple_gossip:join(Node),
%%  ok = wait_to_reconcile(),

%%  Rumor = {rumor, 1000, tweaked_data, Node, [Node], 8, 10000},

%%  gen_server:cast({simple_gossip_server, Node}, {reconcile, Rumor, node()}),
%%  ct:sleep(10),

%%  ?assertMatch({error, mismatch}, simple_gossip:status()),
%%  simple_gossip:leave(Node),
%%  ct_slave:stop(Node).


sync_connecting_node(_Config) ->
  simple_gossip:set("default"),
  Node1 = start_slave_node('sync_1'),

  ok = rpc:call(Node1, simple_gossip, join, [node()]),

  ?assertEqual("default", simple_gossip:get()),

  simple_gossip:set("main"),
  wait_to_reconcile(),

  ?assertEqual("main", simple_gossip:get()),
  ?assertEqual("main", rpc:call(Node1, simple_gossip, get, [])),

  simple_gossip:leave(Node1),

  ct_slave:stop(Node1).


start_slave_node(NodeName) ->
  EbinDirs =
    filename:dirname(filename:dirname(code:priv_dir(simple_gossip))) ++ "/*/ebin",
  ErlFlags = "-pa " ++ EbinDirs,
  Result = ct_slave:start(NodeName,
    [ {kill_if_fail, true},
      {monitor_master, true},
      {init_timeout, 5},
      {boot_timeout, 5},
      {startup_timeout, 5},
      {startup_functions, [{application, ensure_all_started, [simple_gossip]}]},
      {erl_flags, ErlFlags}]),

  case Result of
    {ok, HostNode} ->
      HostNode;
    {error, already_started, HostNode} ->
      HostNode
  end.

receive_notify(Ref) ->
  receive
    {data_changed, {Data, Ref}} ->
      {ok, Data}
  after 1000 ->
    timeout
  end.


wait_to_reconcile() ->
  wait_to_reconcile(node(), 1400).

wait_to_reconcile(Node, Timeout) ->
  Master = self(),
  Pid =
    spawn(fun F() ->
                    timer:sleep(10),
                    case rpc:call(Node, simple_gossip, status, []) of
                      {error, mismatch} ->
                         F();
                       Result ->
                         Master ! {self(), Result}
                     end
          end),
  R = receive {Pid, Result} -> Result after Timeout -> timeout end,
  exit(Pid, kill),
  R.