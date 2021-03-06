%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_test_tools).
-author("Peter Tihanyi").

-include("../src/simple_gossip.hrl").

%% API
-export([stop_cluster/1, wait_to_reconcile/0, start_slave_node/1,
         stop_cluster/0, start_nodes/1, init_netkernel/0, wait_to_reconcile/2]).

-spec start_slave_node(atom()) -> node().
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
                            {startup_functions, [
                              {simple_gossip_persist, delete_file, []}
                            ]},
                            {erl_flags, ErlFlags}]),

  case Result of
    {ok, HostNode} ->
      rpc:call(HostNode, application, ensure_all_started, [simple_gossip]),
      HostNode;
    {error, already_started, HostNode} ->
      HostNode
  end.

-spec start_nodes([atom()]) -> [node()].
start_nodes(NodeNames) ->
  init_netkernel(),
  NodeList = [start_slave_node(NodeName) || NodeName <- NodeNames],
  NodeList ++ [node()].

-spec init_netkernel() -> ok.
init_netkernel() ->
  case net_kernel:start([proper, shortnames]) of
    {error, {already_started, _}} ->
      ok;
    {ok, _} ->
      application:stop(simple_gossip),
      application:ensure_all_started(simple_gossip),
      net_kernel:set_net_ticktime(3, 9),
      ok
  end.

-spec stop_cluster() -> ok.
stop_cluster() ->
  stop_cluster(nodes()).

-spec stop_cluster([node()]) -> ok.
stop_cluster([]) ->
  ok;
stop_cluster([Node | Nodes]) when Node =/= node() ->
  rpc:call(Node, init, stop, []),
  ct_slave:stop(Node),
  stop_cluster(Nodes);
stop_cluster([Node | Nodes]) when Node =:= node() ->
  stop_cluster(Nodes).

-spec wait_to_reconcile() -> timeout | ok.
wait_to_reconcile() ->
  wait_to_reconcile(node(), 3000).

-spec wait_to_reconcile(node(), pos_integer()) -> timeout | ok.
wait_to_reconcile(Node, Timeout) ->
  Master = self(),
  Pid =
    spawn(fun F() ->
      timer:sleep(2),
      case rpc:call(Node, simple_gossip, status, []) of
        {error, gossip_vsn_mismatch, _Leader, _Nodes}->
          timer:sleep(6),
          F();
        _Result ->
          Master ! {self(), ok}
      end
    end),
  R =
    receive
      {Pid, Result} ->
        Result
    after Timeout ->
      timeout
    end,
  exit(Pid, kill),
  R.
