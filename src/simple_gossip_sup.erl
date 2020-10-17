%%%-------------------------------------------------------------------
%% @doc simple_gossip top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(simple_gossip_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()} | 'ignore' | {'error', term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec init([]) ->
  {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}} | ignore.
init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 3, period => 10},
    EventServer = #{id => simple_gossip_event,
                    start => {gen_event, start_link, [{local, simple_gossip_event}]},
                    restart => permanent,
                    shutdown => 5000,
                    modules => [simple_gossip_event]},
    Service = #{id => simple_gossip_server,
                start => {simple_gossip_server, start_link, []},
                restart => permanent,
                shutdown => 5000,
                modules => [simple_gossip_server]},

    ChildSpecs = [EventServer, Service],
    {ok, {SupFlags, ChildSpecs}}.
