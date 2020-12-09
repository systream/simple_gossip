%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_persist).
-author("Peter Tihanyi").

-behaviour(gen_server).

-include("simple_gossip.hrl").

%% API
-export([start_link/0, get/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-ifdef(TEST).
-export([get_file_path/1]).
-endif.

-define(SERVER, ?MODULE).

-record(state, {
  file_path :: string()
}).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec get() -> {ok, rumor()} | {error, undefined}.
get() ->
  gen_server:call(?SERVER, get).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec init(term()) -> {ok, state()}.
init(_) ->
  simple_gossip_event:subscribe(self(), rumor),
  FileName = get_file_path(node()),
  {ok, #state{file_path = FileName}}.

-spec get_file_path(node()) -> string().
get_file_path(Node) ->
  ObjectName = "gossip-" ++ integer_to_list(erlang:phash2(Node)) ++ ".data",
  SaveDir = application:get_env(simple_gossip, save_dir, "./"),
  SaveDir ++ ObjectName.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
                  State :: state()) ->
                   {reply, Reply :: term(), NewState :: state()} |
                   {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
                   {noreply, NewState :: state()} |
                   {noreply, NewState :: state(), timeout() | hibernate} |
                   {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
                   {stop, Reason :: term(), NewState :: state()}).
handle_call(get, _From, #state{file_path = FilePath} = State) ->
  case file:read_file(FilePath) of
    {ok, Data} ->
      {reply, {ok, binary_to_term(Data)}, State};
    {error, enoent} ->
      {reply, {error, undefined}, State}
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: state()) ->
  {noreply, NewState :: state()} |
  {noreply, NewState :: state(), timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: state()}).
handle_cast(_Request, State) ->
  {noreply, State}.

-spec handle_info({rumor, rumor()}, State :: state()) ->
  {noreply, NewState :: state()}.
handle_info({rumor_changed, Rumor}, #state{file_path = FilePath} = State) ->
  %io:format("new: ~p ~p ~n bin: ~p~n~n", [FilePath, Rumor, term_to_binary(Rumor)]),
  ok = file:write_file(FilePath, erlang:term_to_binary(Rumor)),
  {noreply, State}.

