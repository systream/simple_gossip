%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2020, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_persist).

-behaviour(gen_server).

-include("simple_gossip.hrl").

%% API
-export([start_link/0, get/0, delete_file/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-export([get_file_path/0,
         get_file_path/1]).

-define(SAVE_INTERVAL, 2000).
-define(ITERATION_THRESHOLD, 30).
-define(SERVER, ?MODULE).

-record(state, {
    file_path :: string(),
    pending_data :: term(),
    iteration = 0 :: non_neg_integer()}
).

-opaque state() :: #state{}.
-export_type([state/0, rumor/0]).

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
  process_flag(trap_exit, true),
  simple_gossip_event:subscribe(self(), rumor),
  FileName = get_file_path(),
  {ok, #state{file_path = FileName}}.

-spec get_file_path() -> string().
get_file_path() ->
  get_file_path(node()).

-spec get_file_path(node()) -> string().
get_file_path(Node) ->
  ObjectName = "gossip-" ++ integer_to_list(erlang:phash2(Node)) ++ ".data",
  SaveDir = application:get_env(simple_gossip, save_dir, "./"),
  SaveDir ++ ObjectName.

-spec delete_file() -> ok | {error, term()}.
delete_file() ->
  file:delete(get_file_path()).

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
handle_call(get, _From,
            #state{file_path = FilePath, pending_data = undefined} = State) ->
  case file:read_file(FilePath) of
    {ok, Data} ->
      {reply, {ok, binary_to_term(Data)}, State};
    {error, enoent} ->
      {reply, {error, undefined}, State}
  end;
handle_call(get, From, #state{pending_data = PendingData} = State) ->
  gen_server:reply(From, {ok, PendingData}),
  {noreply, save(State)}.

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

-spec handle_info({rumor_changed, rumor()}, State :: state()) ->
  {noreply, NewState :: state()}.
handle_info({rumor_changed, Rumor}, #state{iteration = Iteration} = State) when
  Iteration > ?ITERATION_THRESHOLD ->
  {noreply, save(State#state{pending_data = Rumor})};
handle_info({rumor_changed, Rumor}, #state{iteration = Iteration} = State) ->
  {noreply,
   State#state{pending_data = Rumor, iteration = Iteration + 1},
   ?SAVE_INTERVAL};
handle_info(timeout, State) ->
  {noreply, save(State), hibernate}.

-spec terminate((normal | shutdown | {shutdown, term()} | term()), state()) -> ok.
terminate(_, State) ->
  save(State),
  ok.

-spec save(state()) -> state().
save(#state{pending_data = undefined} = State) ->
  State;
save(#state{file_path = FilePath, pending_data = Rumor} = State) ->
  ok = file:write_file(FilePath, erlang:term_to_binary(Rumor)),
  State#state{pending_data = undefined, iteration = 0}.
