%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2025, systream
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_cfg).

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-define(KEY(K), {?MODULE, K}).

%% gen_server callbacks
-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

%% API
-export([set/2, get/2, clear_store/0]).

-type state() :: no_state.
-export_type([state/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec get(term(), term()) -> term().
get(Key, Default) ->
  case persistent_term:get(?KEY(Key), '$__not_set__') of
    '$__not_set__' ->
      case simple_gossip_server:get() of
        #{Key := Value} = Data ->
          persist(Data),
          Value;
        Data when is_map(Data) ->
          Default
      end;
    Value ->
      Value
  end.

-spec set(term(), term()) -> ok.
set(Key, Value) ->
  ok = simple_gossip_server:set(fun(Data) -> {change, do_set(Key, Value, Data)} end),
  persist(Key, Value).

-spec do_set(term(), term(), undefined | map()) -> map().
do_set(Key, Value, undefined) ->
  do_set(Key, Value, #{});
do_set(Key, Value, Data) ->
  Data#{Key => Value}.

-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec init(term()) -> {ok, state()}.
init(_) ->
  simple_gossip_event:subscribe(self(), data),
  {ok, no_state}.

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
handle_call(_Command, _From, State) ->
  {reply, not_handled, State}.

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

-spec handle_info({data_changed, undefined | map()}, State :: state()) ->
  {noreply, NewState :: state()} | {noreply, NewState :: state(), hibernate}.
handle_info({data_changed, undefined}, State) ->
  {noreply, State, hibernate};
handle_info({data_changed, Data}, State) ->
  persist(Data),
  {noreply, State, hibernate}.

-spec persist(map()) -> ok.
persist(Data) when is_map(Data) ->
  maps:foreach(fun(Key, Value) ->
      case persistent_term:get(Key, '$__not_set__') of
        CValue when CValue =:= Value -> ok;
        _ -> persist(Key, Value)
      end
    end, Data),
  ok;
persist(_Data) ->
  ok.

-spec persist(term(), term()) -> ok.
persist(Key, Value) ->
  persistent_term:put(?KEY(Key), Value).

-spec clear_store() -> ok.
clear_store() ->
  lists:foreach(fun({{?MODULE, _} = Key, _}) ->
                     persistent_term:erase(Key);
                   (_) -> ok
                end, persistent_term:get()).