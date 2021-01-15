%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2021, Systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(simple_gossip_call).
-author("Peter Tihanyi").

-define(MSG_KEY, '$_prevent_late_msgs_$').

%% API
-export([call/3]).

% prevent late responses
-spec call(ServerRef, term(), pos_integer()) -> {ok, term()} | timeout when
  ServerRef ::
  Name | {Name, node()} | {global, Name} | {via, module(), Name} | pid(),
  Name :: term().
call(Name, Request, Timeout) ->
  Pid = spawn_call(Name, Request, Timeout),
  receive_response(Pid, Timeout, fun() ->
                                   exit(Pid, kill),
                                   receive_response(Pid, 10, timeout)
                                 end).


spawn_call(Name, Request, Timeout) ->
  Self = self(),
  spawn(fun() -> do_call(Name, Request, Timeout, Self) end).

do_call(Name, Request, Timeout, Parent) when Timeout > 10 ->
  CallTimeout = Timeout-10,
  Parent ! {?MSG_KEY, self(), catch gen_server:call(Name, Request, CallTimeout)}.

-spec receive_response(pid(), pos_integer(), fun() | term()) ->
  {ok, term()} | term().
receive_response(Pid, Timeout, Next) ->
  receive
    {?MSG_KEY, Pid, Msg} ->
      {ok, Msg}
  after Timeout ->
    case Next of
      Next when is_function(Next) ->
        Next;
      _ ->
        Next
    end
  end.

