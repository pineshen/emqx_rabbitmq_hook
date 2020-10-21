-module(emqx_rabbitmq_hook).

-include_lib("emqx/include/emqx.hrl").

-export([load/1
  , unload/0
]).

%% Hooks functions
-export([on_client_connected/3
  , on_client_disconnected/4
  , on_message_publish/2
]).

-import(emqx_rabbitmq_hook_cli, [ensure_exchange/1, publish/3]).
-import(bson_binary, [put_document/1]).
-import(inet, [ntoa/1]).

-include("emqx_rabbitmq_hook.hrl").


%% Called when the plugin application start
load(_Env) ->
  {ok, ExchangeName} = application:get_env(?APP, exchange),
  emqx_rabbitmq_hook_cli:ensure_exchange(ExchangeName),
  hookup('client.connected', client_connected, fun ?MODULE:on_client_connected/3, [ExchangeName]),
  hookup('client.disconnected', client_disconnected, fun ?MODULE:on_client_disconnected/4, [ExchangeName]),
  hookup('message.publish', message_publish, fun ?MODULE:on_message_publish/2, [ExchangeName]).

  
on_client_connected(#{clientid := ClientId, username := Username, peerhost := Peerhost}, ConnInfo, ExchangeName) ->
    Params = #{ clientid => ClientId
              , username => maybe(Username)
              , ipaddress => iolist_to_binary(ntoa(Peerhost))
              , keepalive => maps:get(keepalive, ConnInfo)
              , proto_ver => maps:get(proto_ver, ConnInfo)
              , connected_at => maps:get(connected_at, ConnInfo)
              , conn_ack => 0
              },         
  emqx_rabbitmq_hook_cli:publish(ExchangeName, bson_binary:put_document(Params), <<"client.connected">>),
  ok.



on_client_disconnected(ClientInfo, {shutdown, Reason}, ConnInfo, ExchangeName) when is_atom(Reason) ->
  on_client_disconnected(ClientInfo, Reason, ConnInfo, ExchangeName);
on_client_disconnected(#{clientid := ClientId, username := Username}, Reason, _ConnInfo, _ExchangeName) ->
    Params = #{ clientid => ClientId
              , username => maybe(Username)
              , disconnected_at => maps:get(disconnected_at, _ConnInfo)
              , reason => stringfy(maybe(Reason))
              },
   emqx_rabbitmq_hook_cli:publish(_ExchangeName, bson_binary:put_document(Params), <<"client.disconnected">>),
   ok.           

on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
  {ok, Message};

on_message_publish(Message = #message{topic = Topic, flags = #{retain := Retain}}, ExchangeName) ->
  Username = case maps:find(username, Message#message.headers) of
               {ok, Value} -> Value;
               _ -> undefined
             end,
  Doc = {
    client_id, Message#message.from,
    username, Username,
    topic, Topic,
    qos, Message#message.qos,
    retained, Retain,
    payload, {bin, bin, Message#message.payload},
    published_at, Message#message.timestamp
  },
  emqx_rabbitmq_hook_cli:publish(ExchangeName, bson_binary:put_document(Doc), <<"message.publish">>),
  {ok, Message}.
  

%% Called when the plugin application stop
unload() ->
  emqx:unhook('client.connected', fun ?MODULE:on_client_connected/3),
  emqx:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/4),
  emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2).


hookup(Event, ConfigName, Func, InitArgs) ->
  case application:get_env(?APP, ConfigName) of
    {ok, true} -> emqx:hook(Event, Func, InitArgs);
    _ -> ok
  end.


stringfy(Term) when is_atom(Term); is_binary(Term) ->
    Term;

stringfy(Term) ->
    unicode:characters_to_binary((io_lib:format("~0p", [Term]))).

maybe(undefined) -> null;
maybe(Str) -> Str.

now_ms() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
	  1000000000 * MegaSecs + Secs * 1000 + MicroSecs div 1000.

now_ms({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000 + round(MicroSecs/1000).
