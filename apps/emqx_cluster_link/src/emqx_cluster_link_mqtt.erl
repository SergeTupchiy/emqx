%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_mqtt).

-include("emqx_cluster_link.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(emqx_resource).
-behaviour(ecpool_worker).

%% ecpool
-export([connect/1]).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2
]).

-export([
    ensure_msg_fwd_resource/1,
    stop_msg_fwd_resource/1,
    decode_route_op/1,
    decode_forwarded_msg/1,
    decode_resp/1
]).

-export([
    publish_actor_init_sync/6,
    actor_init_ack_resp_msg/4,
    publish_route_sync/4,
    encode_field/2
]).

-export([
    forward/2
]).

-define(MSG_CLIENTID_SUFFIX, ":msg:").

-define(MQTT_HOST_OPTS, #{default_port => 1883}).

-define(MSG_POOL_PREFIX, "emqx_cluster_link_mqtt:msg:").
-define(RES_NAME(Prefix, ClusterName), <<Prefix, ClusterName/binary>>).
-define(ROUTE_POOL_NAME(ClusterName), ?RES_NAME(?ROUTE_POOL_PREFIX, ClusterName)).
-define(MSG_RES_ID(ClusterName), ?RES_NAME(?MSG_POOL_PREFIX, ClusterName)).
-define(HEALTH_CHECK_TIMEOUT, 1000).
-define(RES_GROUP, <<"emqx_cluster_link">>).

-define(PROTO_VER, 1).

-define(DECODE(Payload), erlang:binary_to_term(Payload, [safe])).
-define(ENCODE(Payload), erlang:term_to_binary(Payload)).

-define(F_OPERATION, '$op').
-define(OP_ROUTE, <<"route">>).
-define(OP_ACTOR_INIT, <<"actor_init">>).
-define(OP_ACTOR_INIT_ACK, <<"actor_init_ack">>).

-define(F_ACTOR, 10).
-define(F_INCARNATION, 11).
-define(F_ROUTES, 12).
-define(F_TARGET_CLUSTER, 13).
-define(F_PROTO_VER, 14).
-define(F_RESULT, 15).

-define(ROUTE_DELETE, 100).

-define(PUB_TIMEOUT, 10_000).

ensure_msg_fwd_resource(#{upstream := Name, pool_size := PoolSize} = ClusterConf) ->
    ResConf = #{
        query_mode => async,
        start_after_created => true,
        start_timeout => 5000,
        health_check_interval => 5000,
        %% TODO: configure res_buf_worker pool separately?
        worker_pool_size => PoolSize
    },
    emqx_resource:create_local(?MSG_RES_ID(Name), ?RES_GROUP, ?MODULE, ClusterConf, ResConf).

stop_msg_fwd_resource(ClusterName) ->
    emqx_resource:stop(?MSG_RES_ID(ClusterName)).

%%--------------------------------------------------------------------
%% emqx_resource callbacks (message forwarding)
%%--------------------------------------------------------------------

callback_mode() -> async_if_possible.

on_start(ResourceId, #{pool_size := PoolSize} = ClusterConf) ->
    PoolName = ResourceId,
    Options = [
        {name, PoolName},
        {pool_size, PoolSize},
        {pool_type, hash},
        {client_opts, emqtt_client_opts(?MSG_CLIENTID_SUFFIX, ClusterConf)}
    ],
    ok = emqx_resource:allocate_resource(ResourceId, pool_name, PoolName),
    case emqx_resource_pool:start(PoolName, ?MODULE, Options) of
        ok ->
            {ok, #{pool_name => PoolName, topic => ?MSG_FWD_TOPIC}};
        {error, {start_pool_failed, _, Reason}} ->
            {error, Reason}
    end.

on_stop(ResourceId, _State) ->
    #{pool_name := PoolName} = emqx_resource:get_allocated_resources(ResourceId),
    emqx_resource_pool:stop(PoolName).

on_query(_ResourceId, FwdMsg, #{pool_name := PoolName, topic := LinkTopic} = _State) when
    is_record(FwdMsg, message)
->
    #message{topic = Topic, qos = QoS} = FwdMsg,
    handle_send_result(
        ecpool:pick_and_do(
            {PoolName, Topic},
            fun(ConnPid) ->
                emqtt:publish(ConnPid, LinkTopic, ?ENCODE(FwdMsg), QoS)
            end,
            no_handover
        )
    ).

on_query_async(
    _ResourceId, FwdMsg, CallbackIn, #{pool_name := PoolName, topic := LinkTopic} = _State
) ->
    Callback = {fun on_async_result/2, [CallbackIn]},
    #message{topic = Topic, qos = QoS} = FwdMsg,
    %% TODO check message ordering, pick by topic,client pair?
    ecpool:pick_and_do(
        {PoolName, Topic},
        fun(ConnPid) ->
            %% #delivery{} record has no valuable data for a remote link...
            Payload = ?ENCODE(FwdMsg),
            %% TODO: check override QOS requirements (if any)
            emqtt:publish_async(ConnPid, LinkTopic, Payload, QoS, Callback)
        end,
        no_handover
    ).

%% copied from emqx_bridge_mqtt_connector

on_async_result(Callback, Result) ->
    apply_callback_function(Callback, handle_send_result(Result)).

apply_callback_function(F, Result) when is_function(F) ->
    erlang:apply(F, [Result]);
apply_callback_function({F, A}, Result) when is_function(F), is_list(A) ->
    erlang:apply(F, A ++ [Result]);
apply_callback_function({M, F, A}, Result) when is_atom(M), is_atom(F), is_list(A) ->
    erlang:apply(M, F, A ++ [Result]).

handle_send_result(ok) ->
    ok;
handle_send_result({ok, #{reason_code := ?RC_SUCCESS}}) ->
    ok;
handle_send_result({ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}}) ->
    ok;
handle_send_result({ok, Reply}) ->
    {error, classify_reply(Reply)};
handle_send_result({error, Reason}) ->
    {error, classify_error(Reason)}.

classify_reply(Reply = #{reason_code := _}) ->
    {unrecoverable_error, Reply}.

classify_error(disconnected = Reason) ->
    {recoverable_error, Reason};
classify_error(ecpool_empty) ->
    {recoverable_error, disconnected};
classify_error({disconnected, _RC, _} = Reason) ->
    {recoverable_error, Reason};
classify_error({shutdown, _} = Reason) ->
    {recoverable_error, Reason};
classify_error(shutdown = Reason) ->
    {recoverable_error, Reason};
classify_error(Reason) ->
    {unrecoverable_error, Reason}.

%% copied from emqx_bridge_mqtt_connector
on_get_status(_ResourceId, #{pool_name := PoolName} = _State) ->
    Workers = [Worker || {_Name, Worker} <- ecpool:workers(PoolName)],
    try emqx_utils:pmap(fun get_status/1, Workers, ?HEALTH_CHECK_TIMEOUT) of
        Statuses ->
            combine_status(Statuses)
    catch
        exit:timeout ->
            connecting
    end.

get_status(Worker) ->
    case ecpool_worker:client(Worker) of
        {ok, Client} -> status(Client);
        {error, _} -> disconnected
    end.

status(Pid) ->
    try
        case proplists:get_value(socket, emqtt:info(Pid)) of
            Socket when Socket /= undefined ->
                connected;
            undefined ->
                connecting
        end
    catch
        exit:{noproc, _} ->
            disconnected
    end.

combine_status(Statuses) ->
    %% NOTE
    %% Natural order of statuses: [connected, connecting, disconnected]
    %% * `disconnected` wins over any other status
    %% * `connecting` wins over `connected`
    case lists:reverse(lists:usort(Statuses)) of
        [Status | _] ->
            Status;
        [] ->
            disconnected
    end.

%%--------------------------------------------------------------------
%% ecpool
%%--------------------------------------------------------------------

connect(Options) ->
    WorkerId = proplists:get_value(ecpool_worker_id, Options),
    #{clientid := ClientId} = ClientOpts = proplists:get_value(client_opts, Options),
    ClientId1 = emqx_bridge_mqtt_lib:bytes23([ClientId], WorkerId),
    ClientOpts1 = ClientOpts#{clientid => ClientId1},
    case emqtt:start_link(ClientOpts1) of
        {ok, Pid} ->
            case emqtt:connect(Pid) of
                {ok, _Props} ->
                    {ok, Pid};
                Error ->
                    Error
            end;
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "client_start_failed",
                config => emqx_utils:redact(ClientOpts),
                reason => Reason
            }),
            Error
    end.

%%--------------------------------------------------------------------
%% Protocol
%%--------------------------------------------------------------------

%%% New leader-less Syncer/Actor implementation

publish_actor_init_sync(ClientPid, ReqId, RespTopic, TargetCluster, Actor, Incarnation) ->
    PubTopic = ?ROUTE_TOPIC,
    Payload = #{
        ?F_OPERATION => ?OP_ACTOR_INIT,
        ?F_PROTO_VER => ?PROTO_VER,
        ?F_TARGET_CLUSTER => TargetCluster,
        ?F_ACTOR => Actor,
        ?F_INCARNATION => Incarnation
    },
    Properties = #{
        'Response-Topic' => RespTopic,
        'Correlation-Data' => ReqId
    },
    emqtt:publish(ClientPid, PubTopic, Properties, ?ENCODE(Payload), [{qos, ?QOS_1}]).

actor_init_ack_resp_msg(Actor, InitRes, ReqId, RespTopic) ->
    Payload = #{
        ?F_OPERATION => ?OP_ACTOR_INIT_ACK,
        ?F_PROTO_VER => ?PROTO_VER,
        ?F_ACTOR => Actor,
        ?F_RESULT => InitRes
    },
    emqx_message:make(
        undefined,
        ?QOS_1,
        RespTopic,
        ?ENCODE(Payload),
        #{},
        #{properties => #{'Correlation-Data' => ReqId}}
    ).

publish_route_sync(ClientPid, Actor, Incarnation, Updates) ->
    PubTopic = ?ROUTE_TOPIC,
    Payload = #{
        ?F_OPERATION => ?OP_ROUTE,
        ?F_ACTOR => Actor,
        ?F_INCARNATION => Incarnation,
        ?F_ROUTES => Updates
    },
    emqtt:publish(ClientPid, PubTopic, ?ENCODE(Payload), ?QOS_1).

decode_route_op(Payload) ->
    decode_route_op1(?DECODE(Payload)).

decode_resp(Payload) ->
    decode_resp1(?DECODE(Payload)).

decode_route_op1(#{
    ?F_OPERATION := ?OP_ACTOR_INIT,
    ?F_PROTO_VER := ProtoVer,
    ?F_TARGET_CLUSTER := TargetCluster,
    ?F_ACTOR := Actor,
    ?F_INCARNATION := Incr
}) ->
    {actor_init, #{
        actor => Actor,
        incarnation => Incr,
        cluster => TargetCluster,
        proto_ver => ProtoVer
    }};
decode_route_op1(#{
    ?F_OPERATION := ?OP_ROUTE,
    ?F_ACTOR := Actor,
    ?F_INCARNATION := Incr,
    ?F_ROUTES := RouteOps
}) ->
    RouteOps1 = lists:map(fun(Op) -> decode_field(route, Op) end, RouteOps),
    {route_updates, #{actor => Actor, incarnation => Incr}, RouteOps1};
decode_route_op1(Payload) ->
    ?SLOG(warning, #{
        msg => "unexpected_cluster_link_route_op_payload",
        payload => Payload
    }),
    {error, Payload}.

decode_resp1(#{
    ?F_OPERATION := ?OP_ACTOR_INIT_ACK,
    ?F_ACTOR := Actor,
    ?F_PROTO_VER := ProtoVer,
    ?F_RESULT := InitResult
}) ->
    {actor_init_ack, #{actor => Actor, result => InitResult, proto_ver => ProtoVer}}.

decode_forwarded_msg(Payload) ->
    case ?DECODE(Payload) of
        #message{} = Msg ->
            Msg;
        _ ->
            ?SLOG(warning, #{
                msg => "unexpected_cluster_link_forwarded_msg_payload",
                payload => Payload
            }),
            {error, Payload}
    end.

encode_field(route, {add, Route = {_Topic, _ID}}) ->
    Route;
encode_field(route, {delete, {Topic, ID}}) ->
    {?ROUTE_DELETE, Topic, ID}.

decode_field(route, {?ROUTE_DELETE, Topic, ID}) ->
    {delete, {Topic, ID}};
decode_field(route, Route = {_Topic, _ID}) ->
    {add, Route}.

%%--------------------------------------------------------------------
%% emqx_external_broker
%%--------------------------------------------------------------------

forward(ClusterName, #delivery{message = #message{topic = Topic} = Msg}) ->
    QueryOpts = #{pick_key => Topic},
    emqx_resource:query(?MSG_RES_ID(ClusterName), Msg, QueryOpts).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

emqtt_client_opts(ClientIdSuffix, ClusterConf) ->
    #{clientid := BaseClientId} = Opts = emqx_cluster_link_config:mk_emqtt_options(ClusterConf),
    ClientId = emqx_bridge_mqtt_lib:clientid_base([BaseClientId, ClientIdSuffix]),
    Opts#{clientid => ClientId}.
