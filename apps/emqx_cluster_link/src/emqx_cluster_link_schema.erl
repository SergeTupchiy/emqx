%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_schema).

-behaviour(emqx_schema_hooks).

-include("emqx_cluster_link.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([injected_fields/0]).

%% Used in emqx_cluster_link_api
-export([links_schema/1]).

-export([
    roots/0,
    fields/1,
    namespace/0,
    desc/1
]).

-define(MQTT_HOST_OPTS, #{default_port => 1883}).

namespace() -> "cluster_linking".

roots() -> [].

injected_fields() ->
    #{cluster => fields("cluster_linking")}.

links_schema(Meta) ->
    ?HOCON(?ARRAY(?R_REF("link")), Meta#{default => [], validator => fun links_validator/1}).

fields("cluster_linking") ->
    [{links, links_schema(#{})}];
fields("link") ->
    [
        {enable, ?HOCON(boolean(), #{default => true})},
        {upstream, ?HOCON(binary(), #{required => true})},
        {server,
            emqx_schema:servers_sc(#{required => true, desc => ?DESC("server")}, ?MQTT_HOST_OPTS)},
        {clientid, ?HOCON(binary(), #{desc => ?DESC("clientid")})},
        {username, ?HOCON(binary(), #{desc => ?DESC("username")})},
        {password, emqx_schema_secret:mk(#{desc => ?DESC("password")})},
        {ssl, #{
            type => ?R_REF(emqx_schema, "ssl_client_opts"),
            default => #{<<"enable">> => false},
            desc => ?DESC("ssl")
        }},
        {topics,
            ?HOCON(?ARRAY(binary()), #{required => true, validator => fun topics_validator/1})},
        {pool_size, ?HOCON(pos_integer(), #{default => emqx_vm:schedulers() * 2})},
        %% Must not be configured manually. The value is incremented by cluster link config handler
        %% and is used as a globally synchronized sequence to ensure persistent routes actors have
        %% the same next incarnation after each config change.
        {ps_actor_incarnation, ?HOCON(integer(), #{default => 0, importance => ?IMPORTANCE_HIDDEN})}
    ].

desc(_) ->
    "todo".

%% TODO: check that no link name equals local cluster name,
%% but this may be tricky since the link config is injected into cluster config (emqx_conf_schema).
links_validator(Links) ->
    {_, Dups} = lists:foldl(
        fun(Link, {Acc, DupAcc}) ->
            Name = link_name(Link),
            case Acc of
                #{Name := _} ->
                    {Acc, [Name | DupAcc]};
                _ ->
                    {Acc#{Name => undefined}, DupAcc}
            end
        end,
        {#{}, []},
        Links
    ),
    check_errors(Dups, duplicated_cluster_links, names).

link_name(#{upstream := Name}) -> Name;
link_name(#{<<"upstream">> := Name}) -> Name.

topics_validator(Topics) ->
    Errors = lists:foldl(
        fun(T, ErrAcc) ->
            try
                _ = emqx_topic:validate(T),
                validate_sys_link_topic(T, ErrAcc)
            catch
                E:R ->
                    [{T, {E, R}} | ErrAcc]
            end
        end,
        [],
        Topics
    ),
    check_errors(Errors, invalid_topics, topics).

validate_sys_link_topic(T, ErrAcc) ->
    case emqx_topic:match(T, ?TOPIC_PREFIX_WILDCARD) of
        true ->
            [{T, {error, topic_not_allowed}} | ErrAcc];
        false ->
            ErrAcc
    end.

check_errors([] = _Errors, _Reason, _ValuesField) ->
    ok;
check_errors(Errors, Reason, ValuesField) ->
    {error, #{reason => Reason, ValuesField => Errors}}.
