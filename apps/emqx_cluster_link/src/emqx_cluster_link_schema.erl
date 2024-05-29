%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_schema).

-behaviour(emqx_schema_hooks).

-include_lib("hocon/include/hoconsc.hrl").

-export([injected_fields/0]).

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

fields("cluster_linking") ->
    [
        {links,
            ?HOCON(?ARRAY(?R_REF("link")), #{default => [], validator => fun links_validator/1})}
    ];
fields("link") ->
    [
        {enable, ?HOCON(boolean(), #{default => false})},
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
        %% TODO: validate topics:
        %% - basic topic validation
        %% - non-overlapping (not intersecting) filters?
        %%  (this may be not required, depends on config update implementation)
        {topics, ?HOCON(?ARRAY(binary()), #{required => true})},
        {pool_size, ?HOCON(pos_integer(), #{default => emqx_vm:schedulers() * 2})}
    ].

desc(_) ->
    "todo".

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
    case Dups of
        [] -> ok;
        _ -> {error, #{reason => duplicated_cluster_links, names => Dups}}
    end.

link_name(#{upstream := Name}) -> Name;
link_name(#{<<"upstream">> := Name}) -> Name.