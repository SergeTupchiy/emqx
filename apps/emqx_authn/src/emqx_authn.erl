%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authn).

%% Data backup
-import_data(import_data).

-export([
    providers/0,
    check_config/1,
    check_config/2,
    %% for telemetry information
    get_enabled_authns/0
]).

%% Data backup
-export([
    import_data/1
]).

-include("emqx_authn.hrl").

providers() ->
    [
        {{password_based, built_in_database}, emqx_authn_mnesia},
        {{password_based, mysql}, emqx_authn_mysql},
        {{password_based, postgresql}, emqx_authn_pgsql},
        {{password_based, mongodb}, emqx_authn_mongodb},
        {{password_based, redis}, emqx_authn_redis},
        {{password_based, http}, emqx_authn_http},
        {jwt, emqx_authn_jwt},
        {{scram, built_in_database}, emqx_enhanced_authn_scram_mnesia}
    ].

check_config(Config) ->
    check_config(Config, #{}).

check_config(Config, Opts) ->
    case do_check_config(Config, Opts) of
        #{?CONF_NS_ATOM := Checked} -> Checked;
        #{?CONF_NS_BINARY := WithDefaults} -> WithDefaults
    end.

do_check_config(#{<<"mechanism">> := Mec0} = Config, Opts) ->
    Mec = atom(Mec0, #{error => unknown_mechanism}),
    Key =
        case maps:get(<<"backend">>, Config, false) of
            false -> Mec;
            Backend -> {Mec, atom(Backend, #{error => unknown_backend})}
        end,
    case lists:keyfind(Key, 1, providers()) of
        false ->
            Reason =
                case Key of
                    {M, B} ->
                        #{mechanism => M, backend => B};
                    M ->
                        #{mechanism => M}
                end,
            throw(Reason#{error => unknown_authn_provider});
        {_, ProviderModule} ->
            do_check_config_maybe_throw(ProviderModule, Config, Opts)
    end;
do_check_config(Config, _Opts) when is_map(Config) ->
    throw(#{
        error => invalid_config,
        reason => "mechanism_field_required"
    }).

do_check_config_maybe_throw(ProviderModule, Config0, Opts) ->
    Config = #{?CONF_NS_BINARY => Config0},
    case emqx_hocon:check(ProviderModule, Config, Opts#{atom_key => true}) of
        {ok, Checked} ->
            Checked;
        {error, Reason} ->
            throw(Reason)
    end.

%% The atoms have to be loaded already,
%% which might be an issue for plugins which are loaded after node boot
%% but they should really manage their own configs in that case.
atom(Bin, ErrorContext) ->
    try
        binary_to_existing_atom(Bin, utf8)
    catch
        _:_ ->
            throw(ErrorContext#{value => Bin})
    end.

-spec get_enabled_authns() ->
    #{
        authenticators => [authenticator_id()],
        overridden_listeners => #{authenticator_id() => pos_integer()}
    }.
get_enabled_authns() ->
    %% at the moment of writing, `emqx_authentication:list_chains/0'
    %% result is always wrapped in `{ok, _}', and it cannot return any
    %% error values.
    {ok, Chains} = emqx_authentication:list_chains(),
    AuthnTypes = lists:usort([
        Type
     || #{authenticators := As} <- Chains,
        #{id := Type} <- As
    ]),
    OverriddenListeners =
        lists:foldl(
            fun
                (#{name := ?GLOBAL}, Acc) ->
                    Acc;
                (#{authenticators := As}, Acc) ->
                    lists:foldl(fun tally_authenticators/2, Acc, As)
            end,
            #{},
            Chains
        ),
    #{
        authenticators => AuthnTypes,
        overridden_listeners => OverriddenListeners
    }.

tally_authenticators(#{id := AuthenticatorName}, Acc) ->
    maps:update_with(AuthenticatorName, fun(N) -> N + 1 end, 1, Acc).

%%------------------------------------------------------------------------------
%% Data backup
%%------------------------------------------------------------------------------

-define(IMPORT_OPTS, #{rawconf_with_defaults => true, override_to => cluster}).

import_data(RawConf) ->
    AuthnList = maps:get(<<"authentication">>, RawConf, []),
    lists:foreach(
        fun(Authn) ->
            case update_conf(update_req(Authn)) of
                %% Assume not_found error
                {error, _} ->
                    {ok, _} = update_conf(create_req(Authn));
                {ok, _} ->
                    ok
            end
        end,
        AuthnList
    ).

update_req(Authn) ->
    {update_authenticator, ?GLOBAL, emqx_authentication:authenticator_id(Authn), Authn}.

create_req(Authn) ->
    {create_authenticator, ?GLOBAL, Authn}.

update_conf(UpdateReq) ->
    emqx_conf:update(
        [authentication],
        UpdateReq,
        #{rawconf_with_defaults => true, override_to => cluster}
    ).
