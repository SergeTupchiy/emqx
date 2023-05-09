%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_listeners_conf).

%% Data backup
-import_data(import_data).

-export([
    action/4,
    create/3,
    ensure_remove/2,
    get_raw/2,
    update/3
]).

%% Data backup
-export([
    import_data/1
]).

-define(CONF_ROOT_KEY, listeners).
-define(path(_Type_, _Name_), [?CONF_ROOT_KEY, _Type_, _Name_]).
-define(OPTS, #{rawconf_with_defaults => true, override_to => cluster}).

action(Type, Name, Action, Conf) ->
    wrap(emqx_conf:update(?path(Type, Name), {action, Action, Conf}, ?OPTS)).

create(Type, Name, Conf) ->
    wrap(emqx_conf:update(?path(Type, Name), {create, Conf}, ?OPTS)).

ensure_remove(Type, Name) ->
    wrap(emqx_conf:tombstone(?path(Type, Name), ?OPTS)).

get_raw(Type, Name) -> emqx_conf:get_raw(?path(Type, Name), undefined).

update(Type, Name, Conf) ->
    wrap(emqx_conf:update(?path(Type, Name), {update, Conf}, ?OPTS)).

wrap({error, {post_config_update, emqx_listeners, Reason}}) -> {error, Reason};
wrap({error, {pre_config_update, emqx_listeners, Reason}}) -> {error, Reason};
wrap({error, Reason}) -> {error, Reason};
wrap(Ok) -> Ok.

%%------------------------------------------------------------------------------
%% Data backup
%%------------------------------------------------------------------------------

import_data(RawConf) ->
    ListenersConf = maps:get(<<"listeners">>, RawConf, #{}),
    _ = maps:map(
        fun(Type, Listeners) ->
            maps:map(
                fun(Name, ListenerConf) ->
                    {AuthnList, ListenerConf1} = take_authn(ListenerConf),
                    Path = ?path(Type, Name),
                    Action =
                        case emqx:get_raw_config(Path, undefined) of
                            undefined -> create;
                            _ -> update
                        end,
                    {ok, _} = emqx_conf:update(Path, {Action, ListenerConf1}, ?OPTS),
                    maybe_update_authn(AuthnList, Type, Name)
                end,
                Listeners
            )
        end,
        ListenersConf
    ),
    ok.

take_authn(ListenerConf) ->
    case maps:take(<<"authentication">>, ListenerConf) of
        error -> {[], ListenerConf};
        AuthnAndConf -> AuthnAndConf
    end.

maybe_update_authn([], _Type, _Name) ->
    ok;
maybe_update_authn(AuthnList, Type, Name) ->
    lists:foreach(
        fun(Authn) ->
            ListenerID = emqx_listeners:listener_id(Type, Name),
            AuthnID = emqx_authentication:authenticator_id(Authn),
            UpdateReq = {update_authenticator, ListenerID, AuthnID, Authn},
            case update_authn(Type, Name, UpdateReq) of
                {error, {not_found, _}} ->
                    CreateReq = {create_authenticator, ListenerID, Authn},
                    {ok, _} = update_authn(Type, Name, CreateReq);
                {ok, _} ->
                    ok
            end
        end,
        AuthnList
    ).

update_authn(Type, Name, UpdateReq) ->
    emqx_conf:update(
        ?path(Type, Name) ++ [<<"authentication">>],
        UpdateReq,
        #{rawconf_with_defaults => true, override_to => cluster}
    ).
