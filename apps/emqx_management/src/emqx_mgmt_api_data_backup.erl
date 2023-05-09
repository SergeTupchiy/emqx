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

-module(emqx_mgmt_api_data_backup).

-behaviour(minirest_api).

-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([api_spec/0, paths/0, schema/1, fields/1]).

-export([
    data_export/2,
    data_import/2,
    data_file/2,
    data_file_by_name/2
]).

-define(TAGS, [<<"Data Backup">>]).

-define(node_field(IsRequired), ?node_field(IsRequired, #{})).
-define(node_field(IsRequired, Meta),
    {node, ?HOCON(binary(), Meta#{desc => "Node name", required => IsRequired})}
).
-define(filename_field(IsRequired), ?filename_field(IsRequired, #{})).
-define(filename_field(IsRequired, Meta),
    {filename,
        ?HOCON(binary(), Meta#{
            desc => "Data backup file name",
            required => IsRequired
        })}
).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/data/export",
        "/data/import",
        "/data/file",
        "/data/file/:filename"
    ].

%%TODO: add error responses
schema("/data/export") ->
    #{
        'operationId' => data_export,
        post => #{
            tags => ?TAGS,
            desc => <<"Export a data backup file">>,
            responses => #{
                200 =>
                    emqx_dashboard_swagger:schema_with_example(
                        ?R_REF(backup_file_info),
                        backup_file_info_example()
                    )
            }
        },
        get => #{
            tags => ?TAGS,
            desc => <<"List exported backup files">>,
            responses => #{
                200 =>
                    emqx_dashboard_swagger:schema_with_example(
                        ?ARRAY(?R_REF(backup_file_info)),
                        [backup_file_info_example()]
                    )
            }
        }
    };
schema("/data/import") ->
    #{
        'operationId' => data_import,
        post => #{
            tags => ?TAGS,
            desc => <<"Import a data backup file">>,
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                ?R_REF(import_request_body),
                maps:with([node, filename], backup_file_info_example())
            ),

            responses => #{
                204 => <<"No Content">>
            }
        }
    };
schema("/data/file") ->
    #{
        'operationId' => data_file,
        post => #{
            tags => ?TAGS,
            desc => <<"Upload a data backup file">>,
            'requestBody' => emqx_dashboard_swagger:file_schema(filename),
            responses => #{
                204 => <<"No Content">>
            }
        }
    };
schema("/data/file/:filename") ->
    #{
        'operationId' => data_file_by_name,
        get => #{
            tags => ?TAGS,
            desc => <<"Download a data backup file">>,
            parameters => [
                ?filename_field(true, #{in => path}),
                ?node_field(false, #{in => query})
            ],
            responses => #{
                200 => ?HOCON(binary)
            }
        },
        delete => #{
            tags => ?TAGS,
            desc => <<"Delete a data backup file">>,
            parameters => [
                ?filename_field(true, #{in => path}),
                ?node_field(false, #{in => query})
            ],
            responses => #{
                204 => <<"No Content">>
            }
        }
    }.

fields(backup_file_info) ->
    [
        ?node_field(true),
        ?filename_field(true),
        {created_at,
            ?HOCON(binary(), #{
                desc => "Data backup file creation date and time",
                required => true
            })}
    ];
fields(import_request_body) ->
    [?node_field(false), ?filename_field(true)];
fields(data_backup_file) ->
    [
        ?filename_field(true),
        {file,
            ?HOCON(binary(), #{
                desc => "Data backup file content",
                required => true
            })}
    ].

%%------------------------------------------------------------------------------
%% HTTP API Callbacks
%%------------------------------------------------------------------------------

data_export(post, _Request) ->
    case emqx_mgmt_data_backup:export() of
        {ok, File} ->
            {200, File};
        Error ->
            Error
    end;
%% TODO: maybe move to GET /data/file?
data_export(get, _Request) ->
    {200, list_backup_files()}.

data_import(post, #{body := #{<<"filename">> := Filename} = Body}) ->
    case safe_parse_node(Body) of
        error ->
            {400, #{code => 'BAD_REQUEST', message => <<"Unknown node">>}};
        Node ->
            response(emqx_mgmt_data_backup_proto_v1:import_backup_file(Node, Filename, infinity))
    end.

data_file(post, #{body := #{<<"filename">> := Filename, <<"file">> := FileContent}}) ->
    case emqx_mgmt_data_backup:upload_backup_file(Filename, FileContent) of
        ok ->
            {204};
        {error, bad_json} ->
            {400, #{code => 'BAD_REQUEST', message => <<"Invalid JSON backup file content">>}}
    end.

data_file_by_name(Method, #{bindings := #{filename := Filename}, query_string := QS}) ->
    case safe_parse_node(QS) of
        error ->
            {400, #{code => 'BAD_REQUEST', message => <<"Unknown node">>}};
        Node ->
            response(get_or_delete_file(Method, Filename, Node))
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

get_or_delete_file(get, Filename, Node) ->
    emqx_mgmt_data_backup_proto_v1:read_backup_file(Node, Filename, infinity);
get_or_delete_file(delete, Filename, Node) ->
    emqx_mgmt_data_backup_proto_v1:delete_backup_file(Node, Filename, infinity).

safe_parse_node(#{<<"node">> := NodeBin}) ->
    NodesBin = [erlang:atom_to_binary(N, utf8) || N <- emqx:running_nodes()],
    case lists:member(NodeBin, NodesBin) of
        true -> erlang:binary_to_atom(NodeBin, utf8);
        false -> error
    end;
safe_parse_node(_) ->
    node().

response({ok, Res}) ->
    {200, Res};
response(ok) ->
    {204};
response({error, not_found}) ->
    {400, #{code => 'BAD_REQUEST', message => <<"Backup file not found">>}}.

list_backup_files() ->
    Nodes = emqx:running_nodes(),
    Results = emqx_mgmt_data_backup_proto_v1:list_backup_files(Nodes, 10_0000),
    NodeResults = lists:zip(Nodes, Results),
    {Successes, Failures} =
        lists:partition(
            fun({_Node, Result}) ->
                case Result of
                    {ok, _} -> true;
                    _ -> false
                end
            end,
            NodeResults
        ),
    case Failures of
        [] ->
            ok;
        [_ | _] ->
            ?SLOG(error, #{msg => "list_exported_backup_files_failed", node_errors => Failures})
    end,
    FileList = lists:keysort(1, [
        FileInfo
     || {_Node, {ok, FileInfos}} <- Successes, FileInfo <- FileInfos
    ]),
    [Info || {_Timestamp, Info} <- FileList].

backup_file_info_example() ->
    #{
        created_at => <<"2023-5-11 17:43:24">>,
        filename => <<"emqx-export-2023-5-11-17-43-24.json">>,
        node => 'emqx@127.0.0.1',
        size => 22740
    }.
