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

-module(emqx_mgmt_data_backup).

-export([
    export/0,
    import/1,
    list_backup_files/0,
    read_backup_file/1,
    delete_backup_file/1,
    upload_backup_file/2
]).

-include_lib("kernel/include/file.hrl").
-include_lib("emqx/include/logger.hrl").

-define(BACKUP_DIR, "backup").
-define(BACKUP_CERTS_DIR, "certs").
-define(BACKUP_ACL_FILEPATH, ["authz", "acl.conf"]).
-define(BACKUP_MNESIA_DIR, "mnesia").
-define(TAR_SUFFIX, ".tar.gz").
-define(META_FILENAME, "META.hocon").
-define(CLUSTER_HOCON_FILENAME, "cluster.hocon").
-define(CONF_KEYS, [
    <<"mqtt">>,
    <<"alarm">>,
    <<"sysmon">>,
    <<"sys_topics">>,
    <<"limiter">>,
    <<"log">>,
    <<"persistent_session_store">>,
    <<"crl_cache">>,
    <<"conn_congestion">>,
    <<"force_shutdown">>,
    <<"flapping_detect">>,
    <<"broker">>,
    <<"force_gc">>,
    <<"zones">>
]).

-type backup_file_info() :: #{atom() => _}.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec export() -> {ok, backup_file_info()} | {error, _}.
export() ->
    try
        do_export()
    catch
        Class:Reason:Stack ->
            ?SLOG(error, #{
                msg => "emqx_data_export_failed",
                exception => Class,
                reason => Reason,
                stacktrace => Stack
            }),
            {error, Reason}
    end.

-spec import(file:filename_all()) -> ok | {error, _}.
import(BackupFileName) ->
    case is_import_allowed() of
        true ->
            case extract_backup(str(BackupFileName)) of
                {ok, BackupDir} ->
                    case validate_backup(BackupDir) of
                        {ok, Meta} ->
                            do_import(BackupDir, Meta);
                        Err ->
                            Err
                    end;
                Err ->
                    Err
            end;
        false ->
            {error, not_core_node}
    end.

-spec list_backup_files() -> [{non_neg_integer(), backup_file_info()}].
list_backup_files() ->
    Filter =
        fun(File) ->
            case file:read_file_info(File) of
                {ok, #file_info{size = Size, ctime = CTime = {{Y, M, D}, {H, MM, S}}}} ->
                    Seconds = calendar:datetime_to_gregorian_seconds(CTime),
                    BaseFilename = bin(filename:basename(File)),
                    CreatedAt = bin(io_lib:format("~p-~p-~p ~p:~p:~p", [Y, M, D, H, MM, S])),
                    Info = {
                        Seconds,
                        #{
                            filename => BaseFilename,
                            size => Size,
                            created_at => CreatedAt,
                            node => node()
                        }
                    },
                    {true, Info};
                _ ->
                    false
            end
        end,
    lists:filtermap(Filter, backup_files()).

-spec upload_backup_file(file:filename_all(), binary()) -> ok | {error, _}.
upload_backup_file(Filename0, ContentBin) ->
    Filename1 = filename:basename(str(Filename0)),
    case ensure_file_name(Filename1) of
        {ok, Filename} ->
            ?SLOG(info, #{msg => "write_backup_file", filename => Filename}),
            file:write_file(Filename, ContentBin);
        {error, Reason} ->
            {error, Reason}
    end.

-spec read_backup_file(file:filename_all()) ->
    {ok, #{filename => binary(), file => binary()}} | {error, _}.
read_backup_file(FileName) ->
    apply_file_fun(FileName, fun read_file/1).

-spec delete_backup_file(file:filename_all()) -> ok | {error, _}.
delete_backup_file(FileName) ->
    apply_file_fun(FileName, fun delete_file/1).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

do_export() ->
    {{Y, M, D}, {H, MM, S}} = _DateTime = local_datetime(),
    BackupBaseName = str(
        io_lib:format(
            "emqx-export-~p-~p-~p-~p-~p-~p",
            [Y, M, D, H, MM, S]
        )
    ),
    Meta = #{
        version => emqx_release:version(),
        edition => emqx_release:edition(),
        export_data_dir => emqx:data_dir()
    },
    MetaBin = bin(hocon_pp:do(Meta, #{})),
    BackupName = filename:join(backup_dir(), BackupBaseName),
    BackupTarName = BackupName ++ ?TAR_SUFFIX,
    MetaFileName = filename:join(BackupBaseName, ?META_FILENAME),
    {ok, TarDescriptor} = erl_tar:open(BackupTarName, [write, compressed]),
    ok = erl_tar:add(TarDescriptor, MetaBin, MetaFileName, []),
    ok = export_cluster_hocon(TarDescriptor, BackupBaseName),
    ok = export_certs(TarDescriptor, BackupBaseName),
    ok = export_acl_file(TarDescriptor, BackupBaseName),
    ok = export_mnesia_tabs(TarDescriptor, BackupName, BackupBaseName),

    _ = erl_tar:close(TarDescriptor),
    %% TODO: do after cleanup
    ok = file:del_dir_r(BackupName),
    {ok, #file_info{
        size = Size,
        ctime = {{Y1, M1, D1}, {H1, MM1, S1}}
    }} = file:read_file_info(BackupTarName),
    CreatedAt = io_lib:format("~p-~p-~p ~p:~p:~p", [Y1, M1, D1, H1, MM1, S1]),
    {ok, #{
        filename => bin(BackupTarName),
        size => Size,
        created_at => bin(CreatedAt),
        node => node()
    }}.

export_cluster_hocon(TarDescriptor, BackupBaseName) ->
    ClusterHoconFile = emqx:cluster_hocon_file(),
    case filelib:is_regular(ClusterHoconFile) of
        true ->
            NameInArchive = filename:join(BackupBaseName, ?CLUSTER_HOCON_FILENAME),
            ok = erl_tar:add(TarDescriptor, ClusterHoconFile, NameInArchive, []);
        false ->
            ?SLOG(info, #{msg => "no_cluster_hocon_file_to_export"}),
            ok
    end.

export_certs(TarDescriptor, BackupBaseName) ->
    BackupCertsDir = filename:join(BackupBaseName, ?BACKUP_CERTS_DIR),
    CertsDir = string:trim(emqx:mutable_certs_dir(), trailing, "/") ++ "/",
    AllCerts = filelib:wildcard(filename:join(CertsDir, "**")),
    lists:foreach(
        fun(CertFileName) ->
            NameInArchive = filename:join(BackupCertsDir, string:prefix(CertFileName, CertsDir)),
            ok = erl_tar:add(TarDescriptor, CertFileName, NameInArchive, [])
        end,
        AllCerts
    ).

export_acl_file(TarDescriptor, BackupBaseName) ->
    AclFile = emqx_authz:acl_conf_file(),
    case filelib:is_regular(AclFile) of
        true ->
            NameInArchive = filename:join([BackupBaseName | ?BACKUP_ACL_FILEPATH]),
            ok = erl_tar:add(TarDescriptor, AclFile, NameInArchive, []);
        false ->
            ?SLOG(info, #{msg => "no_acl_file_to_export"}),
            ok
    end.

export_mnesia_tabs(TarDescriptor, BackupName, BackupBaseName) ->
    lists:foreach(
        fun(Tab) -> export_mnesia_tab(TarDescriptor, Tab, BackupName, BackupBaseName) end,
        mnesia_tabs_to_backup()
    ).

export_mnesia_tab(TarDescriptor, TabName, BackupName, BackupBaseName) ->
    {ok, MnesiaBackupName} = do_export_mnesia_tab(TabName, BackupName),
    NameInArchive = mnesia_backup_name(BackupBaseName, TabName),
    ok = erl_tar:add(TarDescriptor, MnesiaBackupName, NameInArchive, []).

do_export_mnesia_tab(TabName, BackupName) ->
    Node = node(),
    try
        {ok, TabName, [Node]} = mnesia:activate_checkpoint(
            [{name, TabName}, {min, [TabName]}, {allow_remote, false}]
        ),
        MnesiaBackupName = mnesia_backup_name(BackupName, TabName),
        ok = filelib:ensure_dir(MnesiaBackupName),
        ok = mnesia:backup_checkpoint(TabName, MnesiaBackupName),
        {ok, MnesiaBackupName}
    after
        mnesia:deactivate_checkpoint(TabName)
    end.

mnesia_tabs_to_backup() ->
    %% TODO: avoid using ekka_boot module directly?
    [Tab || {_App, _Mod, Tabs} <- ekka_boot:all_module_attributes(backup_mnesia), Tab <- Tabs].

mnesia_backup_name(Path, TabName) ->
    filename:join([Path, ?BACKUP_MNESIA_DIR, atom_to_list(TabName)]).

is_import_allowed() ->
    mria_rlog:role() =:= core.

validate_backup(BackupDir) ->
    case hocon:files([filename:join(BackupDir, ?META_FILENAME)]) of
        {ok, #{
            <<"edition">> := Edition,
            <<"export_data_dir">> := _,
            <<"version">> := Version
        }} = Meta ->
            validate(
                [
                    fun() -> check_edition(Edition) end,
                    fun() -> check_version(Version) end
                ],
                Meta
            );
        _ ->
            ?SLOG(error, #{msg => "missing_backup_meta", backup => BackupDir}),
            {error, missing_backup_meta}
    end.

validate([ValidatorFun | T], OkRes) ->
    case ValidatorFun() of
        ok -> validate(T, OkRes);
        Err -> Err
    end;
validate([], OkRes) ->
    OkRes.

check_edition(BackupEdition) when BackupEdition =:= <<"ce">>; BackupEdition =:= <<"ee">> ->
    Edition = bin(emqx_release:edition()),
    case {BackupEdition, Edition} of
        {<<"ee">>, <<"ce">>} ->
            {error, ee_to_ce_backup};
        _ ->
            ok
    end;
check_edition(BackupEdition) ->
    ?SLOG(error, #{msg => "invalid_backup_edition", edition => BackupEdition}),
    {error, invalid_edition}.

check_version(Version) ->
    case version_no_patch(Version) of
        {ok, _} ->
            %% No checks for now, v4.x JSON backup format is anyway not compatible and will
            %% need to be transformed to v5
            ok;
        Err ->
            Err
    end.

version_no_patch(Version) ->
    case string:split(Version, ".", all) of
        [Major, Minor | _] ->
            {ok, {Major, Minor}};
        _ ->
            ?SLOG(error, #{msg => "failed_to_parse_backup_version", version => Version}),
            {error, invalid_version}
    end.

do_import(BackupDir, #{<<"export_data_dir">> := ExportDataDir} = _Meta) ->
    try
        ok = import_certs(BackupDir),
        ok = import_acl_file(BackupDir),
        ok = import_cluster_hocon(BackupDir, ExportDataDir),
        import_mnesia_tabs(BackupDir),
        ?SLOG(info, #{msg => "emqx_data_import_success"}),
        ok
    catch
        throw:Reason ->
            {error, Reason};
        Class:Reason:Stack ->
            ?SLOG(error, #{
                msg => "emqx_data_import_failed",
                exception => Class,
                reason => Reason,
                stacktrace => Stack
            }),
            {error, import_failed}
    after
        file:del_dir_r(BackupDir)
    end.

import_mnesia_tabs(BackupDir) ->
    lists:foreach(
        fun(Tab) -> import_mnesia_tab(BackupDir, Tab) end,
        mnesia_tabs_to_backup()
    ).

import_mnesia_tab(BackupDir, TabName) ->
    MnesiaBackupFileName = mnesia_backup_name(BackupDir, TabName),
    case filelib:is_regular(MnesiaBackupFileName) of
        true ->
            BackupNameToImport = MnesiaBackupFileName ++ "_for_import",
            {ok, _} = mnesia:traverse_backup(
                MnesiaBackupFileName, BackupNameToImport, fun backup_converter/2, 0
            ),
            {atomic, [TabName]} = mnesia:restore(BackupNameToImport, [{default_op, keep_tables}]),
            ok = file:delete(BackupNameToImport);
        false ->
            ?SLOG(info, #{msg => "missing_mnesia_backup", table => TabName, backup => BackupDir}),
            ok
    end.

backup_converter({schema, Tab, CreateList}, Acc) ->
    {[{schema, Tab, lists:map(fun convert_copies/1, CreateList)}], Acc};
backup_converter(Other, Acc) ->
    {[Other], Acc}.

convert_copies({K, [_ | _]}) when K == ram_copies; K == disc_copies; K == disc_only_copies ->
    {K, [node()]};
convert_copies(Other) ->
    Other.

extract_backup(BackupFileName) ->
    case {filelib:is_regular(BackupFileName), is_valid_backup_name(BackupFileName)} of
        {true, true} ->
            do_extract_backup(BackupFileName);
        {false, true} ->
            case lookup_file(BackupFileName) of
                {ok, FileName} ->
                    extract_backup(FileName);
                {error, not_found} ->
                    ?SLOG(error, #{
                        msg => "backup_file_not_found",
                        filename => BackupFileName
                    }),
                    {error, not_found}
            end;
        {_, false} ->
            {error, bad_name}
    end.

do_extract_backup(BackupFileName) ->
    BackupDir = backup_dir(),
    case erl_tar:extract(BackupFileName, [{cwd, BackupDir}, compressed]) of
        ok ->
            BackupName = filename:join(BackupDir, filename:basename(BackupFileName, ?TAR_SUFFIX)),
            case filelib:is_dir(BackupName) of
                true ->
                    {ok, BackupName};
                false ->
                    ?SLOG(error, #{
                        msg => "bad_backup_tar_archive_content",
                        filename => BackupFileName
                    }),
                    {error, bad_archive}
            end;
        {error, Reason} = Err ->
            ?SLOG(error, #{
                msg => "failed_to_extract_backup_tar_archive",
                filename => BackupFileName,
                reason => Reason
            }),
            Err
    end.

import_certs(BackupDir) ->
    BackupCertsDir = string:trim(filename:join(BackupDir, ?BACKUP_CERTS_DIR), trailing, "/") ++ "/",
    case filelib:is_dir(BackupCertsDir) of
        true ->
            CertsDir = emqx:mutable_certs_dir(),
            BackupCerts = lists:filter(
                fun filelib:is_regular/1, filelib:wildcard(filename:join(BackupCertsDir, "**"))
            ),
            lists:foreach(
                fun(BackupCertFileName) ->
                    NewCertFileName = filename:join(
                        CertsDir,
                        string:prefix(BackupCertFileName, BackupCertsDir)
                    ),
                    ok = filelib:ensure_dir(NewCertFileName),
                    {ok, _} = file:copy(BackupCertFileName, NewCertFileName),
                    ok
                end,
                BackupCerts
            );
        false ->
            ok
    end.

import_acl_file(BackupDir) ->
    ImportAclFile = filename:join([BackupDir | ?BACKUP_ACL_FILEPATH]),
    case filelib:is_regular(ImportAclFile) of
        true ->
            DestAclFile = emqx_authz:acl_conf_file(),
            ok = filelib:ensure_dir(DestAclFile),
            {ok, _} = file:copy(ImportAclFile, DestAclFile),
            ok;
        false ->
            ok
    end.

import_cluster_hocon(BackupDir, ExportDataDir) ->
    HoconFileName = filename:join(BackupDir, ?CLUSTER_HOCON_FILENAME),
    case filelib:is_regular(HoconFileName) of
        true ->
            case hocon:files([HoconFileName]) of
                {ok, RawConf} ->
                    RawConf1 = maybe_convert_data_dir(RawConf, ExportDataDir),
                    ok = validate_cluster_hocon(RawConf1),
                    do_import_conf(RawConf1);
                {error, Reason} = Err ->
                    ?SLOG(error, #{
                        msg => "failed_to_parse_backup_hocon_config",
                        filename => HoconFileName,
                        reason => Reason
                    }),
                    Err
            end;
        false ->
            ?SLOG(info, #{
                msg => "no_backup_hocon_config_to_import",
                backup_dir => BackupDir
            }),
            ok
    end.

maybe_convert_data_dir(RawConf, ExportDataDir) ->
    DataDir = bin(emqx:data_dir()),
    case ExportDataDir =:= DataDir of
        true ->
            RawConf;
        false ->
            emqx_utils_maps:deep_convert(RawConf, fun data_dir_converter/4, [ExportDataDir, DataDir])
    end.

data_dir_converter(Key, Val, FromDir, ToDir) ->
    Val1 =
        case Val of
            <<FromDir:(byte_size(FromDir))/binary, RestPath/binary>> ->
                <<ToDir/binary, RestPath/binary>>;
            V ->
                V
        end,
    {Key, Val1}.

validate_cluster_hocon(RawConf) ->
    SchemaMod = emqx_conf:schema_module(),
    CurrentRawConf = emqx:get_raw_config([]),
    %% TODO: maybe use emqx_hocon:check/2,3 instead?
    {_, _} = emqx_config:check_config(SchemaMod, hocon:deep_merge(CurrentRawConf, RawConf)),
    ok.

do_import_conf(RawConf) ->
    ok = import_generic_conf(RawConf),
    lists:foreach(
        fun({_App, Mod, [F]}) -> ok = Mod:F(RawConf) end,
        ekka_boot:all_module_attributes(import_data)
    ).

import_generic_conf(Data) ->
    lists:foreach(
        fun(Key) ->
            case maps:get(Key, Data, #{}) of
                Conf when map_size(Conf) > 0 ->
                    {ok, _} = emqx_conf:update([Key], Conf, #{override_to => cluster});
                _ ->
                    ok
            end
        end,
        ?CONF_KEYS
    ).

str(Data) when is_atom(Data) ->
    atom_to_list(Data);
str(Data) ->
    unicode:characters_to_list(Data).

bin(Data) when is_atom(Data) ->
    atom_to_binary(Data, utf8);
bin(Data) ->
    unicode:characters_to_binary(Data).

ensure_file_name(Filename) ->
    case is_valid_backup_name(Filename) of
        true ->
            {ok, filename:join(backup_dir(), Filename)};
        false ->
            {error, bad_filename}
    end.

is_valid_backup_name(FileName) ->
    BaseName = filename:basename(FileName, ?TAR_SUFFIX),
    BaseName ++ ?TAR_SUFFIX =:= filename:basename(FileName).

lookup_file(FileName) ->
    %% Only lookup by basename, don't allow to lookup by file path
    case FileName =:= filename:basename(FileName) of
        true ->
            FilePath = filename:join(backup_dir(), FileName),
            case filelib:is_file(FilePath) of
                true -> {ok, FilePath};
                false -> {error, not_found}
            end;
        false ->
            {error, not_found}
    end.

backup_files() ->
    backup_files(backup_dir()).

backup_files(Dir) ->
    filelib:wildcard(filename:join(Dir, "*" ++ ?TAR_SUFFIX)).

backup_dir() ->
    Dir = filename:join(emqx:data_dir(), ?BACKUP_DIR),
    ok = ensure_path(Dir),
    Dir.

-if(?OTP_RELEASE < 25).
ensure_path(Path) -> filelib:ensure_dir(filename:join([Path, "dummy"])).
-else.
ensure_path(Path) -> filelib:ensure_path(Path).
-endif.

%% TODO: consider increasing precision to ms
local_datetime() ->
    calendar:system_time_to_local_time(erlang:system_time(second), second).

read_file(FileName) ->
    case file:read_file(FileName) of
        {ok, Bin} ->
            {ok, #{
                filename => bin(FileName),
                file => Bin
            }};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_read_backup_file",
                filename => FileName,
                reason => Reason
            }),
            {error, Reason}
    end.

apply_file_fun(FileName, Fun) ->
    FileName1 = str(FileName),
    case is_valid_backup_name(FileName1) of
        true ->
            case lookup_file(FileName1) of
                {ok, FileName2} ->
                    Fun(FileName2);
                {error, not_found} ->
                    {error, not_found}
            end;
        false ->
            {error, bad_filename}
    end.

delete_file(FileName) ->
    case file:delete(FileName) of
        ok ->
            ?SLOG(info, #{msg => "backup_file_deleted", filename => FileName}),
            ok;
        {error, Reason} ->
            ?SLOG(
                error,
                #{
                    msg => "failed_to_delete_backup_file",
                    reason => Reason,
                    filename => FileName
                }
            ),
            {error, Reason}
    end.
