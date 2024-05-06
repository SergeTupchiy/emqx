%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_extrouter_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/asserts.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    ok = init_db(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(TC, Config) ->
    emqx_common_test_helpers:init_per_testcase(?MODULE, TC, Config).

end_per_testcase(TC, Config) ->
    emqx_common_test_helpers:end_per_testcase(?MODULE, TC, Config).

init_db() ->
    mria:wait_for_tables(emqx_cluster_link_extrouter:create_tables()).

%%

t_consistent_routing_view(_Config) ->
    Actor1 = {?FUNCTION_NAME, 1},
    Actor2 = {?FUNCTION_NAME, 2},
    Actor3 = {?FUNCTION_NAME, 3},
    {ok, AS10} = emqx_cluster_link_extrouter:actor_init(Actor1, 1, env()),
    {ok, AS20} = emqx_cluster_link_extrouter:actor_init(Actor2, 1, env()),
    {ok, AS30} = emqx_cluster_link_extrouter:actor_init(Actor3, 1, env()),
    %% Add few routes originating from different actors.
    %% Also test that route operations are idempotent.
    AS11 = apply_operation({add, <<"t/client/#">>, id}, AS10),
    _AS11 = apply_operation({add, <<"t/client/#">>, id}, AS10),
    AS21 = apply_operation({add, <<"t/client/#">>, id}, AS20),
    AS31 = apply_operation({add, <<"t/client/+/+">>, id1}, AS30),
    AS32 = apply_operation({add, <<"t/client/+/+">>, id2}, AS31),
    _AS22 = apply_operation({del, <<"t/client/#">>, id}, AS21),
    AS12 = apply_operation({add, <<"t/client/+/+">>, id1}, AS11),
    AS33 = apply_operation({del, <<"t/client/+/+">>, id1}, AS32),
    _AS34 = apply_operation({del, <<"t/client/+/+">>, id2}, AS33),
    ?assertEqual(
        [<<"t/client/#">>, <<"t/client/+/+">>],
        topics_sorted()
    ),
    ?assertEqual(
        [<<"t/client/#">>],
        lists:sort(emqx_cluster_link_extrouter:match_routes(<<"t/client/42">>))
    ),
    %% Remove all routes from the actors.
    AS13 = apply_operation({del, <<"t/client/#">>, id}, AS12),
    AS14 = apply_operation({del, <<"t/client/+/+">>, id1}, AS13),
    AS14 = apply_operation({del, <<"t/client/+/+">>, id1}, AS13),
    ?assertEqual(
        [],
        topics_sorted()
    ).

t_actor_reincarnation(_Config) ->
    Actor1 = {?FUNCTION_NAME, 1},
    Actor2 = {?FUNCTION_NAME, 2},
    {ok, AS10} = emqx_cluster_link_extrouter:actor_init(Actor1, 1, env()),
    {ok, AS20} = emqx_cluster_link_extrouter:actor_init(Actor2, 1, env()),
    AS11 = apply_operation({add, <<"topic/#">>, id}, AS10),
    AS12 = apply_operation({add, <<"topic/42/+">>, id}, AS11),
    AS21 = apply_operation({add, <<"topic/#">>, id}, AS20),
    ?assertEqual(
        [<<"topic/#">>, <<"topic/42/+">>],
        topics_sorted()
    ),
    {ok, _AS3} = emqx_cluster_link_extrouter:actor_init(Actor1, 2, env()),
    ?assertError(
        _IncarnationMismatch,
        apply_operation({add, <<"toolate/#">>, id}, AS12)
    ),
    ?assertEqual(
        [<<"topic/#">>],
        topics_sorted()
    ),
    {ok, _AS4} = emqx_cluster_link_extrouter:actor_init(Actor2, 2, env()),
    ?assertError(
        _IncarnationMismatch,
        apply_operation({add, <<"toolate/#">>, id}, AS21)
    ),
    ?assertEqual(
        [],
        topics_sorted()
    ).

t_actor_gc(_Config) ->
    Actor1 = {?FUNCTION_NAME, 1},
    Actor2 = {?FUNCTION_NAME, 2},
    {ok, AS10} = emqx_cluster_link_extrouter:actor_init(Actor1, 1, env()),
    {ok, AS20} = emqx_cluster_link_extrouter:actor_init(Actor2, 1, env()),
    AS11 = apply_operation({add, <<"topic/#">>, id}, AS10),
    AS12 = apply_operation({add, <<"topic/42/+">>, id}, AS11),
    AS21 = apply_operation({add, <<"global/#">>, id}, AS20),
    ?assertEqual(
        [<<"global/#">>, <<"topic/#">>, <<"topic/42/+">>],
        topics_sorted()
    ),
    _AS13 = apply_operation(heartbeat, AS12, 50_000),
    ok = emqx_cluster_link_extrouter:actor_gc(env(60_000)),
    ?assertEqual(
        [<<"topic/#">>, <<"topic/42/+">>],
        topics_sorted()
    ),
    ?assertError(
        _IncarnationMismatch,
        apply_operation({add, <<"toolate/#">>, id}, AS21)
    ),
    ok = emqx_cluster_link_extrouter:actor_gc(env(120_000)),
    ?assertEqual(
        [],
        topics_sorted()
    ).

t_consistent_routing_view_concurrent_updates(_Config) ->
    A1Seq = repeat(10, [
        reincarnate,
        {add, <<"t/client/#">>, id},
        {add, <<"t/client/+/+">>, id1},
        {add, <<"t/client/+/+">>, id1},
        {del, <<"t/client/#">>, id}
    ]),
    A2Seq = repeat(10, [
        {add, <<"global/#">>, id},
        {add, <<"t/client/+/+">>, id1},
        {add, <<"t/client/+/+">>, id2},
        {del, <<"t/client/+/+">>, id1},
        heartbeat
    ]),
    A3Seq = repeat(10, [
        {add, <<"global/#">>, id},
        {del, <<"global/#">>, id},
        {add, <<"t/client/+/+">>, id1},
        {del, <<"t/client/+/+">>, id1},
        {add, <<"t/client/+/+">>, id2},
        {del, <<"t/client/+/+">>, id2},
        reincarnate
    ]),
    A4Seq = repeat(10, [
        gc,
        {sleep, 1}
    ]),
    _ = emqx_utils:pmap(
        fun run_actor/1,
        [
            {{?FUNCTION_NAME, 1}, A1Seq},
            {{?FUNCTION_NAME, 2}, A2Seq},
            {{?FUNCTION_NAME, 3}, A3Seq},
            {{?FUNCTION_NAME, gc}, A4Seq}
        ],
        infinity
    ),
    ?assertEqual(
        [<<"global/#">>, <<"t/client/+/+">>, <<"t/client/+/+">>],
        topics_sorted()
    ).

t_consistent_routing_view_concurrent_cluster_updates('init', Config) ->
    Specs = [
        {emqx_external_router1, #{role => core}},
        {emqx_external_router2, #{role => core}},
        {emqx_external_router3, #{role => core}}
    ],
    Cluster = emqx_cth_cluster:start(
        Specs,
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    ok = lists:foreach(
        fun(Node) ->
            ok = erpc:call(Node, ?MODULE, init_db, [])
        end,
        Cluster
    ),
    [{cluster, Cluster} | Config];
t_consistent_routing_view_concurrent_cluster_updates('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(cluster, Config)).

t_consistent_routing_view_concurrent_cluster_updates(Config) ->
    [N1, N2, N3] = ?config(cluster, Config),
    A1Seq = repeat(10, [
        reincarnate,
        {add, <<"t/client/#">>, id},
        {add, <<"t/client/+/+">>, id1},
        {add, <<"t/client/+/+">>, id1},
        {del, <<"t/client/#">>, id}
    ]),
    A2Seq = repeat(10, [
        {add, <<"global/#">>, id},
        {add, <<"t/client/+/+">>, id1},
        {add, <<"t/client/+/+">>, id2},
        {del, <<"t/client/+/+">>, id1},
        heartbeat
    ]),
    A3Seq = repeat(10, [
        {add, <<"global/#">>, id},
        {del, <<"global/#">>, id},
        {add, <<"t/client/+/+">>, id1},
        {del, <<"t/client/+/+">>, id1},
        {add, <<"t/client/+/+">>, id2},
        {del, <<"t/client/+/+">>, id2},
        reincarnate
    ]),
    A4Seq = repeat(10, [
        gc,
        {sleep, 1}
    ]),
    Runners = lists:map(
        fun run_remote_actor/1,
        [
            {N1, {{?FUNCTION_NAME, 1}, A1Seq}},
            {N2, {{?FUNCTION_NAME, 2}, A2Seq}},
            {N3, {{?FUNCTION_NAME, 3}, A3Seq}},
            {N3, {{?FUNCTION_NAME, gc}, A4Seq}}
        ]
    ),
    [?assertReceive({'DOWN', MRef, _, Pid, normal}) || {Pid, MRef} <- Runners],
    ?assertEqual(
        [<<"global/#">>, <<"t/client/+/+">>, <<"t/client/+/+">>],
        erpc:call(N1, ?MODULE, topics_sorted, [])
    ).

run_remote_actor({Node, Run}) ->
    erlang:spawn_monitor(Node, ?MODULE, run_actor, [Run]).

run_actor({Actor, Seq}) ->
    {ok, AS0} = emqx_cluster_link_extrouter:actor_init(Actor, 0, env(0)),
    lists:foldl(
        fun
            ({TS, {add, _, _} = Op}, AS) ->
                apply_operation(Op, AS, TS);
            ({TS, {del, _, _} = Op}, AS) ->
                apply_operation(Op, AS, TS);
            ({TS, heartbeat}, AS) ->
                apply_operation(heartbeat, AS, TS);
            ({TS, gc}, AS) ->
                ok = emqx_cluster_link_extrouter:actor_gc(env(TS)),
                AS;
            ({_TS, {sleep, MS}}, AS) ->
                ok = timer:sleep(MS),
                AS;
            ({TS, reincarnate}, _AS) ->
                {ok, AS} = emqx_cluster_link_extrouter:actor_init(Actor, TS, env(TS)),
                AS
        end,
        AS0,
        lists:enumerate(Seq)
    ).

%%

apply_operation(Op, AS) ->
    apply_operation(Op, AS, _TS = 42).

apply_operation(Op, AS, TS) ->
    emqx_cluster_link_extrouter:actor_apply_operation(Op, AS, env(TS)).

env() ->
    env(42).

env(TS) ->
    #{timestamp => TS}.

topics_sorted() ->
    lists:sort(emqx_cluster_link_extrouter:topics()).

%%

repeat(N, L) ->
    lists:flatten(lists:duplicate(N, L)).
