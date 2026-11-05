%%%   Copyright (c) 2026, Kafka4beam
%%%
%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%
-module(kpro_merge_produce_tests).

-include_lib("eunit/include/eunit.hrl").
-include("kpro_private.hrl").

-define(PARTITIONS, 4).
-define(BAD_PARTITION, 99).
-define(TIMEOUT, 5000).

merge_test() ->
  with_connection(
    #{merge_produce => true},
    fun(Conn, Vsn, [A, B | _]) ->
        Reqs = [req(Vsn, A), req(Vsn, A), req(Vsn, B), req(Vsn, A), req(Vsn, B)],
        {Rsps, KafkaReqs} = send_held(Conn, Reqs),
        ?assertEqual(3, KafkaReqs),
        [ {A, no_error, A1}
        , {A, no_error, A2}
        , {B, no_error, B1}
        , {A, no_error, A3}
        , {B, no_error, B2}
        ] = results(Rsps),
        ?assert(A1 < A2 andalso A2 < A3 andalso B1 < B2)
    end).

partition_error_test() ->
  with_connection(
    #{merge_produce => true},
    fun(Conn, Vsn, [A, B | _]) ->
        Reqs = [req(Vsn, A), req(Vsn, ?BAD_PARTITION), req(Vsn, B)],
        {Rsps, KafkaReqs} = send_held(Conn, Reqs),
        ?assertEqual(2, KafkaReqs),
        [ {A, no_error, _}
        , {?BAD_PARTITION, unknown_topic_or_partition, _}
        , {B, no_error, _}
        ] = results(Rsps)
    end).

atom_topic_test() ->
  with_connection(
    #{merge_produce => true},
    fun(Conn, Vsn, [A, B | _]) ->
        %% produce/6 also takes an atom topic, but a response has a binary topic.
        Topic = binary_to_atom(topic()),
        Reqs = [kpro_req_lib:produce(Vsn, Topic, P, [#{value => value()}]) || P <- [A, A, B]],
        {Rsps, KafkaReqs} = send_held(Conn, Reqs),
        ?assertEqual(2, KafkaReqs),
        [{A, no_error, _}, {A, no_error, _}, {B, no_error, _}] = results(Rsps)
    end).

different_acks_test() ->
  with_connection(
    #{merge_produce => true},
    fun(Conn, Vsn, [A, B | _]) ->
        Reqs = [req(Vsn, A), req(Vsn, A), req(Vsn, B, #{required_acks => 1}), req(Vsn, B)],
        {Rsps, KafkaReqs} = send_held(Conn, Reqs),
        %% The second request of B cannot go with the second request of A,
        %% because the first request of B has other acks and goes later.
        ?assertEqual(4, KafkaReqs),
        [{A, no_error, _}, {A, no_error, _}, {B, no_error, B1}, {B, no_error, B2}] =
          results(Rsps),
        ?assert(B1 < B2)
    end).

size_limit_test() ->
  with_connection(
    #{merge_produce => true, merge_produce_max_bytes => 1000},
    fun(Conn, Vsn, [A, B | _]) ->
        Big = binary:copy(<<"x">>, 600),
        Reqs = [req(Vsn, A), req(Vsn, A, #{value => Big}), req(Vsn, B, #{value => Big})],
        {Rsps, KafkaReqs} = send_held(Conn, Reqs),
        ?assertEqual(3, KafkaReqs),
        [{A, no_error, _}, {A, no_error, _}, {B, no_error, _}] = results(Rsps)
    end).

other_produce_request_test() ->
  with_connection(
    #{merge_produce => true},
    fun(Conn, Vsn, [A, B | _]) ->
        %% kpro_req_lib:make/3 encodes the whole request, so the connection
        %% cannot take it apart.
        Other = make_produce_req(Vsn, [A, B]),
        {[_, OtherRsp, _], _} = send_held(Conn, [req(Vsn, A), Other, req(Vsn, B)]),
        #kpro_rsp{ref = Ref, msg = Msg} = OtherRsp,
        ?assertEqual(Other#kpro_req.ref, Ref),
        [TopicRsp] = kpro:find(responses, Msg),
        PartitionRsps = kpro:find(partition_responses, TopicRsp),
        ?assertEqual([{A, no_error}, {B, no_error}],
                     lists:sort([{kpro:find(partition, P), kpro:find(error_code, P)}
                                 || P <- PartitionRsps]))
    end).

other_produce_request_order_test() ->
  with_connection(
    #{merge_produce => true},
    fun(Conn, Vsn, [A | _]) ->
        Reqs = [req(Vsn, A), req(Vsn, A), make_produce_req(Vsn, [A])],
        {Rsps, _} = send_held(Conn, Reqs),
        [{A, no_error, O1}, {A, no_error, O2}, {A, no_error, O3}] = results(Rsps),
        ?assert(O1 < O2 andalso O2 < O3)
    end).

no_ack_order_test() ->
  with_connection(
    #{merge_produce => true},
    fun(Conn, Vsn, [A | _]) ->
        [R1, R2, R4] = Acked = [req(Vsn, A), req(Vsn, A), req(Vsn, A)],
        ok = send_suspended(Conn, [R1, R2, req(Vsn, A, #{required_acks => 0}), R4]),
        [{A, no_error, O1}, {A, no_error, O2}, {A, no_error, O4}] =
          results([receive_rsp(Conn, R) || R <- Acked]),
        %% Each request has one record, and the request with no response has offset O2 + 1.
        ?assertEqual({O1 + 1, O2 + 2}, {O2, O4})
    end).

no_ack_other_partition_test() ->
  with_connection(
    #{merge_produce => true},
    fun(Conn, Vsn, [A, B | _]) ->
        %% No older request of B waits, so the request of B goes before the second request of A.
        [R1, R2, R3] = [req(Vsn, A), req(Vsn, A), req(Vsn, B, #{required_acks => 0})],
        {_, Sent} = trace_calls(Conn, {kpro_req_lib, encode, 3},
                                fun() ->
                                    ok = send_suspended(Conn, [R1, R2, R3]),
                                    [receive_rsp(Conn, R) || R <- [R1, R2]]
                                end),
        %% The connection gives a part a new ref, thus the requests are found by their msg.
        ?assertEqual([R1#kpro_req.msg, R3#kpro_req.msg, R2#kpro_req.msg],
                     [Msg || [_, _, #kpro_req{msg = Msg}] <- Sent])
    end).

max_queue_bytes_test() ->
  with_connection(
    #{merge_produce => true, merge_produce_max_queue_bytes => 1000},
    fun(Conn, Vsn, [A, B | _]) ->
        Big = binary:copy(<<"x">>, 1000),
        %% The second round fails if the queue keeps the bytes of the parts that went.
        lists:foreach(
          fun(_Round) ->
              [R1, R2, R3] = [req(Vsn, A), req(Vsn, B), req(Vsn, A, #{value => Big})],
              {Tag2, Tag3} = {make_ref(), make_ref()},
              ok = sys:suspend(Conn),
              Conn ! {{self(), noreply}, {send, R1}},
              Conn ! {{self(), Tag2}, {send, R2}},
              Conn ! {{self(), Tag3}, {send, R3}},
              ok = sys:resume(Conn),
              %% R2 is in the limit, thus it gets ok at once. R3 is not in the limit,
              %% thus it gets ok when it goes to the socket after the response of R1.
              ?assertEqual([ Tag2, R1#kpro_req.ref
                           , Tag3, R2#kpro_req.ref, R3#kpro_req.ref
                           ],
                           receive_in_order(Conn, 5))
          end, [first, second])
    end).

%%%_* Help functions ===========================================================

with_connection(Config, Fun) ->
  %% Fun gets the partitions of one leader that has two or more partitions,
  %% so that their requests go on the same connection.
  [First | _] = Partitions = same_leader_partitions(),
  ConnFun = fun(Es, Cfg) -> kpro:connect_partition_leader(Es, Cfg, topic(), First) end,
  kpro_test_lib:with_connection(
    kpro_test_lib:connection_config(Config), ConnFun,
    fun(Conn) ->
        {ok, Vsns} = kpro:get_api_versions(Conn),
        {_, Vsn} = maps:get(produce, Vsns),
        Fun(Conn, Vsn, Partitions)
    end).

same_leader_partitions() ->
  Leaders =
    kpro_test_lib:with_connection(
      fun(Conn) ->
          lists:map(
            fun(P) ->
                {ok, Leader} = kpro:discover_partition_leader(Conn, topic(), P, ?TIMEOUT),
                {Leader, P}
            end, lists:seq(0, ?PARTITIONS - 1))
      end),
  Groups = lists:foldl(fun({Leader, P}, Acc) ->
                           maps:update_with(Leader, fun(Ps) -> Ps ++ [P] end, [P], Acc)
                       end, #{}, Leaders),
  case [Ps || Ps <- maps:values(Groups), length(Ps) >= 2] of
    [Partitions | _] -> Partitions;
    [] -> erlang:error({no_leader_with_two_partitions, topic(), Leaders})
  end.

send_held(Conn, Reqs) ->
  {Rsps, Calls} = trace_calls(Conn, {kpro_req_lib, encode, 3},
                              fun() ->
                                  ok = send_suspended(Conn, Reqs),
                                  [receive_rsp(Conn, Req) || Req <- Reqs]
                              end),
  {Rsps, length(Calls)}.

trace_calls(Pid, {M, F, _} = MFA, Fun) ->
  %% Hand-rolled mocking, like meck:history/1
  1 = erlang:trace_pattern(MFA, true, []),
  1 = erlang:trace(Pid, true, [call]),
  Result = Fun(),
  1 = erlang:trace(Pid, false, [call]),
  1 = erlang:trace_pattern(MFA, false, []),
  TraceRef = erlang:trace_delivered(Pid),
  receive {trace_delivered, Pid, TraceRef} -> ok end,
  {Result, receive_calls(Pid, M, F, [])}.

send_suspended(Conn, Reqs) ->
  %% The connection gets all requests before it sends the first one, so all
  %% except the first one wait for a response.
  %% kpro:send/2 waits for a reply to a request with no_ack, thus send the message directly.
  ok = sys:suspend(Conn),
  lists:foreach(fun(Req) -> Conn ! {{self(), noreply}, {send, Req}} end, Reqs),
  sys:resume(Conn).

receive_rsp(Conn, #kpro_req{ref = Ref}) ->
  receive
    {msg, Conn, #kpro_rsp{ref = Ref} = Rsp} -> Rsp
  after
    ?TIMEOUT -> erlang:error({no_response, Ref})
  end.

%% Messages from one process come in the order that the process sent them.
receive_in_order(Conn, N) ->
  [receive
     {msg, Conn, #kpro_rsp{ref = Ref}} -> Ref;
     {Tag, ok} when is_reference(Tag) -> Tag
   after
     ?TIMEOUT -> erlang:error(no_message)
   end || _ <- lists:seq(1, N)].

receive_calls(Pid, M, F, Acc) ->
  receive
    {trace, Pid, call, {M, F, Args}} -> receive_calls(Pid, M, F, [Args | Acc])
  after
    0 -> lists:reverse(Acc)
  end.

results(Rsps) ->
  lists:map(
    fun(Rsp) ->
        #{partition := P, error_code := EC, base_offset := O} = kpro_test_lib:parse_rsp(Rsp),
        {P, EC, O}
    end, Rsps).

req(Vsn, Partition) ->
  req(Vsn, Partition, #{}).

%% kpro_req_lib:produce/5 ignores the `value' key in Opts.
req(Vsn, Partition, Opts) ->
  Batch = [#{value => maps:get(value, Opts, value())}],
  kpro_req_lib:produce(Vsn, topic(), Partition, Batch, Opts).

make_produce_req(Vsn, Partitions) ->
  Magic = kpro_lib:produce_api_vsn_to_magic_vsn(Vsn),
  Batch = kpro:encode_batch(Magic, [#{value => value()}], ?no_compression),
  TopicData = [ {topic, topic()}
              , {data, [[{partition, P}, {record_set, Batch}] || P <- Partitions]}
              ],
  Fields = [{transactional_id, ?kpro_null} || Vsn > 2] ++
           [ {acks, -1}
           , {timeout, ?TIMEOUT}
           , {topic_data, [TopicData]}
           ],
  kpro:make_request(produce, Vsn, Fields).

topic() -> kpro_test_lib:get_topic_merge().

value() ->
  term_to_binary({?MODULE, os:system_time()}).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
