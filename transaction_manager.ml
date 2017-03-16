open Cmdliner
open Koptions
open Kcluster
open Lwt
open Shop
module Cluster = Kcluster
module Partition = ShopPartition

let info =                                                                                                                                                                                                 
  let doc = "Manage DataMint transactions published on Kafka." in
  let man = [
    `S "DESCRIPTION";
    `P "$(tname) MANAGER_ID MANAGER_COUNT";

    `P "Consume transactions from the kafka TRANSAC_TOPIC.
        This topic must have a single partition.";

    `P "Process only transactions with an offsets equals to MANAGER_ID modulo MANAGER_COUNT.
        Hence, MANAGER_COUNT managers are expected to be launched on the cluster,
        taking for MANAGER_ID all the value in the range [1, ..., MANAGER_COUNT]";

    `P "Publish partition requests on the kafka REQUEST_TOPIC.
        These requests are consumed and processed by N partition servers.";

    `P "Consume the responses of the partition servers from the kafka CRITERIA_TOPIC.
        This topic must have MANAGER_COUNT partitions.";

    `P "Publish transaction outcomes on the kafka OUTCOME_TOPIC.";
  ] in
  Term.info "transaction_manager" ~doc ~man

let handle_transaction request_topic outcome_topic state txn_id = function
  | Query q ->
    let requests = query_storage_requests txn_id q in
    let state = Transaction_state.add_query state txn_id q requests in
    Lwt_list.iter_p (fun request -> 
      let partition_key = Partition.partition_key_of_storage_request request in
      let msg = show_storage_request request in
      Cluster.send request_topic partition_key msg
    ) requests
    >>= fun () ->
    return state
  | Operation op -> (
    let requests = operation_storage_requests txn_id op in
    let state = Transaction_state.add_operation state txn_id requests op in
    Lwt_list.iter_p (fun request -> 
      let partition_key = Partition.partition_key_of_storage_request request in
      let msg = show_storage_request request in
      Cluster.send request_topic partition_key msg
    ) requests
    >>= fun () ->
    return state
  )
  | garbage ->
    let partition_key = Partition.transaction_partition_key txn_id in
    let response = Ignored (txn_id, garbage) in
    let msg =show_outcome response in
    Cluster.send outcome_topic partition_key msg
    >>= fun () ->
    return state

let handle_storage_response request_topic outcome_topic state = function
  | PartialRead (txn_id, res) -> (
    let state,outcome = Transaction_state.add_partial_read state txn_id res in
    match outcome with
    | None -> return state
    | Some response -> 
      let partition_key = Partition.transaction_partition_key txn_id in
      let msg =show_outcome response in
      Cluster.send outcome_topic partition_key msg
      >>= fun () ->
      return state
  )
  | PartialCheck (txn_id, seq_id, res) -> (
    let state,outcome = Transaction_state.add_partial_check state txn_id res in
    match outcome with
    | None -> return state
    | Some (status, operation) -> 
      let commit, outcome =
        if status
        then (Commit txn_id, Accepted (txn_id, operation))
        else (Cancel txn_id, Rejected (txn_id, operation, ""))
      in
      let outcome_partition_key = Partition.transaction_partition_key txn_id in
      let commit_partition_keys = Partition.operation_partition_keys operation in
      Lwt_io.printf "DISPATCH %s\n%!" (show_storage_request commit)
      >>= fun () ->
      Cluster.dispatch request_topic commit_partition_keys (show_storage_request commit)
      >>= fun () ->
      Cluster.send outcome_topic outcome_partition_key (show_outcome outcome)
      >>= fun () ->
      return state
  )

let handle_transaction_msg is_responsible request_topic outcome_topic state =
  let handle_transaction state tnx request =
    Lwt_io.printf "%s\n%!" (show_request request)
    >>= fun () ->
    handle_transaction request_topic outcome_topic state tnx request
  in
  let handle_storage_response state response =
    Lwt_io.printf "%s\n%!" (show_storage_response response)
    >>= fun () ->
    handle_storage_response request_topic outcome_topic state response
  in
  function
  | Kafka.Message (_,_,offset,msg,_) when is_responsible offset -> (
    try
      let data = Parser.parse_data msg in
      DataDecoder.decode_cases [
        DataDecoder.Case (ShopDecoder.decode_request, handle_transaction state offset);
        DataDecoder.Case (ShopDecoder.decode_storage_response, handle_storage_response state);
      ] (fun () -> handle_transaction state offset (IllTyped ("Not a storage response",msg))) data
    with
      | Parser.SyntaxError (_,_,_) -> handle_transaction state offset (IllFormated msg)
  )
  | _ -> (
    return state
  )

let is_responsible manager_partition manager_count =
  let manager_offset = Int64.of_int manager_partition in
  let manager_count = Int64.of_int manager_count in
  function offset -> Int64.rem offset manager_count = manager_offset

let rec list_fold add acc = function
  | [] -> Lwt.return acc
  | x::xs -> (
    add acc x >>= fun acc_x -> list_fold add acc_x xs
  )

let lwt_main kafka_cluster transac_topic request_topic criteria_topic outcome_topic manager_id manager_count =
  let manager_partition = manager_id mod manager_count in
  let kafka_options = ["metadata.broker.list",kafka_cluster] in
  let consumer = Kafka.new_consumer kafka_options in
  let producer = Kafka_lwt.new_producer kafka_options in
  let transac_topic = topic_consumer consumer transac_topic "transaction_manager" manager_id manager_count in
  let _ = check_topic consumer transac_topic 1 in
  let criteria_topic = topic_consumer consumer criteria_topic "transaction_manager" manager_id manager_count in
  let _ = check_topic consumer criteria_topic manager_count in
  let request_topic = Cluster.topic_producer producer request_topic in
  let outcome_topic = Cluster.topic_producer producer outcome_topic in
  let queue = Kafka.new_queue consumer in
  let start () =
    Kafka.consume_start_queue queue transac_topic 0 Kafka.offset_stored;
    Kafka.consume_start_queue queue criteria_topic manager_partition Kafka.offset_stored; 
    Lwt_io.printl "Started - waiting for transactions."
    >>= fun () ->
    return Transaction_state.empty
  in
  let rec loop state = 
    let handle_transaction_msg = handle_transaction_msg (is_responsible manager_partition manager_count) request_topic outcome_topic in
    let timeout_ms = 1 in
    Kafka_lwt.consume_batch_queue ~timeout_ms queue
    >>=
    list_fold handle_transaction_msg state
    >>=
    loop
  in
  let term state =
    Kafka_lwt.wait_delivery producer
    >>= fun () ->
    Kafka.consume_stop transac_topic 0;
    Kafka.consume_stop criteria_topic manager_partition;
    Kafka.destroy_topic transac_topic;
    Kafka.destroy_topic (fst request_topic);
    Kafka.destroy_topic criteria_topic;
    Kafka.destroy_topic (fst outcome_topic);
    Kafka.destroy_queue queue;
    Kafka.destroy_handler consumer;
    Kafka.destroy_handler producer;
    return ()
  in
  Lwt_main.run (start () >>= loop >>= term)

let tnx_manager_t = Term.(pure lwt_main $ kafka_cluster $ transac_topic $ request_topic $ criteria_topic $ outcome_topic $ manager_id $ manager_count)

let main () =
  match Term.eval (tnx_manager_t, info) with
  | `Error _ -> exit 1
  | _ -> exit 0

let () =
  Lwt_engine.set (new Lwt_engine.Versioned.libev_2 ());
  Lwt_unix.with_async_detach main
