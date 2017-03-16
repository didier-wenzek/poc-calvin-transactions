open Cmdliner
open Koptions
open Kcluster
open Lwt
open Shop
module Cluster = Kcluster
module Partition = ShopPartition

let info =                                                                                                                                                                                                 
  let doc = "Server a DataMint partition over Kafka." in
  let man = [
    `S "DESCRIPTION";
    `P "$(tname) PARTITION_ID PARTITION_COUNT";

    `P "Consume requests sent by the tranasaction managers to the kafka REQUEST_TOPIC on the partition PARTITION_ID.";

    `P "PARTITION_COUNT servers are expected to be launched on the cluster,
        taking for PARTITION_ID all the value in the range [1, ..., PARTITION_COUNT]";

    `P "Publish transaction partial critera the kafka CRITERIA_TOPIC.
        These criteria are aggregated by the transaction managers which broadcast then the global decision over the kafka REQUEST_TOPIC.";
  ] in
  Term.info "partition_server" ~doc ~man

let handle_request state request = match request with
  | Read _ | Check _ -> (
    ShopMVCC.forward_response state request
  )
  | Write _ -> (
    ShopMVCC.apply_command state request
  )
  | Commit tnx_id -> (
    ShopMVCC.commit state tnx_id
  )
  | Cancel tnx_id -> (
    ShopMVCC.cancel state tnx_id
  )

let handle_request_msg state = function
  | Kafka.Message (_,_,_,msg,_) -> begin
    match ShopDecoder.decode_storage_request (Parser.parse_data msg) with
    | Result.Ok request -> (
      Lwt_io.printf "%s\n%!" (show_storage_request request)
      >>= fun () ->
      handle_request state request
    )
    | Result.Error err -> (
      Lwt_io.printf "TYPE ERROR: %s: %s\n%!" err msg
      >>= fun () ->
      return state
    )
    | exception Parser.SyntaxError (_,_,_) ->  (
      Lwt_io.printf "FORMAT ERROR: %s\n%!" msg
      >>= fun () ->
      return state
    )
  end
  | _ -> ( (* pause in the stream *)
      return state
     )

let lwt_main kafka_cluster request_topic response_topic partition_id partition_count =
  let kafka_options = ["metadata.broker.list",kafka_cluster] in
  let consumer = Kafka.new_consumer kafka_options in
  let producer = Kafka_lwt.new_producer kafka_options in
  let request_topic = topic_consumer consumer request_topic "partition_server" partition_id partition_count in
  let _ = check_topic consumer request_topic partition_count in 
  let request_partition = partition_id mod partition_count in
  let response_topic = Cluster.topic_producer producer response_topic in
  let forward_response response =
    let partition_key = Partition.response_partition_key response in
    let msg = show_storage_response response in
    Cluster.send response_topic partition_key msg
  in
  let initial_state = ShopMVCC.empty forward_response in
  let start () =
    Kafka.consume_start request_topic request_partition Kafka.offset_stored;
    Lwt_io.printl "Started - waiting for requests."
    >>= fun () ->
    return initial_state
  in
  let rec loop state = 
    let timeout_ms = 1 in
    Kafka_lwt.consume_batch ~timeout_ms request_topic request_partition
    >>=
    ShopMVCC.list_fold handle_request_msg state
    >>=
    loop
  in
  let term final_state =
    Kafka_lwt.wait_delivery producer
    >>= fun () ->
    Kafka.consume_stop request_topic request_partition;
    Kafka.destroy_topic request_topic;
    Kafka.destroy_topic (fst response_topic);
    Kafka.destroy_handler consumer;
    Kafka.destroy_handler producer;
    return ()
  in
  Lwt_main.run (start () >>= loop >>= term)

let tnx_manager_t = Term.(pure lwt_main $ kafka_cluster $ request_topic $ criteria_topic $ partition_id $ partition_count)

let main () =
  match Term.eval (tnx_manager_t, info) with
  | `Error _ -> exit 1
  | _ -> exit 0

let () =
  Lwt_engine.set (new Lwt_engine.Versioned.libev_2 ());
  Lwt_unix.with_async_detach main
