let get_partition_count kafka topic =
  let open Kafka.Metadata in
  let timeout_ms = 3000 in
  let tmd = Kafka.topic_metadata ~timeout_ms kafka topic in
  List.length tmd.topic_partitions
  
let check_topic kafka topic expected_partition_count =
  let partition_count = get_partition_count kafka topic in
  if partition_count <> expected_partition_count
  then raise (Invalid_argument (Printf.sprintf "The topic '%s' must have %d partitions (found %d)" (Kafka.topic_name topic) expected_partition_count partition_count))
  else expected_partition_count

let topic_consumer consumer topic_name client_name client_id client_count =
  let offset_store_path = Printf.sprintf ".%s-%s-%d-%d.offset" topic_name client_name client_id client_count in
  Kafka.new_topic consumer topic_name [
    "offset.store.method","file";
    "offset.store.path",offset_store_path;
    "auto.commit.enable","true";
  ]

let topic_producer producer topic_name =
  let topic = Kafka.new_topic producer topic_name [
    "request.required.acks", "0";
  ] in
  let partition_count = get_partition_count producer topic in
  (topic, partition_count)

let produce topic msg partition_id =
  Lwt.async (fun () -> Kafka_lwt.produce topic partition_id msg);
  Lwt.return_unit

let send (topic, partition_count) partition_key msg =
  let partition_id = partition_key mod partition_count in
  produce topic msg partition_id

let dispatch (topic, partition_count) partition_keys =
  let partition_keys =
    partition_keys |> List.map (fun key -> key mod partition_count) |> List.sort_uniq (-)
  in
  fun msg -> Lwt_list.iter_p (produce topic msg) partition_keys

let ints =
  let rec loop r n =
    if n = 0 then 0::r
    else loop (n::r) (n-1)
  in loop []

let broadcast (topic, partition_count) =
  let partition_ids = ints (partition_count - 1) in
  let send msg partition_id = Kafka_lwt.produce topic partition_id msg in
  fun msg -> Lwt_list.iter_p (send msg) partition_ids
