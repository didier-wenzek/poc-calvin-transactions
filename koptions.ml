open Cmdliner

let manager_id =
  let doc = "Manager identifier" in
  Arg.(required & pos 0 (some int) None & info [] ~docv:"MANAGER_ID" ~doc)

let manager_count =
  let doc = "Count of managers" in
  Arg.(required & pos 1 (some int) None & info [] ~docv:"MANAGER_COUNT" ~doc)

let partition_id =
  let doc = "Database partition identifier" in
  Arg.(required & pos 0 (some int) None & info [] ~docv:"PARTITION_ID" ~doc)

let partition_count =
  let doc = "Count of database partition" in
  Arg.(required & pos 1 (some int) None & info [] ~docv:"PARTITION_COUNT" ~doc)

let kafka_cluster =
  let doc = "Connection string to the Kafka cluster." in
  Arg.(value & opt string "localhost" & info ["k"; "kafka"] ~docv:"KAFKA_CLUSTER" ~doc)

let transac_topic =
  let doc = "Kafka topic from where the transactions are consumed." in
  Arg.(value & opt string "db_transaction_request" & info ["t"; "transac_topic"] ~docv:"TRANSAC_TOPIC" ~doc)

let outcome_topic =
  let doc = "Kafka topic where the transaction outcomes are published." in
  Arg.(value & opt string "db_transaction_outcome" & info ["o"; "outcome_topic"] ~docv:"OUTCOME_TOPIC" ~doc)

let request_topic =
  let doc = "Kafka topic where the partition requests are published." in
  Arg.(value & opt string "db_partition_request" & info ["r"; "request_topic"] ~docv:"REQUEST_TOPIC" ~doc)

let criteria_topic =
  let doc = "Kafka topic where the transaction criteria are published." in
  Arg.(value & opt string "db_transaction_criteria" & info ["c"; "criteria_topic"] ~docv:"CRITERIA_TOPIC" ~doc)

let fatal_error msg =
   prerr_string "ERROR: ";
   prerr_endline msg;
   exit 2

let consumer_options brokers = [
  "metadata.broker.list", brokers;
  "fetch.wait.max.ms", "10";
]

let producer_options brokers = [
  "metadata.broker.list", brokers;
  "queue.buffering.max.ms", "5";
]
