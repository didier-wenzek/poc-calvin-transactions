open Shop

let query_partition_keys = function
  | GetCustomer customer_id -> [ customer_id ]
  | GetCategory category_id -> [ category_id ]
  | GetProduct product_id -> [ product_id ]
  | GetOrders customer_id-> [ customer_id ]
  | GetOrder order_id -> [ order_id ]
  | GetStock product_ids -> product_ids

let source_key (type a) (type b): a -> (a,b) relation -> int =
  fun a rel -> match rel with
  | CustomerName -> a
  | CategoryName -> a
  | ProductName -> a
  | ProductCategory -> a
  | ProductCategoryName -> a
  | ProductQuantity -> a
  | OrderCustomer -> a
  | OrderProducts -> a

let query_partition_key = function
  | GetTarget (a,rel) -> source_key a rel
  | GetSource (rel,b) -> 0 (* FIXME *)

let rec check_partition_key = function
  | ExistRelation (a,rel,b) -> source_key a rel
  | ExistSource (a,rel) -> source_key a rel
  | ExistTarget (rel,b) -> 0 (* FIXME *)
  | Not rel -> check_partition_key rel
  | TargetGreaterThan (a,rel,b) -> source_key a rel

let update_partition_key = function
  | AddRelation (a,rel,b) -> source_key a rel
  | RemRelation (a,rel,b) -> source_key a rel

let transaction_partition_key = Int64.to_int

let partition_key_of_storage_request =  function
  | Check (transaction_id, sequence_id, relation_check) -> check_partition_key relation_check
  | Write (transaction_id, relation_operation) ->  update_partition_key relation_operation
  | Read (transaction_id, query) -> query_partition_key query 
  | _ -> 0
(*
  | Commit (transaction_id) -> 0 (* FIXME ! Should be sent to all the partition implied by the query *)
  | Cancel (transaction_id) -> 0 (* FIMXE ! idem *)
*)

let operation_partition_keys op =
  let checklist = operation_checklist op |> List.map check_partition_key in
  let updates = operation_outcomes op |> List.map update_partition_key in
  List.rev_append checklist updates |>  List.sort_uniq (-)

let response_partition_key = function
  | PartialRead (tnx_id, _)
  | PartialCheck (tnx_id, _, _) -> transaction_partition_key tnx_id
