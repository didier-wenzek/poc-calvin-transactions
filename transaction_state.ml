open Shop

module TnxMap = Map.Make(Int64)

type operation_state = {
  pending_status_count: int;
  current_status: bool;
  operation: operation;
}

type query_state = {
  pending_response_count: int;
  current_response: relation_value list;
  query: query;
}

type tnx_state =
  | PendingOperation of operation_state
  | PendingQuery of query_state

type state = tnx_state TnxMap.t

let empty = TnxMap.empty

let add_query state txn_id query subqueries =
  let query_state = PendingQuery {
    pending_response_count = List.length subqueries;
    current_response = [];
    query = query;
  } in
  TnxMap.add txn_id query_state state

let is_check = function
  | Check _ -> true
  | _ -> false

let count_check requests = List.length (List.filter is_check requests)

let add_operation state txn_id requests op =
  let operation_state = PendingOperation {
    pending_status_count = count_check requests;
    current_status = true;
    operation = op;
  } in
  TnxMap.add txn_id operation_state state
  
let add_partial_read state transaction_id response = 
  match TnxMap.find transaction_id state with
  | PendingQuery query_state -> (
    let current_response = response :: query_state.current_response in
    let pending_count = query_state.pending_response_count - 1 in
    if pending_count = 0
    then
       let state = TnxMap.remove transaction_id state in
       (state, Some (Response (transaction_id, make_response query_state.query current_response)))
    else
      let query_state = PendingQuery {
        query_state with
        pending_response_count = pending_count;
        current_response = current_response;
      } in
      let state = TnxMap.add transaction_id query_state state in
      (state, None)
  )
  | _ -> (state, None)
  | exception Not_found -> (state, None)

let add_partial_check state txn_id status =
  match TnxMap.find txn_id state with
  | PendingOperation operation_state -> (
    let pending_count = operation_state.pending_status_count - 1 in
    let status = operation_state.current_status && status in
    if pending_count = 0 || (not status)
    then
      let state = TnxMap.remove txn_id state in
      (state, Some (status, operation_state.operation))
    else
      let operation_state = PendingOperation {
        operation_state with
        pending_status_count = pending_count;
      } in
      let state = TnxMap.add txn_id operation_state state in
      (state, None)
  )
  | _ -> (state, None)
  | exception Not_found -> (state, None)


