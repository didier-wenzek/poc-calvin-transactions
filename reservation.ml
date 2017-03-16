open Shop

module ReservationComparable = struct
  type t = Shop.reservation_key
  let compare = Pervasives.compare
end

module ReservationSet = Set.Make(ReservationComparable)
module ReservationMap = Map.Make(ReservationComparable)
module TransactionMap = Map.Make(Int64)

type t = {
  running_transactions: ReservationSet.t TransactionMap.t;
  pending_requests: (transaction_id * transaction_id * storage_request Pqueue.t) ReservationMap.t
}

let empty = {
  running_transactions = TransactionMap.empty;
  pending_requests = ReservationMap.empty;
}

type status = Free | Reserved of transaction_id * transaction_id 

let request_transaction_id = function
  | Read (txn, _) -> txn
  | Check (txn, _, _) -> txn
  | Write (txn, _) -> txn
  | Commit txn -> txn
  | Cancel txn -> txn

let last_write_transaction_id last_txn = function
  | Write (new_txn, _) -> new_txn
  | _ -> last_txn

let read_reservation = function
  | Read (_, query) -> read_reservation query
  | Check (_, _, check) -> check_reservation check
  | _ -> raise Not_found

let write_reservation = function
  | Write (_, op) -> write_reservation op
  | _ -> raise Not_found

let add_pending_request reservations entry txn pending request =
  let last_txn = last_write_transaction_id txn request in
  let pending = Pqueue.push request pending in
  {
    reservations with
    pending_requests = ReservationMap.add entry (txn,last_txn,pending) reservations.pending_requests
  } 

let reservations_of_transaction running_transactions txn =
  try TransactionMap.find txn running_transactions
  with Not_found -> ReservationSet.empty

let update_running_transaction running_transactions txn entry =
  let tnx_reservations = reservations_of_transaction running_transactions txn in
  let updated_tnx_reservations = ReservationSet.add entry tnx_reservations in
  TransactionMap.add txn updated_tnx_reservations running_transactions

let add_running_transaction reservations txn entry =
  {
    running_transactions = update_running_transaction reservations.running_transactions txn entry;
    pending_requests = ReservationMap.add entry (txn, txn, Pqueue.empty) reservations.pending_requests;
  }

let add_read_reservation reservations request =
  try
    let entry = read_reservation request in
    let txn, last_txn, pending = ReservationMap.find entry reservations.pending_requests in
    if txn = request_transaction_id request
    then (Free, reservations)
    else (Reserved (txn,last_txn), add_pending_request reservations entry txn pending request)
  with
    Not_found -> (Free, reservations)

let add_write_reservation reservations command = 
  let command_txn = request_transaction_id command in
  let entry = write_reservation command in
  try
    let running_txn, last_txn, pending = ReservationMap.find entry reservations.pending_requests in
    if running_txn = command_txn
    then (Free, reservations)
    else (Reserved (running_txn,last_txn), add_pending_request reservations entry running_txn pending command)
  with
    Not_found -> (Free, add_running_transaction reservations command_txn entry)

let release_requests reservations released_reservations =
  let rec release released_reads released_writes (released_txn, last_txn, queue) = 
    match Pqueue.pop queue with
    | None, empty_queue -> (released_reads, released_writes, None)
    | Some ((Write (new_tnx, _)) as write), queue -> (released_reads, write::released_writes, Some (new_tnx,last_txn,queue))
    | Some read, queue -> release (read::released_reads) released_writes (released_txn,last_txn,queue)
  in
  let release_reservation entry (released_reads, released_writes, pending_requests, running_transactions) =
    try
      let entry_requests = ReservationMap.find entry pending_requests in
      match release released_reads released_writes entry_requests with 
      | (released_reads, released_writes, None) ->
        let updated_pending_requests = ReservationMap.remove entry pending_requests in
        (released_reads, released_writes, updated_pending_requests, running_transactions)
      | (released_reads, released_writes, Some entry_requests) ->
        let (txn,_,_) = entry_requests in
        let updated_pending_requests = ReservationMap.add entry entry_requests pending_requests in
        let updated_running_transactions = update_running_transaction running_transactions txn entry in
        (released_reads, released_writes, updated_pending_requests, updated_running_transactions)
    with Not_found ->
        (released_reads, released_writes, pending_requests, running_transactions)
  in
  ReservationSet.fold release_reservation released_reservations ([], [], reservations.pending_requests, reservations.running_transactions) 

let release_reservations reservations txn =
  try
    let released_reservations = TransactionMap.find txn reservations.running_transactions in
    let released_reads, released_writes, still_pending_requests, new_running_transactions = release_requests reservations released_reservations in
    let updated_reservations = {
      running_transactions = TransactionMap.remove txn new_running_transactions;
      pending_requests = still_pending_requests;
    } in            (updated_reservations, List.rev released_reads, List.rev released_writes)
  with Not_found -> (reservations, [], [])
