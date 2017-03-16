open Lwt

module VersionMap = Map.Make(Int64)

type version_t = Int64.t

type txn_status = Running | Committed | Canceled

type txn_state = {
  status: txn_status;
  pending_subcommands: int;
}

type state = {
  current_db: ShopDB.state;
  current_version: version_t;
  db_versions: Shop.relation_operation list VersionMap.t;
  txn_states: txn_state VersionMap.t;
  reservations: Reservation.t;
  forward_response: Shop.storage_response -> unit Lwt.t;
}

let empty_version = -1L (* So the transaction N produces the version N of the database *)

let empty forward_response = {
  current_db = ShopDB.empty;
  current_version = empty_version;
  db_versions = VersionMap.empty;
  txn_states = VersionMap.empty;
  reservations = Reservation.empty;
  forward_response = forward_response;
}

let new_txn_state = {
  status = Running;
  pending_subcommands = 0;
}

let version_of_txn state txn =
  try VersionMap.find txn state.db_versions
  with Not_found ->  []

let state_of_txn txn_states txn =
  try VersionMap.find txn txn_states
  with Not_found -> new_txn_state

let start_version state txn =
  try
    let version = VersionMap.find txn state.db_versions in
    (version, state)
  with Not_found ->
    let new_version = [] in
    let new_state = { state with
      db_versions = VersionMap.add txn new_version state.db_versions
    } in
    (new_version, new_state)

let prepare_subcommand_release txn_state = function
  | Shop.Check (_, _, _)
  | Shop.Write (_, _) -> { txn_state with pending_subcommands = txn_state.pending_subcommands + 1 }
  | Shop.Commit _ -> { txn_state with status = Committed }
  | Shop.Cancel _ -> { txn_state with status = Canceled }
  | Shop.Read _ -> txn_state
  
let release_subcommand txn_state = function
  | Shop.Check (_, _, _)
  | Shop.Write (_, _) -> { txn_state with pending_subcommands = txn_state.pending_subcommands - 1 }
  | Shop.Commit _
  | Shop.Cancel _
  | Shop.Read _ -> txn_state

let prepare_subcommand_tnx txn_states command = match command with
  | Shop.Check (txn, _, _)
  | Shop.Write (txn, _)
  | Shop.Commit txn
  | Shop.Cancel txn -> (
    let txn_state = state_of_txn txn_states txn in
    VersionMap.add txn (prepare_subcommand_release txn_state command) txn_states
  )
  | Shop.Read _ ->
    txn_states

let release_subcommand_txn txn_states command = match command with
  | Shop.Check (txn, _, _)
  | Shop.Write (txn, _) -> (
    try
      let txn_state = VersionMap.find txn txn_states in
      VersionMap.add txn (release_subcommand txn_state command) txn_states
    with Not_found -> txn_states
  )
  | Shop.Read _
  | Shop.Commit  _
  | Shop.Cancel _ ->
    txn_states

let release_txn_states commands txn_states =
  List.fold_left release_subcommand_txn txn_states commands

let update_version state txn version =
  {
    state with
    db_versions = VersionMap.add txn version state.db_versions
  }

let rec list_fold add acc = function
  | [] -> Lwt.return acc
  | x::xs -> (
    add acc x >>= fun acc_x -> list_fold add acc_x xs
  )

let rec handle_request state = function
  | Shop.Read (txn, query) -> (
    let db = state.current_db in
    let responses = ShopDB.get_atomic_response db query in
    let responses = List.map (fun r -> Shop.PartialRead (txn,r)) responses in
    Lwt_list.iter_p state.forward_response responses
    >>= fun () ->
    return state
  )
  | Shop.Check (txn, seq, test) -> (
    let txn_state = state_of_txn state.txn_states txn in
    if txn_state.status = Running
    then
      let db = state.current_db in
      let result = ShopDB.check db test in
      state.forward_response (Shop.PartialCheck (txn, seq, result))
      >>= fun () ->
      return state
    else
      close_if_done state txn txn_state
  )
  | Shop.Write (txn, update) -> (
    let txn_state = state_of_txn state.txn_states txn in
    if txn_state.status = Canceled
    then
      close_if_done state txn txn_state
    else
      let db, state = start_version state txn in
      let new_version = update::db in
      let new_state = update_version state txn new_version in
      if txn_state.status = Running
      then return new_state
      else close_if_done new_state txn txn_state
  )
  | Shop.Commit txn -> (
    do_commit state txn
  )
  | Shop.Cancel txn -> (
    do_cancel state txn 
  )

and release_reservations state txn =
  let updated_reservations, released_reads, released_writes = Reservation.release_reservations state.reservations txn in
  let new_txn_states = state.txn_states |> release_txn_states released_writes |> release_txn_states released_reads in
  let new_state = { state with
    reservations = updated_reservations;
    txn_states = new_txn_states;
  } in
  let handle_command state request =
    Lwt_io.printf "RELEASED %s\n%!" (Shop.show_storage_request request)
    >>= fun () -> 
    handle_request state request
  in
  let handle_request state request =
    handle_command state request
    >>= fun _ ->
    return_unit
  in
  Lwt_list.iter_p (handle_request new_state) released_reads
  >>= fun () ->
  list_fold handle_command new_state released_writes

and do_commit state txn = 
  let updates = version_of_txn state txn in
  let new_db = List.fold_left ShopDB.apply state.current_db updates in
  let new_state = { state with
    current_version = txn;
    current_db = new_db;
    db_versions = VersionMap.remove txn state.db_versions;
    txn_states = VersionMap.remove txn state.txn_states;
  } in
  release_reservations new_state txn

and do_cancel state txn =
  let new_state = { state with
    db_versions = VersionMap.remove txn state.db_versions;
    txn_states = VersionMap.remove txn state.txn_states;
  } in
  release_reservations new_state txn

and close_if_done state txn txn_state =
  if txn_state.pending_subcommands = 0
  && txn_state.status <> Running
  then
    Lwt_io.printf "COMPLETE %Ld\n%!" txn
    >>= fun () ->
    match txn_state.status with
      | Committed -> do_commit state txn
      | Canceled -> do_cancel state txn
      | _ -> assert false
  else
    return state

let delay state txn txn_state command =
  let delayed_command = prepare_subcommand_release txn_state command in
  Lwt_io.printf "DELAYED %s (waiting %d commands)\n%!" (Shop.show_storage_request command) txn_state.pending_subcommands
  >>= fun () ->
  return { state with txn_states = VersionMap.add txn delayed_command state.txn_states }

let forward_response state query =
  match Reservation.add_read_reservation state.reservations query with
  | Reservation.Free, _ -> (
    handle_request state query
  )
  | Reservation.Reserved (running_txn, last_txn), updated_reservations -> (
    Lwt_io.printf "BLOCKED by %Ld-%Ld %s\n%!" running_txn last_txn (Shop.show_storage_request query)
    >>= fun () ->
    return { state with
      reservations = updated_reservations;
      txn_states = prepare_subcommand_tnx state.txn_states query;
    }
  )

let apply_command state command =
  match Reservation.add_write_reservation state.reservations command with
  | Reservation.Free, updated_reservations -> (
    let updated_state = { state with reservations = updated_reservations } in
    handle_request updated_state command
  )
  | Reservation.Reserved (running_txn, last_txn), updated_reservations -> (
    Lwt_io.printf "BLOCKED by %Ld-%Ld %s\n%!" running_txn last_txn (Shop.show_storage_request command)
    >>= fun () ->
    return { state with
      reservations = updated_reservations;
      txn_states = prepare_subcommand_tnx state.txn_states command;
    }
  )

let commit state txn =
  let txn_state = state_of_txn state.txn_states txn in
  if txn_state.pending_subcommands = 0
  then do_commit state txn
  else delay state txn txn_state (Shop.Commit txn)

let cancel state txn =
  let txn_state = state_of_txn state.txn_states txn in
  if txn_state.pending_subcommands = 0
  then do_cancel state txn
  else delay state txn txn_state (Shop.Cancel txn)
