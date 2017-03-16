open Shop
open ShopDecoder
open Parser

let process_transaction state tnx request =
  let response, new_state = ShopDB.apply_request state tnx request in
    print_endline (show_outcome response);
    new_state
    
let rec loop tnx state = 
  try 
    let line = String.trim (read_line ()) in
    if line = ""
    then loop tnx state
    else begin
      let request = match decode_request (parse_data line) with
        | Ok request -> request
        | Error err -> IllTyped (err,line)
        | exception Parser.SyntaxError (_,_,_) -> IllFormated(line)
      in
      let new_state = process_transaction state tnx request in
      loop (Int64.succ tnx) new_state
    end
  with End_of_file -> ()

let _ = loop 0L ShopDB.empty
