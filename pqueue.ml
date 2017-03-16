type 'a t = {
  head : 'a list;
  rev_tail : 'a list;
}

let empty = {
  head = [];
  rev_tail = [];
}

let push x xs = {
  xs with
  rev_tail = x::xs.rev_tail
}

let list_is_empty = function
  | [] -> true
  | _ -> false

let is_empty xs = list_is_empty xs.head && list_is_empty xs.rev_tail 

let rev_and_pop xs = match List.rev xs.rev_tail with
  | x::head -> (Some x, { head ; rev_tail = [] })
  | [] -> (None, empty)

let pop xs = match xs.head with
  | x::head -> (Some x, { xs with head })
  | [] -> rev_and_pop xs
