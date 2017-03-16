type 'a pprinter = Format.formatter -> 'a -> unit

let print (type a): a pprinter -> a -> unit =
  fun pprinter_a -> pprinter_a Format.std_formatter

let show (type a): a pprinter -> a -> string =
  fun pprinter_a a ->
    let buffer = Buffer.create 80 in
    let formatter = Format.formatter_of_buffer buffer in
    let () = pprinter_a formatter a in
    let () = Format.pp_print_flush formatter () in
    Buffer.contents buffer

let pp_int = Format.pp_print_int
let pp_str = Format.pp_print_string
let pp_bool formatter b =
  if b then Format.pp_print_string formatter "True"
  else      Format.pp_print_string formatter "False"

let pp_option (type a): 'a pprinter -> 'a option pprinter =
  fun pp_a formatter oa -> match oa with
    | Some a -> pp_a formatter a
    | None -> Format.pp_print_string formatter "_"

let pp_tag tag (type a): 'a pprinter -> 'a pprinter =
  fun pp_a formatter a -> (
    Format.pp_print_string formatter tag;
    Format.pp_print_string formatter "(";
    pp_a formatter a;
    Format.pp_print_string formatter ")"
  )

let pp_pair tag (type a) (type b): a pprinter -> b pprinter -> (a*b) pprinter =
  fun pp_a pp_b formatter (a,b) ->
    let p = Format.pp_print_string formatter in
    (
      p tag; p "("; pp_a formatter a; p ","; pp_b formatter b ; p ")"
    )

let pp_tup3 tag (type a) (type b) (type c): a pprinter -> b pprinter -> c pprinter -> (a*b*c) pprinter =
  fun pp_a pp_b pp_c formatter (a,b,c) ->
    let p = Format.pp_print_string formatter in
    (
      p tag; p "("; pp_a formatter a; p ","; pp_b formatter b ; p ","; pp_c formatter c ; p ")"
    )

let pp_list (type a): a pprinter -> a list pprinter =
  fun pp_a formatter ->
    let p = Format.pp_print_string formatter in
    let p_a = pp_a formatter in
    let p_items = function
      | [] -> ()
      | [a] -> p_a a
      | a::others -> (
        pp_a formatter a; List.iter (fun a -> p ","; p_a a) others
      )
    in
    fun items ->
    (
      p "["; p_items items ; p "]"
    )
    
