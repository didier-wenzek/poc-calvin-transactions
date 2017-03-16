type name = string

let pp_name = Show.pp_str

type t =
  | Str of string
  | Num of string
  | Seq of t list
  | Tup of t list
  | Rec of tagged list
  | Tag of tagged
and tagged = name * t
