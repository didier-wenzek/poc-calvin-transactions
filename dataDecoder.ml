type 'a decoder = Data.t -> ('a,string) result

module Result = struct
  let (|>) r f = match r with
    | Ok x -> f x
    | Error r -> Error r

  let one_of ?(unknown_case="no matching options") fs x =
    let rec loop = function
      | [] -> Error unknown_case
      | f::fs -> (
        match f x with
        | Error _ -> loop fs
        | r -> r
      )
    in                                                                                                                                                                                                     
    loop fs

  let map_list f =
    let rec loop rs = function
      | [] -> Ok (List.rev rs)
      | x::xs -> (
        match f x with
        | Ok r -> loop (r::rs) xs
        | Error r -> Error r
      )
    in
    loop []
end

let decode decoder s = Parser.(
  try decoder (parse_data s)
  with SyntaxError (line, pos, token) -> Error (error_msg (line, pos, token))
)

let succeed r = function _ -> Ok r
let fail r = function _ -> Error r

let string = function
  | Data.Str s -> Ok s
  | _ -> Error "Not a string"

let int = function
  | Data.Num n -> (
    try Ok (int_of_string n)
    with Failure _ -> Error "Not an integer"
  )
  | _ -> Error "Not an integer"

let int64 = function
  | Data.Num n -> (
    try Ok (Int64.of_string n)
    with Failure _ -> Error "Not an integer"
  )
  | _ -> Error "Not an integer"

let unit = function
  | Data.Tup [] -> Ok ()
  | _ -> Error "Not a unit value"

let bool = function
  | Data.Tag ("True", Data.Tup []) -> Ok true
  | Data.Tag ("False", Data.Tup []) -> Ok false
  | _ -> Error "Not a bool"

let field name decoder = function
  | Data.Rec pairs -> (
    try List.assoc name pairs |> decoder
    with not_found -> Error ("Missing field "^name)
  )
  | data -> Error ("Not a record: ")

let record2 item1_decoder item2_decoder pack data =
  Result.(
    item1_decoder data
    |> fun item1 -> item2_decoder data
    |> fun item2 -> Ok (pack item1 item2)
  )

let record3 item1_decoder item2_decoder item3_decoder pack data =
  Result.(
    item1_decoder data
    |> fun item1 -> item2_decoder data
    |> fun item2 -> item3_decoder data
    |> fun item3 -> Ok (pack item1 item2 item3)
  )

let struct1 item1_decoder pack data = Result.(item1_decoder data |> (fun x -> Ok (pack x)))
  
let struct2 item1_decoder item2_decoder pack = function
  | Data.Tup [data1; data2] -> Result.(
    item1_decoder data1
    |> fun item1 -> item2_decoder data2
    |> fun item2 -> Ok (pack item1 item2)
  )
  | Data.Tup _ -> Error "Not a tuple of size 2"
  | data -> Error ("Not a tuple: ")

let struct3 item1_decoder item2_decoder item3_decoder pack = function
  | Data.Tup [data1; data2; data3] -> Result.(
    item1_decoder data1
    |> fun item1 -> item2_decoder data2
    |> fun item2 -> item3_decoder data3
    |> fun item3 -> Ok (pack item1 item2 item3)
  )
  | Data.Tup _ -> Error "Not a tuple of size 2"
  | data -> Error ("Not a tuple: ")

let pair item1 item2 = struct2 item1 item2 (fun x y -> (x,y))
let tuple3 item1 item2 item3 = struct3 item1 item2 item3 (fun x y z -> (x,y,z))

let list item_decoder = function
  | Data.Seq items -> Result.map_list item_decoder items
  | data -> Error ("Not a list: ")

let tag named_decoders = function
  | Data.Tag (name, value) -> (
    try
      let decode = List.assoc name named_decoders in
      decode value
    with Not_found -> Error ("Unknown tag "^name)
  )
  | data -> Error ("Not a tag union: ")

let either cases = Result.one_of ~unknown_case:"Unknown case" cases

type 'a case_decoder =
  | Case: 'b decoder * ('b -> 'a) -> 'a case_decoder

let rec decode_cases case_decoders no_match_case data = match case_decoders with
  | [] -> no_match_case ()
  | (Case (decode, use))::case_decoders -> (
    match decode data  with
    | Error _ -> decode_cases case_decoders no_match_case data
    | Ok r -> use r
  )

