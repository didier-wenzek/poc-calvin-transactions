exception SyntaxError of int * int * string

let error_msg = function
  | (line, pos, "") -> Format.sprintf "syntax error line %d, pos %d, missing token" line pos
  | (line, pos, token) -> Format.sprintf "syntax error line %d, pos %d, unexpected token '%s'" line pos token

let parse parse token =
  let parse lexbuf =
    try parse token lexbuf
    with exn -> Lexing.(
      let curr = lexbuf.lex_curr_p in
      let line = curr.pos_lnum in
      let cnum = curr.pos_cnum - curr.pos_bol in
      let token = lexeme lexbuf in
      raise (SyntaxError (line,cnum,token))
    )
  in
  fun str -> parse (Lexing.from_string str)

let parse_data = parse DataParser.data DataLexer.token
