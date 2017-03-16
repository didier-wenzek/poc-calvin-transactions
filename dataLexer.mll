{
  open DataParser
}

let name = ['a'-'z'] ['a'-'z' 'A'-'Z' '0'-'9''_']*
let typename = ['A'-'Z' '\''] ['a'-'z' 'A'-'Z' '0'-'9''_']*
let str = '"'([^'"''\n']|"\\\"")*'"'
let num = '-'? ['0'-'9']+ ('.'['0'-'9']+)? ('e'['+''-']?['0'-'9']+)?
let space = [' ' '\t'] | "\\\n"
let comment = "#" [^'\n']* '\n'
let eof_comment = "#" [^'\n']* eof

rule token = parse
  | space           { token lexbuf }
  | '\n'            { LNSEP }
  | comment         { LNSEP }

  | ','             { COMMA }
  | '='             { EQ }
  | ':'             { COLON }
  | '('             { L_PAR }
  | ')'             { R_PAR }
  | '{'             { L_CURL }
  | '}'             { R_CURL }
  | '['             { L_BRACKET }
  | ']'             { R_BRACKET }

  | name as s       { NAME(s) }
  | typename as s   { TYPENAME(s) }
  | str as s        { STR(s) }
  | num as s        { NUM(s) }

  | eof             { EOF }
  | eof_comment     { EOF }
  | [^'.']          { WEIRD_CHAR }

