%{
 open Data
 
 let remove_quotes s =
   let len = String.length s in
   String.sub s 1 (len - 2)

%}

%token <string> NAME
%token <string> TYPENAME
%token <string> STR
%token <string> NUM
%token COMMA
%token EQ COLON
%token L_PAR R_PAR
%token L_CURL R_CURL
%token L_BRACKET R_BRACKET
%token LNSEP
%token EOF
%token WEIRD_CHAR

%start data
%type <Data.t> data

%%

data:
  | STR { Data.Str $1 }
  | NUM { Data.Num $1 }
  | L_PAR data_seq R_PAR { Data.Tup $2 }
  | L_BRACKET data_seq R_BRACKET { Data.Seq $2 }
  | L_CURL pair_seq R_CURL { Data.Rec $2 }
  | TYPENAME data { match $2 with Data.Tup [d] -> Data.Tag ($1, d) | _ -> Data.Tag ($1, $2) }
  | TYPENAME { Data.Tag ($1, Data.Tup []) }
;

data_seq:
|                            { [] }
| lnseps                     { [] }
|        rev_data_seq        { List.rev $1 }
|        rev_data_seq lnseps { List.rev $1 }
| lnseps rev_data_seq        { List.rev $2 }
| lnseps rev_data_seq lnseps { List.rev $2 }
;

rev_data_seq:
| data                    { [$1] }
| rev_data_seq sep data   { $3 :: $1 }
;

pair_seq:
|                            { [] }
| lnseps                     { [] }
|        rev_pair_seq        { List.rev $1 }
|        rev_pair_seq lnseps { List.rev $1 }
| lnseps rev_pair_seq        { List.rev $2 }
| lnseps rev_pair_seq lnseps { List.rev $2 }
;

rev_pair_seq:
| NAME EQ data                   { [($1, $3)] }
| rev_pair_seq sep NAME EQ data  { ($3, $5) :: $1 }
;

lnseps:
| LNSEP                 { () }
| LNSEP lnseps          { () }

sep:  
|        COMMA          { () }   /* At most one comma, eventualy decorated by newlines. */
| lnseps COMMA          { () }
|        COMMA lnseps   { () }
| lnseps COMMA lnseps   { () }
| lnseps                { () }   /* At least one newline, if there is no comma. */
