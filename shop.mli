type customer_id = int
type product_id = int
type category_id = int
type order_id = int
type name = string
type quantity = int
type reason = string
type transaction_id = int64
type sequence_id = int

type operation =
  | NewCustomer of customer_id * name
  | NewCategory of category_id * name
  | NewProduct of product_id * category_id * name
  | NewOrder of order_id * customer_id * (product_id * quantity) list
  | NewStockDelivery of (product_id * quantity) list

val show_operation: operation -> string

type query =
  | GetCustomer of customer_id
  | GetCategory of category_id
  | GetProduct of product_id
  | GetOrders of customer_id
  | GetOrder of order_id
  | GetStock of product_id list

val show_query: query -> string

type response =
  | UnknownCustomer of customer_id
  | UnknownCategory of category_id
  | UnknownProduct of product_id
  | UnknownOrder of order_id
  | Customer of customer_id * name
  | Category of category_id * name
  | Product of product_id * category_id * name
  | Orders of customer_id * order_id list
  | Order of order_id * customer_id * (product_id * quantity) list
  | Stock of (product_id * quantity) list

val show_response: response -> string

type request =
  | Operation of operation
  | Query of query
  | IllFormated of string
  | IllTyped of string * string

val show_request: request -> string

type outcome =
  | Accepted of transaction_id * operation
  | Rejected of transaction_id * operation * reason
  | Response of transaction_id * response
  | Ignored of transaction_id * request

val show_outcome: outcome -> string

type (_,_) relation =
  | CustomerName : (customer_id, name) relation
  | CategoryName : (category_id, name) relation
  | ProductName : (product_id, name) relation
  | ProductCategory : (product_id, category_id) relation
  | ProductCategoryName : (product_id,category_id*name) relation
  | ProductQuantity: (product_id, quantity) relation
  | OrderCustomer : (order_id, customer_id) relation
  | OrderProducts : (order_id, product_id * quantity) relation

type relation_query =
  | GetTarget: ('a * ('a,'b) relation) -> relation_query
  | GetSource: (('a,'b) relation * 'b) -> relation_query

val show_relation_query: relation_query -> string

type relation_value =
  | RelationVal: ('a * ('a,'b) relation * 'b) -> relation_value
  | UnknownSource: ('a * ('a,'b) relation) -> relation_value
  | UnknownTarget: (('a,'b) relation * 'b) -> relation_value

val show_relation_value: relation_value -> string

type relation_check =
  | ExistRelation: ('a * ('a,'b) relation * 'b ) -> relation_check
  | ExistSource: ('a * ('a,'b) relation) -> relation_check
  | ExistTarget: (('a,'b) relation * 'b ) -> relation_check
  | Not: relation_check -> relation_check
  | TargetGreaterThan: ('a * ('a,'b) relation * 'b ) -> relation_check

val show_relation_check: relation_check -> string

type relation_operation =
  | AddRelation: ('a * ('a,'b) relation * 'b) -> relation_operation
  | RemRelation: ('a * ('a,'b) relation * 'b) -> relation_operation

val show_relation_operation: relation_operation -> string

type storage_request =
  | Read of transaction_id * relation_query
  | Check of transaction_id * sequence_id * relation_check
  | Write of transaction_id * relation_operation
  | Commit of transaction_id
  | Cancel of transaction_id

val show_storage_request: storage_request -> string

type storage_response =
  | PartialRead of transaction_id * relation_value
  | PartialCheck of transaction_id * sequence_id * bool

val show_storage_response: storage_response -> string

val query_subqueries: query -> relation_query list
val operation_checklist: operation -> relation_check list
val operation_outcomes: operation -> relation_operation list

val query_storage_requests: transaction_id -> query-> storage_request list
val operation_storage_requests: transaction_id -> operation-> storage_request list

val make_response: query -> relation_value list -> response

type reservation_key =
  | SrcRes: 'a * ('a,'b) relation -> reservation_key
  | TgtRes: ('a,'b) relation * 'b -> reservation_key

val read_reservation: relation_query -> reservation_key
val check_reservation: relation_check -> reservation_key
val write_reservation: relation_operation -> reservation_key
