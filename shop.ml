open Show

type customer_id = int 
type product_id = int 
type category_id = int
type order_id = int
type name = string
type quantity = int
type reason = string
type transaction_id = int64
type sequence_id = int

let pp_transaction_id formater tnx = Format.pp_print_string formater (Int64.to_string tnx)

type operation =
  | NewCustomer of customer_id * name
  | NewCategory of category_id * name
  | NewProduct of product_id * category_id * name
  | NewOrder of order_id * customer_id * (product_id * quantity) list
  | NewStockDelivery of (product_id * quantity) list

let pp_items = pp_list (pp_pair "" pp_int pp_int)

let pp_operation formatter = function
  | NewCustomer (customer_id, name) -> pp_pair "NewCustomer" pp_int pp_str formatter (customer_id, name)
  | NewCategory (category_id, name) -> pp_pair "NewCategory" pp_int pp_str formatter (category_id, name)
  | NewProduct (product_id, category_id, name) -> pp_tup3 "NewProduct" pp_int pp_int pp_str formatter (product_id, category_id, name)
  | NewOrder (order_id, customer_id, items) -> pp_tup3 "NewOrder" pp_int pp_int pp_items formatter (order_id, customer_id, items)
  | NewStockDelivery (items) -> pp_tag "NewStockDelivery" pp_items formatter items

let show_operation = show pp_operation

type query =
  | GetCustomer of customer_id
  | GetCategory of category_id
  | GetProduct of product_id
  | GetOrders of customer_id
  | GetOrder of order_id
  | GetStock of product_id list

let pp_query formatter = function
  | GetCustomer customer_id -> pp_tag "GetCustomer" pp_int formatter customer_id
  | GetCategory category_id -> pp_tag "GetCategory" pp_int formatter category_id
  | GetProduct product_id -> pp_tag "GetProduct" pp_int formatter product_id
  | GetOrders customer_id -> pp_tag "GetOrders" pp_int formatter customer_id
  | GetOrder order_id -> pp_tag "GetOrder" pp_int formatter order_id
  | GetStock items -> pp_tag "GetStock" (pp_list pp_int) formatter items

let show_query = show pp_query

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

let pp_response formatter = function
  | UnknownCustomer customer_id -> pp_tag "UnknownCustomer" pp_int formatter customer_id
  | UnknownCategory category_id -> pp_tag "UnknownCategory" pp_int formatter category_id
  | UnknownProduct product_id -> pp_tag "UnknownProduct" pp_int formatter product_id
  | UnknownOrder order_id -> pp_tag "UnknownOrder" pp_int formatter order_id
  | Customer (customer_id, name) -> pp_pair "Customer" pp_int pp_str formatter (customer_id, name)
  | Category (category_id, name) -> pp_pair "Category" pp_int pp_str formatter (category_id, name)
  | Product (product_id, category_id, name) -> pp_tup3 "Product" pp_int pp_int pp_str formatter (product_id, category_id, name)
  | Orders (customer_id, orders) -> pp_pair "Orders" pp_int (pp_list pp_int) formatter (customer_id, orders)
  | Order (order_id, customer_id, items) -> pp_tup3 "Order" pp_int pp_int pp_items formatter (order_id, customer_id, items)
  | Stock items -> pp_tag "Stock" pp_items formatter items

let show_response = show pp_response

type request =
  | Operation of operation
  | Query of query
  | IllFormated of string
  | IllTyped of string * string

let pp_request formatter = function
  | Operation operation -> pp_tag "Operation" pp_operation formatter operation
  | Query query -> pp_tag "Query" pp_query formatter query
  | IllFormated r -> pp_tag "IllFormated" pp_str formatter r
  | IllTyped (err,r) -> pp_pair "IllTyped" pp_str pp_str formatter (err,r)

let show_request = show pp_request

type outcome =
  | Accepted of transaction_id * operation
  | Rejected of transaction_id * operation * reason
  | Response of transaction_id * response
  | Ignored of transaction_id * request

let pp_outcome formatter = function
  | Accepted (tnx, op) -> pp_pair "Accepted" pp_transaction_id pp_operation formatter (tnx, op)
  | Rejected (tnx, op, err) -> pp_tup3 "Rejected" pp_transaction_id pp_operation pp_str formatter (tnx, op, err)
  | Response (tnx, r) -> pp_pair "Response" pp_transaction_id pp_response formatter (tnx, r)
  | Ignored (tnx, r) -> pp_pair  "Ignored" pp_transaction_id pp_request formatter (tnx, r) 

let show_outcome = show pp_outcome

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

type relation_value =
  | RelationVal: ('a * ('a,'b) relation * 'b) -> relation_value
  | UnknownSource: ('a * ('a,'b) relation) -> relation_value
  | UnknownTarget: (('a,'b) relation * 'b) -> relation_value

type relation_check =
  | ExistRelation: ('a * ('a,'b) relation * 'b ) -> relation_check
  | ExistSource: ('a * ('a,'b) relation) -> relation_check
  | ExistTarget: (('a,'b) relation * 'b ) -> relation_check
  | Not: relation_check -> relation_check
  | TargetGreaterThan: ('a * ('a,'b) relation * 'b ) -> relation_check

type relation_operation =
  | AddRelation: ('a * ('a,'b) relation * 'b) -> relation_operation
  | RemRelation: ('a * ('a,'b) relation * 'b) -> relation_operation

type storage_request =
  | Read of transaction_id * relation_query
  | Check of transaction_id * sequence_id * relation_check
  | Write of transaction_id * relation_operation
  | Commit of transaction_id
  | Cancel of transaction_id

type storage_response =
  | PartialRead of transaction_id * relation_value
  | PartialCheck of transaction_id * sequence_id * bool

let pp_relation f tag (type a) (type b): a -> (a,b) relation -> b -> unit =
  let p = Format.pp_print_string f in
  fun a rel b -> match rel with
  | CustomerName -> (p tag; p "(CustomerName("; pp_int f a; p "," ; pp_str f b; p "))")
  | CategoryName -> (p tag; p "(CategoryName("; pp_int f a; p "," ; pp_str f b; p "))")
  | ProductName -> (p tag; p "(ProductName("; pp_int f a; p "," ; pp_str f b; p "))")
  | ProductCategory -> (p tag; p "(ProductCategory("; pp_int f a; p "," ; pp_int f b; p "))")
  | ProductCategoryName -> (let (b,c) = b in p tag; p "(ProductCategoryName("; pp_int f a; p "," ; pp_int f b; p "," ; pp_str f c; p "))")
  | ProductQuantity -> (p tag; p "(ProductQuantity("; pp_int f a; p "," ; pp_int f b; p "))")
  | OrderCustomer -> (p tag; p "(OrderCustomer("; pp_int f a; p "," ; pp_int f b; p "))")
  | OrderProducts -> (let (b,c) = b in p tag; p "(OrderProducts("; pp_int f a; p "," ; pp_int f b; p "," ; pp_int f c; p "))")

let pp_source f tag (type a) (type b): a -> (a,b) relation -> unit =
  let p = Format.pp_print_string f in
  fun a rel -> match rel with
  | CustomerName -> (p tag; p "(CustomerName("; pp_int f a; p "))")
  | CategoryName -> (p tag; p "(CategoryName("; pp_int f a; p "))")
  | ProductName -> (p tag; p "(ProductName("; pp_int f a; p "))")
  | ProductCategory -> (p tag; p "(ProductCategory("; pp_int f a; p "))")
  | ProductCategoryName -> (p tag; p "(ProductCategoryName("; pp_int f a; p "))")
  | ProductQuantity -> (p tag; p "(ProductQuantity("; pp_int f a; p "))")
  | OrderCustomer -> (p tag; p "(OrderCustomer("; pp_int f a; p "))")
  | OrderProducts -> (p tag; p "(OrderProducts("; pp_int f a; p "))")

let pp_target f tag (type a) (type b): (a,b) relation -> b -> unit =
  let p = Format.pp_print_string f in
  fun rel b -> match rel with
  | CustomerName -> (p tag; p "(CustomerName("; pp_str f b; p "))")
  | CategoryName -> (p tag; p "(CategoryName("; pp_str f b; p "))")
  | ProductName -> (p tag; p "(ProductName("; pp_str f b; p "))")
  | ProductCategory -> (p tag; p "(ProductCategory("; pp_int f b; p "))")
  | ProductCategoryName -> (let (b,c) = b in p tag; p "(ProductCategoryName("; pp_int f b; p "," ; pp_str f c; p "))")
  | ProductQuantity -> (p tag; p "(ProductQuantity("; pp_int f b; p "))")
  | OrderCustomer -> (p tag; p "(OrderCustomer("; pp_int f b; p "))")
  | OrderProducts -> (let (b,c) = b in p tag; p "(OrderProducts("; pp_int f b; p "," ; pp_int f c; p "))")

let pp_relation_query formatter = function
  | GetTarget (a, rel) -> pp_source formatter "GetTarget" a rel
  | GetSource (rel, b) -> pp_target formatter "GetSource" rel b

let pp_relation_value formatter = function
  | RelationVal (a, rel, b) -> pp_relation formatter "RelationVal" a rel b
  | UnknownSource (a, rel) -> pp_source formatter "UnknownSource" a rel
  | UnknownTarget (b, rel) -> pp_target formatter "UnknownTarget" b rel

let rec pp_relation_check formatter = function
  | ExistRelation(a, rel, b) -> pp_relation formatter "ExistRelation" a rel b
  | ExistSource(a, rel) -> pp_source formatter "ExistSource" a rel
  | ExistTarget(rel, b) -> pp_target formatter "ExistTarget" rel  b
  | Not rel_check -> pp_tag "Not" pp_relation_check formatter rel_check
  | TargetGreaterThan(a, rel, b) -> pp_relation formatter "TargetGreaterThan" a rel b

let pp_relation_operation formatter = function
  | AddRelation(a, rel, b) -> pp_relation formatter "AddRelation" a rel b
  | RemRelation(a, rel, b) -> pp_relation formatter "RemRelation" a rel b

let pp_storage_request formatter = function
  | Read (tnx, query)               -> pp_pair "Read" pp_transaction_id pp_relation_query formatter (tnx, query)
  | Check (tnx, seq, relation_check)-> pp_tup3 "Check" pp_transaction_id pp_int pp_relation_check formatter (tnx, seq, relation_check)
  | Write (tnx, relation_operation) -> pp_pair "Write" pp_transaction_id pp_relation_operation formatter (tnx, relation_operation)
  | Commit tnx                      -> pp_tag "Commit" pp_transaction_id formatter tnx
  | Cancel tnx                      -> pp_tag "Cancel" pp_transaction_id formatter tnx

let pp_storage_response formatter = function
  | PartialRead (tnx, res)      -> pp_pair "PartialRead" pp_transaction_id pp_relation_value formatter (tnx, res)
  | PartialCheck (tnx, seq, res)-> pp_tup3 "PartialCheck" pp_transaction_id pp_int pp_bool formatter (tnx, seq, res)

let show_relation_query = show pp_relation_query
let show_relation_value = show pp_relation_value
let show_relation_check = show pp_relation_check
let show_relation_operation = show pp_relation_operation
let show_storage_request = show pp_storage_request
let show_storage_response = show pp_storage_response

let (@) = List.rev_append
let map f =
  let map_rev_cons ys x = (f x) :: ys in
  List.fold_left map_rev_cons []
let flatmap f =
  let map_rev_append ys x = (f x) @ ys in
  List.fold_left map_rev_append []

let query_subqueries = function
  | GetCustomer customer_id -> [
      GetTarget (customer_id, CustomerName);
  ]
  | GetCategory category_id -> [
      GetTarget (category_id, CategoryName);
  ]
  | GetProduct product_id -> [
      GetTarget (product_id, ProductName);
      GetTarget (product_id, ProductCategory);
  ]
  | GetOrders customer_id -> [
      GetSource (OrderCustomer, customer_id);
  ]
  | GetOrder order_id -> [
      GetTarget (order_id, OrderCustomer);
      GetTarget (order_id, OrderProducts);
  ]
  | GetStock product_ids -> map (fun product_id ->
      GetTarget (product_id, ProductQuantity)
  ) product_ids

let make_response request values =
  let invalid_argument err = raise (Invalid_argument err) in
  let order_id_of_order_customer_pair : int -> relation_value -> int = fun customer_id -> function
    | RelationVal (order_id, OrderCustomer, id) when customer_id = id -> order_id
    | UnknownTarget (OrderCustomer, id) when customer_id = id -> raise Not_found
    | _ -> invalid_argument "not an OrderCustomer pair"
  in
  let customer_id_of_order_product_pair : int -> relation_value -> int list = fun order_id -> function
    | RelationVal (_, OrderProducts, _) -> [ ]
    | RelationVal (id, OrderCustomer, customer_id) when order_id = id -> [ customer_id ]
    | UnknownSource (id, OrderCustomer) when order_id = id -> raise Not_found
    | _ -> invalid_argument "not an OrderCustomer pair"
  in
  let product_id_of_order_product_pair : int -> relation_value -> (int*int) list = fun order_id -> function
    | RelationVal (id, OrderProducts, product_qty) when order_id = id -> [ product_qty ]
    | RelationVal (_, OrderCustomer, _) -> []
    | UnknownSource (id, OrderProducts) when order_id = id -> raise Not_found
    | _ -> invalid_argument "not an OrderProduct pair"
  in
  let items_product_quantity : relation_value -> (int*int) = function
    | RelationVal (id, ProductQuantity, qty) -> (id, qty)
    | _ -> invalid_argument "not an ProductQuantity pair"
  in

  match request, values with
  | GetCustomer customer_id, [ UnknownSource (id, CustomerName) ] when id = customer_id ->
    UnknownCustomer (customer_id)
  | GetCategory category_id, [ UnknownSource (id, CategoryName) ] when id = category_id ->
    UnknownCategory (category_id)
  | GetProduct product_id, UnknownSource (id, ProductName)::_ when id = product_id ->
    UnknownProduct (product_id)
  | GetProduct product_id, UnknownSource (id, ProductCategory)::_ when id = product_id ->
    UnknownProduct (product_id)

  | GetCustomer customer_id, [ RelationVal (id, CustomerName, name) ] when id = customer_id ->
    Customer (customer_id, name)
  | GetCategory category_id, [ RelationVal (id, CategoryName, name) ] when id = category_id ->
    Category (category_id, name)
  | GetProduct product_id, [ RelationVal (id, ProductName, name); RelationVal (id', ProductCategory, category) ] when id = product_id && id' = id ->
    Product(product_id, category, name)
  | GetProduct product_id, [ RelationVal (id', ProductCategory, category); RelationVal (id, ProductName, name) ] when id = product_id && id' = id ->
    Product(product_id, category, name)
  | GetOrders customer_id, orders -> (
    try 
      Orders (customer_id, map (order_id_of_order_customer_pair customer_id) orders)
    with Not_found -> UnknownCustomer (customer_id)
  )
  | GetOrder order_id, items -> (
    try
      let customer_id = List.hd (flatmap (customer_id_of_order_product_pair order_id) items) in
      let items = flatmap (product_id_of_order_product_pair order_id) items in
      Order(order_id, customer_id, items)
    with Not_found -> UnknownOrder order_id
  )
  | GetStock _, items ->
    Stock (map items_product_quantity items)
  | _ -> invalid_argument "not an expected response"


let operation_checklist = function
  | NewCustomer (customer_id, name) -> [
      Not (ExistSource (customer_id, CustomerName))
  ]
  | NewCategory (category_id, name) -> [
      Not (ExistSource (category_id, CategoryName))
  ]
  | NewProduct (product_id, category_id, name) -> [
      Not (ExistSource (product_id, ProductName));
      Not (ExistSource (product_id, ProductCategory));
      Not (ExistSource (product_id, ProductQuantity));
      ExistSource (category_id, CategoryName);
      Not (ExistTarget (ProductCategoryName, (category_id,name)))
  ]
  | NewOrder (order_id, customer_id, items) ->
      let item_checklist (product_id,quantity) = [
        ExistSource (product_id, ProductName);
        TargetGreaterThan(product_id, ProductQuantity, quantity)
      ] in
      let items_checklist = flatmap item_checklist items
      in [
        ExistSource (customer_id, CustomerName);
        Not (ExistSource (order_id, OrderCustomer));
        Not (ExistSource (order_id, OrderProducts))
      ] @ items_checklist

  | NewStockDelivery items ->
      let item_checklist (product_id,quantity) = [
        ExistSource (product_id, ProductName)
      ] in
      flatmap item_checklist items

let operation_outcomes = function
  | NewCustomer (customer_id, name) -> [
      AddRelation (customer_id, CustomerName, name)
  ]
  | NewCategory (category_id, name) -> [
      AddRelation (category_id, CategoryName, name)
  ]
  | NewProduct (product_id, category_id, name) -> [
      AddRelation (product_id, ProductName, name);
      AddRelation (product_id, ProductCategory, category_id);
      AddRelation (product_id, ProductCategoryName, (category_id,name));
      AddRelation (product_id, ProductQuantity, 0);
  ]
  | NewOrder (order_id, customer_id, items) ->
      let item_outcomes ((product_id,quantity) as item) = [
        AddRelation(product_id, ProductQuantity, - quantity);
        AddRelation (order_id, OrderProducts, item)
      ] in
      let items_outcomes = flatmap item_outcomes items
      in [
        AddRelation (order_id, OrderCustomer, customer_id)
      ] @ items_outcomes
   
  | NewStockDelivery items ->
      let item_outcomes (product_id,quantity) = [
        AddRelation(product_id, ProductQuantity, quantity);
      ] in
      flatmap item_outcomes items

let storage_request_of_check tnx seq r = Check (tnx,seq,r)
let storage_request_of_query tnx r = Read (tnx,r)
let storage_request_of_update tnx r = Write (tnx,r)

let query_storage_requests tnx query =
  let subqueries = query_subqueries query in
  List.map (storage_request_of_query tnx) subqueries

let operation_storage_requests tnx op =
  let checklist = operation_checklist op |> List.mapi (storage_request_of_check tnx) in
  let updates = operation_outcomes op |> List.map (storage_request_of_update tnx) in
  List.rev_append checklist updates

type reservation_key =
  | SrcRes: 'a * ('a,'b) relation -> reservation_key
  | TgtRes: ('a,'b) relation * 'b -> reservation_key

(* FIXME : reservations shall be derived from a query plan *)
let read_reservation = function
  | GetTarget (a,rel) -> SrcRes (a,rel)
  | GetSource (rel,b) -> TgtRes (rel,b)

let rec check_reservation = function 
  | ExistSource (customer_id, CustomerName) -> SrcRes (customer_id, CustomerName)
  | ExistSource (category_id, CategoryName) -> SrcRes (category_id, CategoryName)
  | ExistSource (product_id, ProductName) -> SrcRes (product_id, ProductName)
  | ExistSource (product_id, ProductCategory) ->  SrcRes (product_id, ProductCategory)
  | ExistTarget (ProductCategoryName, category_name) -> TgtRes (ProductCategoryName, category_name)
  | ExistSource (product_id, ProductQuantity) -> SrcRes (product_id, ProductQuantity)
  | ExistSource (order_id, OrderCustomer) -> SrcRes (order_id, OrderCustomer)
  | ExistSource (order_id, OrderProducts) -> SrcRes (order_id, OrderProducts)
  | TargetGreaterThan (product_id, ProductQuantity, _) -> SrcRes (product_id, ProductQuantity)
  | Not p -> check_reservation p
  | _ -> raise (Invalid_argument "NOT IMPLEMENT")

let write_reservation = function
  | AddRelation (customer_id, CustomerName, _) -> SrcRes (customer_id, CustomerName)
  | RemRelation (customer_id, CustomerName, _) -> SrcRes (customer_id, CustomerName)
  | AddRelation (category_id, CategoryName, _) -> SrcRes (category_id, CategoryName)
  | RemRelation (category_id, CategoryName, _) -> SrcRes (category_id, CategoryName)
  | AddRelation (product_id, ProductName, _) ->  SrcRes (product_id, ProductName)
  | RemRelation (product_id, ProductName, _) -> SrcRes (product_id, ProductName)
  | AddRelation (product_id, ProductCategory, _) -> SrcRes (product_id, ProductCategory)
  | RemRelation (product_id, ProductCategory, _) -> SrcRes (product_id, ProductCategory)
  | AddRelation (product_id, ProductCategoryName, category_name) -> TgtRes (ProductCategoryName, category_name)
  | RemRelation (_, ProductCategoryName, category_name) -> TgtRes (ProductCategoryName, category_name)
  | AddRelation (product_id, ProductQuantity, _) -> SrcRes (product_id, ProductQuantity)
  | RemRelation (product_id, ProductQuantity, _) -> SrcRes (product_id, ProductQuantity)
  | AddRelation (order_id, OrderCustomer, _)  -> SrcRes (order_id, OrderCustomer)
  | RemRelation (order_id, OrderCustomer, _) -> SrcRes (order_id, OrderCustomer)
  | AddRelation (order_id, OrderProducts, (product_id,_))  -> SrcRes (product_id, OrderProducts)
  | RemRelation (order_id, OrderProducts, (product_id,_)) -> SrcRes (product_id, OrderProducts)

