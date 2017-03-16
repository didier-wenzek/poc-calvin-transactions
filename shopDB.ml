open Shop

module IntKeyedMap = Map.Make(struct
  type t = int
  let compare x y = x - y
end)

module IntStrKeyedMap = Map.Make(struct
  type t = int*string
  let compare (x,s) (y,t) =
    let c = x - y in
    if c = 0
    then compare s t
    else c
end)

module IntIntKeyedMap = Map.Make(struct
  type t = int*int
  let compare (x,s) (y,t) =
    let c = x - y in
    if c = 0
    then s - t
    else c
end)

module IntIntMultiMap = struct
  type 'a t = ('a IntKeyedMap.t) IntKeyedMap.t

  let empty = IntKeyedMap.empty

  let mem x m = IntKeyedMap.mem x m

  let mem_pair x y m =
    try IntKeyedMap.mem y (IntKeyedMap.find x m)
    with Not_found -> false

  let get_all x m =
    try IntKeyedMap.find x m
    with Not_found -> IntKeyedMap.empty

  let get_all_pairs x m =
    IntKeyedMap.fold (fun y z m -> (x,y)::m) (get_all x m) []

  let add x y z m = 
    let former = get_all x m in
    let updated = IntKeyedMap.add y z former in
    IntKeyedMap.add x updated m

  let remove x y m =
    try
      let former = IntKeyedMap.find x m in
      let updated = IntKeyedMap.remove y former in
      IntKeyedMap.add x updated m
    with Not_found -> m
end

let get_sum key map =
  try IntKeyedMap.find key map
  with Not_found -> 0

let add_sum key update map =
  let base = get_sum key map in
  IntKeyedMap.add key (base + update) map

let rem_sum key update map =
  let base = get_sum key map in
  IntKeyedMap.add key (base - update) map

let rem_single key value map =
  try
    let former = IntKeyedMap.find key map in
    if former = value
    then IntKeyedMap.remove key map
    else  map
  with Not_found -> map

type state = {
  customer_name: name IntKeyedMap.t ;
  category_name: name IntKeyedMap.t ;
  product_name: name IntKeyedMap.t ;
  product_category: int IntKeyedMap.t ;
  product_category_name_index: int IntStrKeyedMap.t;
  product_quantity: int IntKeyedMap.t ;
  order_customer: int IntKeyedMap.t ;
  order_product: quantity IntIntMultiMap.t ;
}

let empty = {
  customer_name = IntKeyedMap.empty ;
  category_name = IntKeyedMap.empty ;
  product_name = IntKeyedMap.empty ;
  product_category = IntKeyedMap.empty ;
  product_category_name_index = IntStrKeyedMap.empty ;
  product_quantity = IntKeyedMap.empty ;
  order_customer = IntKeyedMap.empty ;
  order_product = IntIntMultiMap.empty ;
}

let rec check state = function
  | ExistSource (customer_id, CustomerName) -> IntKeyedMap.mem customer_id state.customer_name
  | ExistSource (category_id, CategoryName) -> IntKeyedMap.mem category_id state.category_name
  | ExistSource (product_id, ProductName) -> IntKeyedMap.mem product_id state.product_name
  | ExistSource (product_id, ProductCategory) -> IntKeyedMap.mem product_id state.product_category
  | ExistTarget (ProductCategoryName, category_name) -> IntStrKeyedMap.mem category_name state.product_category_name_index
  | ExistSource (product_id, ProductQuantity) -> IntKeyedMap.mem product_id state.product_quantity
  | ExistSource (order_id, OrderCustomer) -> IntKeyedMap.mem order_id state.order_customer
  | ExistSource (order_id, OrderProducts) -> IntIntMultiMap.mem order_id state.order_product
  | TargetGreaterThan (product_id, ProductQuantity, quantity) -> (
    let available = get_sum product_id state.product_quantity in
    available >= quantity
  )
  | Not p -> not (check state p)
  | _ -> false (* not implemented *)

let apply state update = match update with
  | AddRelation (customer_id, CustomerName, name) -> { state with
    customer_name = IntKeyedMap.add customer_id name state.customer_name
  }
  | RemRelation (customer_id, CustomerName, name) -> { state with
    customer_name = rem_single customer_id name state.customer_name
  }
  | AddRelation (category_id, CategoryName, name) -> { state with
    category_name = IntKeyedMap.add category_id name state.category_name
  }
  | RemRelation (category_id, CategoryName, name) -> { state with
    category_name = rem_single category_id name state.category_name
  }
  | AddRelation (product_id, ProductName, name) -> { state with
    product_name = IntKeyedMap.add product_id name state.product_name
  }
  | RemRelation (product_id, ProductName, name) -> { state with
    product_name = rem_single product_id name state.product_name
  }
  | AddRelation (product_id, ProductCategory, category_id) -> { state with
    product_category = IntKeyedMap.add product_id category_id state.product_category
  }
  | RemRelation (product_id, ProductCategory, category_id) -> { state with
    product_category = rem_single product_id category_id state.product_category
  }
  | AddRelation (product_id, ProductCategoryName, (category_id,name)) -> { state with
    product_category_name_index = IntStrKeyedMap.add (category_id,name) product_id state.product_category_name_index
  }
  | RemRelation (_, ProductCategoryName, (category_id,name)) -> { state with
    product_category_name_index = IntStrKeyedMap.remove (category_id,name) state.product_category_name_index
  }
  | AddRelation (product_id, ProductQuantity, quantity) -> { state with
    product_quantity = add_sum product_id quantity state.product_quantity
  }
  | RemRelation (product_id, ProductQuantity, quantity) -> { state with
    product_quantity = rem_sum product_id quantity state.product_quantity
  }
  | AddRelation (order_id, OrderCustomer, customer_id) -> { state with
    order_customer = IntKeyedMap.add order_id customer_id state.order_customer
  }
  | RemRelation (order_id, OrderCustomer, customer_id) -> { state with
    order_customer = rem_single order_id customer_id state.order_customer
  }
  | AddRelation (order_id, OrderProducts, (product_id,quantity)) -> { state with
    order_product = IntIntMultiMap.add order_id product_id quantity state.order_product
  }
  | RemRelation (order_id, OrderProducts, (product_id,_)) -> { state with
    order_product = IntIntMultiMap.remove order_id product_id state.order_product
  }

let get_atomic_response state = function
  | GetTarget (customer_id, CustomerName) -> (
    try
      let name = IntKeyedMap.find customer_id state.customer_name in
      [RelationVal (customer_id, CustomerName, name)]
    with Not_found -> [ UnknownSource (customer_id, CustomerName)]
  )
  | GetTarget (category_id, CategoryName) -> (
    try
      let name = IntKeyedMap.find category_id state.category_name in
      [RelationVal (category_id, CategoryName, name)]
    with Not_found -> [ UnknownSource (category_id, CategoryName)]
  )
  | GetTarget (product_id, ProductName) -> (
    try
      let name = IntKeyedMap.find product_id state.product_name in
      [RelationVal (product_id, ProductName, name)]
    with Not_found -> [ UnknownSource (product_id, ProductName)]
  )
  | GetTarget (product_id, ProductCategory) -> (
    try
      let category_id = IntKeyedMap.find product_id state.product_category in
      [RelationVal (product_id, ProductCategory, category_id)]
    with Not_found -> [ UnknownSource (product_id, ProductCategory)]
  )
  | GetSource(OrderCustomer, customer_id) -> [
    (* TODO *)
  ]
  | GetTarget (order_id, OrderCustomer) -> (
    try
      let customer_id = IntKeyedMap.find order_id state.order_customer in
      [RelationVal (order_id, OrderCustomer, customer_id)]
    with Not_found -> [ UnknownSource (order_id, OrderCustomer)]
  )
  | GetTarget (order_id, OrderProducts) -> (
    try
      let items = IntIntMultiMap.get_all_pairs order_id state.order_product in
      List.map (fun product_qty -> RelationVal (order_id, OrderProducts, product_qty)) items
    with Not_found -> [ UnknownSource (order_id, OrderProducts)]
  )
  | GetTarget (product_id, ProductQuantity) -> (
    try
      let quantity = get_sum product_id state.product_quantity in
      [RelationVal (product_id, ProductQuantity, quantity)]
    with Not_found -> [ UnknownSource (product_id, ProductQuantity) ]
  )
  | _ -> []

let get_response state = function
  | GetCustomer customer_id -> (
    try
      let name = IntKeyedMap.find customer_id state.customer_name in
      Customer (customer_id, name)
    with Not_found -> UnknownCustomer customer_id
  )
  | GetCategory category_id -> (
    try
      let name = IntKeyedMap.find category_id state.category_name in
      Category (category_id, name)
    with Not_found -> UnknownCategory category_id
  )
  | GetProduct product_id -> (
    try
      let name = IntKeyedMap.find product_id state.product_name in
      let category_id = IntKeyedMap.find product_id state.product_category in
      Product (product_id, category_id, name)
    with Not_found -> UnknownProduct product_id
  )
  | GetOrders customer_id -> (
    try
      let _ = IntKeyedMap.find customer_id state.customer_name in
      Orders (customer_id, [])      (* TODO *)
    with Not_found -> UnknownCustomer customer_id
  )
  | GetOrder order_id -> (
    try
      let customer_id = IntKeyedMap.find order_id state.order_customer in
      let items = IntIntMultiMap.get_all_pairs order_id state.order_product in
      Order (order_id, customer_id, items)
    with Not_found -> UnknownOrder order_id
  )
  | GetStock product_ids -> (
    let get_quantity stock product =
      let quantity = get_sum product stock in
      (product, quantity)
    in
    let quantities = List.map (get_quantity state.product_quantity) product_ids in
    Stock quantities
  )

let check_operation state op =
  let checklist = operation_checklist op in
  List.for_all (check state) checklist

let apply_operation state tnx op =
  if check_operation state op
  then
    let response = Accepted (tnx,op) in
    let updates = operation_outcomes op in
    let new_state = List.fold_left apply state updates in
    response, new_state
  else
    let response = Rejected (tnx, op, "missing pre-requisites") in
    response, state

let apply_request state tnx = function
  | Query q -> (Response (tnx, get_response state q), state)
  | Operation op -> apply_operation state tnx op
  | garbage -> (Ignored (tnx,garbage) , state)


