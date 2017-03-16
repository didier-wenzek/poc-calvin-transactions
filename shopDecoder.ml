open Shop

let decode_operation = DataDecoder.(tag [
  "NewCustomer", struct2 int string (fun id name -> NewCustomer (id, name));
  "NewCategory", struct2 int string (fun id name -> NewCategory (id, name));
  "NewProduct", struct3 int int string (fun id cid name -> NewProduct (id, cid, name));
  "NewOrder", struct3 int int (list (pair int int)) (fun oid cid items -> NewOrder (oid, cid, items)); 
  "NewStockDelivery", struct1 (list (pair int int)) (fun items -> NewStockDelivery items);
])

let decode_query = DataDecoder.(tag [
  "GetCustomer", struct1 int (fun id -> GetCustomer id);
  "GetCategory", struct1 int (fun id -> GetCategory id);
  "GetProduct", struct1 int (fun id -> GetProduct id);
  "GetOrders", struct1 int (fun id -> GetOrders id);
  "GetOrder", struct1 int (fun id -> GetOrder id);
  "GetStock", struct1 (list int) (fun ids -> GetStock ids);
])

let decode_request = DataDecoder.(either [
  struct1 decode_operation (fun op -> Operation op);
  struct1 decode_query (fun op -> Query op);
])

let decode_response = DataDecoder.(tag [
  "Customer", struct2 int string (fun id name -> Customer (id, name));
  "Category", struct2 int string (fun id name -> Category (id, name));
  "Product", struct3 int int string (fun id cid name -> Product (id, cid, name));
  "Orders", struct2 int (list int) (fun id orders -> Orders (id, orders)); 
  "Order", struct3 int int (list (pair int int)) (fun oid cid items -> Order (oid, cid, items)); 
  "Stock", struct1 (list (pair int int)) (fun items -> Stock items);
])

let decode_outcome = DataDecoder.(tag [
  "Accepted", struct2 int64 decode_operation (fun tnx op -> Accepted (tnx,op));
  "Rejected", struct3 int64 decode_operation string (fun tnx op error -> Rejected (tnx, op, error));
  "Response", struct2 int64 decode_response (fun tnx r -> Response (tnx,r))
])

type relation_pair =
  | Relation: ('a * ('a,'b) relation * 'b) -> relation_pair

let decode_relation = DataDecoder.(tag [
  "CustomerName", struct2 int string (fun id name -> Relation (id, CustomerName, name));
  "CategoryName", struct2 int string (fun id name -> Relation (id, CategoryName, name));
  "ProductName", struct2 int string (fun id name -> Relation (id, ProductName, name));
  "ProductCategory", struct2 int int (fun id cat -> Relation (id, ProductCategory, cat));
  "ProductCategoryName", struct3 int int string (fun id cat name -> Relation (id, ProductCategoryName, (cat,name)));
  "ProductQuantity", struct2 int int (fun id qtd -> Relation (id, ProductQuantity, qtd));
  "OrderCustomer", struct2 int int (fun id cus -> Relation (id, OrderCustomer, cus));
  "OrderProducts", struct3 int int int (fun id pid qtd -> Relation (id, OrderProducts, (pid,qtd)));
])

type relation_src =
  | Source: ('a * ('a,'b) relation) -> relation_src

let decode_source = DataDecoder.(tag [
  "CustomerName", struct1 int (fun id -> Source (id, CustomerName));
  "CategoryName", struct1 int (fun id -> Source (id, CategoryName));
  "ProductName", struct1 int (fun id -> Source (id, ProductName));
  "ProductCategory", struct1 int (fun id -> Source (id, ProductCategory));
  "ProductCategoryName", struct1 int (fun id -> Source (id, ProductCategoryName));
  "ProductQuantity", struct1 int (fun id -> Source (id, ProductQuantity));
  "OrderCustomer", struct1 int (fun id -> Source (id, OrderCustomer));
  "OrderProducts", struct1 int (fun id -> Source (id, OrderProducts));
])

type relation_target =
  | Target: (('a,'b) relation * 'b) -> relation_target

let decode_target = DataDecoder.(tag [
  "CustomerName", struct1 string (fun name -> Target (CustomerName, name));
  "CategoryName", struct1 string (fun name -> Target (CategoryName, name));
  "ProductName", struct1 string (fun name -> Target (ProductName, name));
  "ProductCategory", struct1 int (fun cat -> Target (ProductCategory, cat));
  "ProductCategoryName", struct2 int string (fun cat name -> Target (ProductCategoryName, (cat,name)));
  "ProductQuantity", struct1 int (fun qtd -> Target (ProductQuantity, qtd));
  "OrderCustomer", struct1 int (fun cus -> Target (OrderCustomer, cus));
  "OrderProducts", struct2 int int (fun pid qtd -> Target (OrderProducts, (pid,qtd)));
])

let decode_relation_query = DataDecoder.(tag [
  "GetTarget", struct1 decode_source (function Source rel -> GetTarget rel);
  "GetSource", struct1 decode_target (function Target rel -> GetSource rel);
])

let decode_relation_value = DataDecoder.(tag [
  "RelationVal", struct1 decode_relation (function Relation rel -> RelationVal rel);
  "UnknownSource", struct1 decode_source (function Source rel -> UnknownSource rel);
  "UnknownTarget", struct1 decode_target (function Target rel -> UnknownTarget rel);
])

let decode_relation_check = DataDecoder.(tag [
  "ExistRelation", struct1 decode_relation (function Relation rel -> ExistRelation rel);
  "ExistSource", struct1 decode_source (function Source rel -> ExistSource rel);
  "ExistTarget", struct1 decode_target (function Target rel -> ExistTarget rel);
  "TargetGreaterThan", struct1 decode_relation (function Relation rel -> TargetGreaterThan rel);
])

let decode_relation_check = DataDecoder.(either [
  decode_relation_check;
  tag ["Not", struct1 decode_relation_check (fun test -> Not test)]
])

let decode_relation_operation = DataDecoder.(tag [
  "AddRelation", struct1 decode_relation (function Relation rel -> AddRelation rel);
  "RemRelation", struct1 decode_relation (function Relation rel -> RemRelation rel);
])

let decode_storage_request = DataDecoder.(tag [
  "Read", struct2 int64 decode_relation_query (fun tnx q -> Read (tnx,q));
  "Check", struct3 int64 int decode_relation_check (fun tnx seq c -> Check (tnx,seq,c));
  "Write", struct2 int64 decode_relation_operation (fun tnx op -> Write (tnx,op));
  "Commit", struct1 int64 (fun tnx -> Commit tnx);
  "Cancel", struct1 int64 (fun tnx -> Cancel tnx);
])

let decode_storage_response = DataDecoder.(tag [
  "PartialRead", struct2 int64 decode_relation_value (fun tnx q -> PartialRead (tnx,q));
  "PartialCheck", struct3 int64 int bool (fun tnx seq c -> PartialCheck (tnx,seq,c));
])
