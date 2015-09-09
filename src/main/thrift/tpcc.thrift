namespace java thrift.tpcc  // defines the namespace

include "db_schema.thrift"

typedef i32 int  //typedefs to get convenient names for your types
typedef i64 long

struct DBRequest {
    1:binary key
    2:optional map<string,binary> value
}

exception AbortException {
}

exception NoSuchKeyException {
}

service TpccService {  // defines the service to add two numbers

    long txn_begin();
    bool txn_commit() throws (1:AbortException e);
    bool txn_abort();
    db_schema.TObject get(1:int table, 2:string key) throws (1:AbortException ae, 2:NoSuchKeyException nse);
    void put(1:int table, 2:string key, 3:db_schema.TObject value) throws (1:AbortException e);

    bool reset(1:string type, 2:int ntables);
    bool shutdown();
}