namespace java thrift.server  // defines the namespace

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

service DBService {  // defines the service to add two numbers

    long txn_begin();
    bool txn_commit() throws (1:AbortException e);
    bool txn_abort();
    map<string,binary> get(1:string key) throws (1:AbortException ae, 2:NoSuchKeyException nse);
    void put(1:string key, 2:map<string,binary> value) throws (1:AbortException e);

    bool reset(1:string type);
}