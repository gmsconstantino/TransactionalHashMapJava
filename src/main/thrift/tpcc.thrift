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

service TpccService {  // defines the service to add two numbers

    bool reset(1:string type);
    bool shutdown();
}