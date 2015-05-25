# Transactional System over a Concurrent Hash Map

Run on server TpccEmbeded

java -Xmx4g -Xms2g -cp target/myhashdb-1.0.3.jar:../thrift-0.9.2/lib/java/build/libthrift-0.9.2.jar  bench.tpcc.TpccEmbeded -w 10 -c 10 -t 10 -tp SI -B


java -Xmx4g -Xms2g -cp target/myhashdb-1.0.3.jar:/local/cj.gomes/thrift-0.9.2/lib/java/build/libthrift-0.9.2.jar bench.tpcc.TpccEmbeded -w 10 -c 10 -t 10 -tp SI -B