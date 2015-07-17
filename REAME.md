# Transactional System over a Concurrent Hash Map

Run on server TpccEmbeded

java -Xmx20g -Xms4g -cp target/myhashdb-1.0.3.jar:../thrift-0.9.2/lib/java/build/libthrift-0.9.2.jar  bench.tpcc.TpccEmbeded -w 10 -c 10 -t 10 -tp SI -B

java -Xmx4g -Xms2g -cp target/myhashdb-1.0.3.jar:/local/cj.gomes/thrift-0.9.2/lib/java/build/libthrift-0.9.2.jar bench.tpcc.TpccEmbeded -w 10 -c 10 -t 10 -tp SI -B

java -cp target/myhashdb-1.0.3.jar bench.MicroSI SI 30000 8 20 10 10

mvn exec:java -Dexec.mainClass="bench.tpcc.TpccEmbeded" -Dexec.args="-w 4 -c 1 -t 60 -tp SI -B"