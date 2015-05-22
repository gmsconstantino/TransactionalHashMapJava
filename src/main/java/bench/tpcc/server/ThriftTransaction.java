package bench.tpcc.server;

import fct.thesis.database.Database;
import fct.thesis.database.TransactionAbortException;
import fct.thesis.database.TransactionTimeoutException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import thrift.server.DBService;
import thrift.tpcc.TpccService;
import thrift.tpcc.schema.MyObject;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by Constantino Gomes on 17/05/15.
 */
public class ThriftTransaction<K extends String,V extends MyObject> extends fct.thesis.database.Transaction<K,V> {

    TTransport transport;
    TpccService.Client client;

    private static final boolean verbose = false;

    public ThriftTransaction() {
        super(null);

        String ip = Config.SERVER_IP_DEFAULT;
        int port = Integer.parseInt(Config.SERVER_PORT_DEFAULT);

        if(verbose)
            System.out.println("Connect to: "+ip+":"+port);

        try {
            transport = new TSocket(ip, port);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new TpccService.Client(protocol);
        } catch (TTransportException e) {
            e.printStackTrace();
        }

        try {
            client.txn_begin();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    @Override
    public V get(int table, K key) throws TransactionTimeoutException, TransactionAbortException {
        try {
            return (V) client.get(table,key);
        } catch (TException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void put(int table, K key, V value) throws TransactionTimeoutException, TransactionAbortException {
        try {
            client.put(table, key, value);
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean commit() throws TransactionTimeoutException, TransactionAbortException {
        try {
            return client.txn_commit();
        } catch (TException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void abort() throws TransactionTimeoutException {
        try {
            client.txn_abort();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    public void finalize() throws Throwable {
        transport.close();
        super.finalize();
    }

}
