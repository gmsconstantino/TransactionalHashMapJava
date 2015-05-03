package thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import thrift.server.DBService;

/**
 * Created by gomes on 03/05/15.
 */
public class ShutdownDB {

    public static void main(String[] args) {

        if (args.length < 1 || args.length > 2){
            usageMessage();
            System.exit(0);
        }

        String ip = args[0];
        int port = 9090;

        if (args.length == 2 )
            port = Integer.parseInt(args[1]);

        TTransport transport;
        DBService.Client client;

        try {
            transport = new TSocket(ip, port);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new DBService.Client(protocol);

            if(client.shutdown())
                System.out.println("Stopped Database.");

            transport.close();
            return;
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException x) {
            x.printStackTrace();
        }
        System.out.println("Error: Failed to stop database.");
    }

    public static void usageMessage()
    {
        System.out.println("Usage: java thrift.ResetDB type hostname [port]");
        System.out.println("Options:");
        System.out.println("  host: name of the host with ServerDb running;");
        System.out.println("  port:  specify the port to connect to ServerDB, default 9090;");
    }

}
