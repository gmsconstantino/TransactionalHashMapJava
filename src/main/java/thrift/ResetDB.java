package thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import thrift.server.DBService;

/**
 * Created by gomes on 02/05/15.
 */
public class ResetDB {

    public static void main(String[] args) {

        if (args.length < 2 || args.length > 3){
            usageMessage();
            System.exit(0);
        }

        String ip = args[1];
        int port = 9090;

        if (args.length == 3 )
            port = Integer.parseInt(args[2]);

        TTransport transport;
        DBService.Client client;

        try {
            transport = new TSocket(ip, port);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new DBService.Client(protocol);

            if(client.reset(args[0]))
                System.out.println("Reset database to use algorithm "+args[0]);

            transport.close();
            return;
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException x) {
            x.printStackTrace();
        }
        System.out.println("Error: Failed to reset database to use "+args[0]);
    }

    public static void usageMessage()
    {
        System.out.println("Usage: java thrift.ResetDB type hostname [port]");
        System.out.println("Options:");
        System.out.println("  type: reset database to algorithm;");
        System.out.println("  host: name of the host with ServerDb running;");
        System.out.println("  port:  specify the port to connect to ServerDB, default 9090;");
    }

}
