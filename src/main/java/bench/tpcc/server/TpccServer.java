package bench.tpcc.server;

import bench.tpcc.Environment;
import fct.thesis.database.TransactionFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import fct.thesis.database.TransactionTypeFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Created by gomes on 01/05/15.
 */
public class TpccServer {

    static TServer server;

    public static void usageMessage()
    {
        System.out.println("Usage: java bench.tpcc.server.TpccServer [options]");
        System.out.println("Options:");
        System.out.println("  -p transaction.type=[TYPE]: ");
        System.out.println("Others:");
        System.out.println("  -P propertyfile: load properties from the given file. Multiple files can");
        System.out.println("                   be specified, and will be processed in the order specified");
        System.out.println("  -p name=value:  specify a property to be passed to the DB and workloads;");
        System.out.println("                  multiple properties can be specified, and override any");
        System.out.println("                  values in the propertyfile");
    }

    public static void ServerDB() {
        try {
            TServerTransport serverTransport = new TServerSocket(9090);
            TThreadPoolServer.Args args=new TThreadPoolServer.Args(serverTransport).minWorkerThreads(64)
                    .maxWorkerThreads(128).processorFactory(new TpccProcessorFactory());

            // Use this for a multithreaded server
            server = new TThreadPoolServer(args);

            System.out.println("Starting the Database server...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        Properties props=new Properties();

        //parse arguments
        int argindex=0;

        if (args.length==0)
        {
            usageMessage();
            System.exit(0);
        }

        while (args[argindex].startsWith("-"))
        {
            if (args[argindex].compareTo("-P")==0)
            {
                argindex++;
                if (argindex>=args.length)
                {
                    usageMessage();
                    System.exit(0);
                }
                String propfile=args[argindex];
                argindex++;

                Properties myfileprops=new Properties();
                try
                {
                    myfileprops.load(new FileInputStream(propfile));
                }
                catch (IOException e)
                {
                    System.out.println(e.getMessage());
                    System.exit(0);
                }

                //Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
                for (Enumeration e=myfileprops.propertyNames(); e.hasMoreElements(); )
                {
                    String prop=(String)e.nextElement();
                    props.setProperty(prop,myfileprops.getProperty(prop));
                }

            }
            else if (args[argindex].compareTo("-p")==0)
            {
                argindex++;
                if (argindex>=args.length)
                {
                    usageMessage();
                    System.exit(0);
                }
                int eq=args[argindex].indexOf('=');
                if (eq<0)
                {
                    usageMessage();
                    System.exit(0);
                }

                String name=args[argindex].substring(0,eq);
                String value=args[argindex].substring(eq+1);
                props.put(name,value);
                //System.out.println("["+name+"]=["+value+"]");
                argindex++;
            }
            else
            {
                System.out.println("Unknown option "+args[argindex]);
                usageMessage();
                System.exit(0);
            }

            if (argindex>=args.length)
            {
                break;
            }
        }

        if (argindex!=args.length)
        {
            usageMessage();
            System.exit(0);
        }

        System.out.println("Database Transactions Type : "+ props.getProperty("transaction.type","TWOPL"));
        TransactionFactory.type type = TransactionTypeFactory.getType(props.getProperty("transaction.type","TWOPL"));
        Environment.setTransactionype(type);

        ServerDB();
    }
}
