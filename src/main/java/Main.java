import bindings.DctStorage;
import pt.dct.cli.CLI;
import pt.dct.cli.ClientThread;
import pt.dct.cli.TxCmd;
import pt.dct.cli.TxStorage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by gomes on 02/03/15.
 */
public class Main {

//    public static void main(String[] args) {
//
//        CLI.run(new DctStorage());
//
//    }

    public static final String PROMPT_SYMB = "> ";
    public static final String EXIT_CMD = "exit";
    public static final String START_THREAD_CMD = "start";
    public static final String SWITCH_TO_CMD = "switch";
    public static final String STATUS_CMD = "status";
    public static final String THREAD_CMD = "thread";
    public static final String BEGIN_CMD = "begin";
    public static final String COMMIT_CMD = "commit";
    public static final String READ_CMD = "read";
    public static final String WRITE_CMD = "write";


    public static String readLine() {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            return br.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static boolean finish = false;
    public static Map<String,ClientThread> clientThreads = new HashMap<String,ClientThread>();
    public static ClientThread currentThread = null;

    public static TxStorage storage = new DctStorage();

    public static void main(String[] args) {

        storage.init();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            while(!finish) {
                System.out.print(PROMPT_SYMB);
                String line = br.readLine();
                if (line == null) {
                    continue;
                }

                String[] tokens = line.split("\\s");

                execCommand(tokens);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Terminating... Done");
    }

    public static void execCommand(String[] tokens) {
        if (tokens[0].toLowerCase().equals(EXIT_CMD)){
            exitCmd();
        } else if (tokens[0].toLowerCase().equals(START_THREAD_CMD)){
            startThreadCmd(tokens);
        } else if (tokens[0].toLowerCase().equals(SWITCH_TO_CMD)){
            switchToCmd(tokens);
        } else if (tokens[0].toLowerCase().equals(STATUS_CMD)){
            statusCmd();
        } else if (tokens[0].toLowerCase().equals(THREAD_CMD)){
            threadCmd(tokens);
        } else if (tokens[0].toLowerCase().equals(BEGIN_CMD)){
            beginCmd();
        } else if (tokens[0].toLowerCase().equals(COMMIT_CMD)){
            commitCmd();
        } else if (tokens[0].toLowerCase().equals(READ_CMD)){
            readCmd(tokens);
        } else if (tokens[0].toLowerCase().equals(WRITE_CMD)){
            writeCmd(tokens);
        }
    }


    public static void exitCmd() {
        for (ClientThread ct : clientThreads.values()) {
            ct.finish();
            try {
                ct.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        finish = true;
    }

    public static void startThreadCmd(String[] params) {
        if (params.length < 2) {
            System.out.println("usage: start \"key\"");
            return;
        }
        String key = params[1];
        if (clientThreads.containsKey(key)) {
            System.out.println("error: client thread \""+key+"\" already exists");
            return;
        }

        ClientThread cthread = new ClientThread(key, storage);
        clientThreads.put(key, cthread);
        currentThread = cthread;
        cthread.start();

        System.out.println("# client thread \""+key+"\" started");
    }

    public static void switchToCmd(String[] params) {
        if (params.length < 2) {
            System.out.println("usage: switch \"key\"");
            return;
        }

        String key = params[1];
        if (!clientThreads.containsKey(key)) {
            System.out.println("error: client thread \""+key+"\" does not exist");
            return;
        }

        currentThread = clientThreads.get(key);

        System.out.println("# current thread is \""+key+"\"");
    }

    public static void statusCmd() {
        if (currentThread == null) {
            System.out.println("# no thread created");
        }
        System.out.println("# current thread is \""+currentThread.getName()+"\"");
    }

    public static void threadCmd(String[] params) {
        if (params.length < 3) {
            System.out.println("usage: thread \"key\" \"cmd\"");
            return;
        }

        String key = params[1];
        if (!clientThreads.containsKey(key)) {
            System.out.println("error: client thread \""+key+"\" does not exist");
            return;
        }

        ClientThread temp = currentThread;
        currentThread = clientThreads.get(key);
        execCommand(Arrays.copyOfRange(params, 2, params.length));
        currentThread = temp;
    }

    public static void beginCmd() {
        if (currentThread == null) {
            System.out.println("error: you must create a thread first");
            return;
        }

        currentThread.submitCmd(new TxCmd.BeginCmd());
    }

    public static void commitCmd() {
        if (currentThread == null) {
            System.out.println("error: you must create a thread first");
            return;
        }

        currentThread.submitCmd(new TxCmd.CommitCmd());
    }

    public static void readCmd(String[] tokens) {
        if (currentThread == null) {
            System.out.println("error: you must create a thread first");
            return;
        }

        if (tokens.length < 2) {
            System.out.println("usage: read \"key\"");
            return;
        }

        currentThread.submitCmd(new TxCmd.ReadCmd(tokens));
    }

    public static void writeCmd(String[] tokens) {
        if (currentThread == null) {
            System.out.println("error: you must create a thread first");
            return;
        }

        if (tokens.length < 3) {
            System.out.println("usage: write \"key\" \"value\"");
            return;
        }

        currentThread.submitCmd(new TxCmd.WriteCmd(tokens));
    }



}
