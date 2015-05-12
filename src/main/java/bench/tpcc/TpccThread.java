package bench.tpcc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class TpccThread extends Thread {

    int number;
    int num_ware;
    int num_conn;


    public TpccThread(int number, int num_ware, int num_conn) {

        this.number = number;
        this.num_conn = num_conn;
        this.num_ware = num_ware;

    }

    public void run() {



    }

}

