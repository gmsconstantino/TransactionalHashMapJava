package fct.thesis.databaseBlotter;

import fct.thesis.database.ObjectDb;
import fct.thesis.database.Transaction;
import fct.thesis.database.TransactionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by gomes on 26/02/15.
 */
public class Database<K,V> extends fct.thesis.database.Database{


    public Database(Properties properties){
        this();
    }

    public Database(){
        super();
    }


    public void cleanup(){
    }

}
