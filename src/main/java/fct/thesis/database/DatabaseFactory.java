package fct.thesis.database;

/**
 * Created by gomes on 14/04/15.
 */
public class DatabaseFactory {

    public static <K,V> fct.thesis.database.Database createDatabase(TransactionFactory.type t){
        switch (t){
            case NMSI:
                return new fct.thesis.databaseBlotter.Database<K,V>();
            default:
                return new fct.thesis.database.Database<K,V>();
        }
    }

}
