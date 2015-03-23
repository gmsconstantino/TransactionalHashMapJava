package database;

import databaseOCC.Transaction;

/**
 * Created by gomes on 05/03/15.
 */
public class TransactionFactory {

    public enum type { TWOPL, OCC, OCC_MULTI }

    public static database.Transaction createTransaction(type t, Database db){
        switch (t){
            case TWOPL:
                return new database2PL.Transaction(db);
            case OCC:
                return new databaseOCC.Transaction(db);
            case OCC_MULTI:
                return new databaseOCCMulti.Transaction(db);
            default:
                return null;
        }
    }

}
