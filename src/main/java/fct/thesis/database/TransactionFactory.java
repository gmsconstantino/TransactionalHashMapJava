package fct.thesis.database;

import fct.thesis.databaseOCC.Transaction;

/**
 * Created by gomes on 05/03/15.
 */
public class TransactionFactory {

    public enum type { TWOPL, OCC, OCC2, OCC_MULTI, SI, BLOTTER }

    public static fct.thesis.database.Transaction createTransaction(type t, Database db){
        switch (t){
            case TWOPL:
                return new fct.thesis.database2PL.Transaction(db);
            case OCC:
                return new fct.thesis.databaseOCC.Transaction(db);
//            case OCC2:
//                return new fct.thesis.databaseOCC2.Transaction(db);
            case OCC_MULTI:
                return new fct.thesis.databaseOCCMulti.Transaction(db);
            case SI:
                return new fct.thesis.databaseSI.Transaction(db);
            case BLOTTER:
                return new fct.thesis.databaseBlotter.Transaction(db);
            default:
                return null;
        }
    }

}
