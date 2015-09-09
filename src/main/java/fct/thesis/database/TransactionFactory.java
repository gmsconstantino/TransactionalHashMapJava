package fct.thesis.database;

/**
 * Created by gomes on 05/03/15.
 */
public class TransactionFactory {

    public enum type { TWOPL, OCC, OCCNA, OCCLL, OCCRDIAS, OCC_MULTI, SI, NMSI, NMSI_ARRAY, NMSI_TIMEOUT}

    public static TransactionAbst createTransaction(type t, Database db){
        switch (t){
            case TWOPL:
                return new fct.thesis.database2PL.Transaction(db);
            case OCC:
                return new fct.thesis.databaseOCC.Transaction(db);
            case OCCNA:
                return new fct.thesis.databaseOCCNoAbort.Transaction(db);
            case OCCLL:
                return new fct.thesis.databaseOCCLongLock.Transaction(db);
            case OCCRDIAS:
                return new fct.thesis.databaseOCCRDIAS.Transaction(db);
            case OCC_MULTI:
                return new fct.thesis.databaseOCCMulti.Transaction(db);
            case SI:
                return new fct.thesis.databaseSI.Transaction(db);
            case NMSI:
                return new fct.thesis.databaseNMSI.Transaction(db);
            case NMSI_ARRAY:
                return new fct.thesis.databaseNMSI_Array.Transaction(db);
            case NMSI_TIMEOUT:
                return new fct.thesis.databaseNMSI_Timeout.Transaction<>(db);
            default:
                return null;
        }
    }

}
