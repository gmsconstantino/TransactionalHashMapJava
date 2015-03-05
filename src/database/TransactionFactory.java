package database;

/**
 * Created by gomes on 05/03/15.
 */
public class TransactionFactory {

    public enum type { TWOPL, OCC }

    public static Transaction getFactory(type t, Database db){
        switch (t){
            case TWOPL:
                return new Transaction2PL(db);
            case OCC:
                return new TransactionOCC(db);
            default:
                return null;
        }
    }

}
