package thrift;


import fct.thesis.database.TransactionFactory;

/**
 * Created by gomes on 26/03/15.
 */
public class TransactionTypeFactory {

    public static TransactionFactory.type getType(String value){
        if (value == null) {
            return null;
        }

        if (value.toUpperCase().equals("TWOPL")){
            return TransactionFactory.type.TWOPL;
        } else if (value.toUpperCase().equals("OCC")){
            return TransactionFactory.type.OCC;
        } else if (value.toUpperCase().equals("OCCNA")){
            return TransactionFactory.type.OCCNA;
        } else if (value.toUpperCase().equals("OCCLL")){
            return TransactionFactory.type.OCCLL;
        } else if (value.toUpperCase().equals("OCCMV")) {
            return TransactionFactory.type.OCC_MULTI;
        } else if(value.toUpperCase().equals("SI")) {
            return TransactionFactory.type.SI;
        } else if(value.toUpperCase().equals("BLOTTER")) {
            return TransactionFactory.type.BLOTTER;
        } else
            return null;
    }

}
