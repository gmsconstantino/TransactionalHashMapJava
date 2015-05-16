package bench.tpcc;

import thrift.tpcc.schema.History;

/**
 * Created by Constantino Gomes on 12/05/15.
 */
public class KeysUtils {

    public static String WarehousePrimaryKey(int w_id) {
        return "warehousePK_" + w_id;
    }

    public static String StockPrimaryKey(int s_w_id, int s_i_id) {
        return "stockPK_"+s_w_id + "_" + s_i_id;
    }

    public static String DistrictPrimaryKey(int d_w_id, int d_id) {
        return "districtPK_"+d_id + "_" + d_w_id;
    }

    public static String CustomerPrimaryKey(int c_w_id, int c_d_id, int c_id) {
        return "customerPK_"+c_w_id + "_" + c_d_id + "_" + c_id;
    }

    public static String CustomerSecundaryKey(int c_w_id, int c_d_id, String c_last) {
        return "customerSK_"+c_w_id + "_" + c_d_id + "_" + c_last;
    }

    public static String HistoryPrimaryKey(int h_c_id, int h_c_d_id, int h_c_w_id, int h_d_id, int h_w_id,
                                           String h_date, double h_amount, String h_data){
        return "historyPK_"+h_c_id + "_" + h_c_d_id + "_" + h_c_w_id + "_" + h_d_id + "_" + h_w_id + "_" + h_date + "_" + h_amount + "_" + h_data;
    }

    public static String HistoryPrimaryKey(History h) {
        return "historyPK_"+h.h_c_id + "_" + h.h_c_d_id + "_" + h.h_c_w_id + "_" + h.h_d_id + "_" + h.h_w_id + "_" + h.h_date + "_" + h.h_amount + "_" + h.h_data;
    }

    public static String ItemPrimaryKey(int i_id) {
        return "itemPK_" + i_id;
    }

    public static String NewOrderPrimaryKey(int no_w_id, int no_d_id, int no_o_id){
        return "neworderPK_"+no_w_id+"_"+no_d_id+"_"+no_o_id;
    }

    public static String NewOrderSecundaryKey(int no_w_id, int no_d_id){
        return "neworderSK_"+no_w_id+"_"+no_d_id;
    }

    public static String OrderPrimaryKey(int o_w_id, int o_d_id, int o_id){
        return "orderPK_"+o_w_id+"_"+o_d_id+"_"+o_id;
    }

    public static String OrderSecundaryKey(int o_w_id, int o_d_id, int o_c_id){
        return "orderSK_"+o_w_id+"_"+o_d_id+"_"+o_c_id;
    }

    public static String OrderLinePrimaryKey(int ol_w_id, int ol_d_id, int ol_o_id, int ol_number){
        return "orderlinePK_"+ol_w_id+"_"+ol_d_id+"_"+ol_o_id+"_"+ol_number;
    }


}
