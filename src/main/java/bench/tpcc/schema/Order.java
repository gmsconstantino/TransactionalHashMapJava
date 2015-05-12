package bench.tpcc.schema;

/**
 * Created by gomes on 12/05/15.
 */
public class Order {

    public static String newOrderKey(int no_o_id, int no_d_id, int no_w_id){
        return no_w_id+"_"+no_d_id+"_"+no_o_id;
    }

    public static String getPrimaryKey(int o_w_id, int o_d_id, int o_id){
        return o_w_id+"_"+o_d_id+"_"+o_id;
    }

    public static String getSecundaryKey(int o_w_id, int o_d_id, int o_c_id){
        return ""+o_w_id+"_"+o_d_id+"_"+o_c_id;
    }

    public int o_c_id;
    public String o_entry_d;
    public int o_carrier_id;
    public long o_ol_cnt;
    public String o_all_local;

}
