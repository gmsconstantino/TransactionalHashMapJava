package bench.tpcc.schema;

/**
 * Created by gomes on 12/05/15.
 */
public class OrderLine {

    public static String getPrimaryKey(int ol_w_id, int ol_d_id, int ol_o_id, int ol_number){
        return ""+ol_w_id+"_"+ol_d_id+"_"+ol_o_id+"_"+ol_number;
    }

    public int ol_i_id;
    public int ol_supply_w_id;
    public String ol_delivery_d;
    public long ol_quantity;
    public double ol_amount;
    public String ol_dist_info;

}
