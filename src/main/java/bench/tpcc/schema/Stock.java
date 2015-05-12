package bench.tpcc.schema;

/**
 * Created by gomes on 12/05/15.
 */
public class Stock {

    public static String getPrimaryKey(int s_w_id, int s_i_id){
        return ""+s_w_id+"_"+s_i_id;
    }

    public int s_quantity;
    public String s_dist_01;
    public String s_dist_02;
    public String s_dist_03;
    public String s_dist_04;
    public String s_dist_05;
    public String s_dist_06;
    public String s_dist_07;
    public String s_dist_08;
    public String s_dist_09;
    public String s_dist_10;
    public long s_ytd;
    public int s_order_cnt;
    public int s_remote_cnt;
    public String s_data;

}
