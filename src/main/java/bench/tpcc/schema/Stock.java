package bench.tpcc.schema;

/**
 * Created by gomes on 12/05/15.
 */
public class Stock {

    public static String getPrimaryKey(int s_w_id, int s_i_id){
        return ""+s_w_id+"_"+s_i_id;
    }



}
