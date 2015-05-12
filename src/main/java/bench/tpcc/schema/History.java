package bench.tpcc.schema;

/**
 * Created by gomes on 12/05/15.
 */
public class History {


    public static String getPrimaryKey(History h) {
        return h.h_c_id+"_"+h.h_c_d_id+"_"+h.h_c_w_id+"_"+h.h_d_id+"_"+h.h_w_id+"_"+h.h_date+"_"+h.h_amount+"_"+h.h_data;
    }

    public int h_c_id;
    public int h_c_d_id;
    public int h_c_w_id;
    public int h_d_id;
    public int h_w_id;
    public String h_date;
    public double h_amount;
    public String h_data;

}
