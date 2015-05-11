package bench.tpcc.schema;

/**
 * Created by gomes on 11/05/15.
 */
public class District {

    public static String getPrimaryKey(int d_w_id, int d_id){
        return ""+d_id+"_"+d_w_id;
    }
    public class PK {
        public int d_w_id;
        public int d_id;
    }

    public String d_name;
    public String d_street_1;
    public String d_street_2;
    public String d_city;
    public String d_state;
    public String d_zip;
    public double d_tax;
    public long d_ytd;
    public int d_next_o_id;
}
