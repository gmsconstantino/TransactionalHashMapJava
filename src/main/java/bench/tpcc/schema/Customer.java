package bench.tpcc.schema;

/**
 * Created by gomes on 12/05/15.
 */
public class Customer {

    public static String getPrimaryKey(int c_w_id, int c_d_id, int c_id){
        return ""+c_w_id+"_"+c_d_id+"_"+c_id;
    }

    public static String getSecundaryKey(int c_w_id, int c_d_id, String c_last){
        return ""+c_w_id+"_"+c_d_id+"_"+c_last;
    }

    public String c_first;
    public String c_middle;
    public String c_last;
    public String c_street_1;
    public String c_street_2;
    public String c_city;
    public String c_state;
    public String c_zip;
    public String c_phone;
    public String c_since;
    public String c_credit;
    public double c_credit_lim;
    public double c_discount;
    public double c_balance;
    public long c_ytd_payment;
    public int c_payment_cnt;
    public int c_delivery_cnt;
    public String c_data;

}
