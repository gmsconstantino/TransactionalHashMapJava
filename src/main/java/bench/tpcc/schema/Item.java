package bench.tpcc.schema;

/**
 * Created by gomes on 12/05/15.
 */
public class Item {

    public static String getPrimaryKey(int i_id){
        return ""+i_id;
    }

    public int i_im_id;
    public String i_name;
    public double i_price;
    public String i_data;
}
