package bench.tpcc.schema;

/**
 * Created by gomes on 11/05/15.
 */
public class Warehouse {

    private static final String NULL = "";

    public static String getPrimaryKey(int w_id){
        return ""+w_id;
    }

    public String w_name;
    public String w_street_1;
    public String w_street_2;
    public String w_city;
    public String w_state;
    public String w_zip;
    public double w_tax;
    public double w_ytd;

    @Override
    public String toString() {
        return "Warehouse@"+hashCode()+"{" +
                "w_name='" + w_name + '\'' +
                ", w_street_1='" + w_street_1 + '\'' +
                ", w_street_2='" + w_street_2 + '\'' +
                ", w_city='" + w_city + '\'' +
                ", w_state='" + w_state + '\'' +
                ", w_zip='" + w_zip + '\'' +
                ", w_tax=" + w_tax +
                ", w_ytd=" + w_ytd +
                '}';
    }
}
