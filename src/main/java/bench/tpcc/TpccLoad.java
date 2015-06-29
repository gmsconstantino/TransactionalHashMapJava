package bench.tpcc;

import fct.thesis.database.Transaction;
import fct.thesis.database.TransactionFactory;
import fct.thesis.database.TransactionTypeFactory;
import thrift.tpcc.schema.*;

import java.text.DecimalFormat;
import java.util.*;

import static bench.tpcc.KeysUtils.*;
import static bench.tpcc.Util.makeAlphaString;
import static bench.tpcc.Util.randomNumber;

/**
 * Created by Constantino Gomes on 11/05/15.
 */
public class TpccLoad {

    static int numWare = 0;
    static boolean option_debug = false;
    static boolean verbose = false;
    static Random random;

    public static final int ITEM_TABLE = 0;

    private static void parseArguments(String[] args){
        //parse arguments
        int argindex=0;

        if (args.length==0)
        {
            usageMessage();
            System.exit(0);
        }

        TransactionFactory.type type = null;

        while (args[argindex].startsWith("-"))
        {
            if (args[argindex].compareTo("-w")==0)
            {
                argindex++;
                if (argindex>=args.length)
                {
                    usageMessage();
                    System.exit(0);
                }
                numWare =Integer.parseInt(args[argindex]);
                argindex++;
            }
            else if (args[argindex].compareTo("-tp")==0)
            {
                argindex++;
                if (argindex>=args.length)
                {
                    usageMessage();
                    System.exit(0);
                }
                type = TransactionTypeFactory.getType(args[argindex]);
                argindex++;
            }
            else if (args[argindex].compareTo("-d")==0)
            {
                if (argindex>=args.length)
                {
                    usageMessage();
                    System.exit(0);
                }
                option_debug = true;
                argindex++;
            }
            else if (args[argindex].compareTo("-r")==0)
            {
                if (argindex>=args.length)
                {
                    usageMessage();
                    System.exit(0);
                }
                Environment.remote = true;
                argindex++;
            }
            else if (args[argindex].compareTo("-v")==0)
            {
                if (argindex>=args.length)
                {
                    usageMessage();
                    System.exit(0);
                }
                verbose = true;
                argindex++;
            }
            else
            {
                System.out.println("Unknown option "+args[argindex]);
                usageMessage();
                System.exit(0);
            }

            if (argindex>=args.length)
            {
                break;
            }
        }

        if(type!=null && numWare != -1) {
            Environment.setTransactiontype(type, numWare+1); //One more because of items table
        } else {
            usageMessage();
            System.exit(0);
        }

    }

    private static void usageMessage() {
    }


    public static void main(String[] args) {
        parseArguments(args);

        Environment.getInstance();
        tpccLoadData(numWare);
    }

    public static void tpccLoadData(int cnt_warehouses){
        System.out.println("TPCC Data Load Started...");
        long start = System.currentTimeMillis();
        random = new Random();
        numWare = cnt_warehouses;
        LoadItems();
        LoadWare();
        LoadCust();
        LoadOrd();

        DecimalFormat df = new DecimalFormat("#,##0.00");
        System.out.println("Load time (s): "+ df.format((System.currentTimeMillis()-start)/1000));
    }

    /*
     * Loads the Orders and Order_Line Tables
     */
    private static void LoadOrd() {

        for (int w_id = 1; w_id <= numWare; w_id++) {
            for (int d_id = 1; d_id <= TpccConstants.DIST_PER_WARE; d_id++) {

                if(verbose)
                    System.out.printf("Loading Orders for D=%d, W= %d\n", d_id, w_id);

                int o_d_id = d_id;
                int o_w_id = w_id;
                Util.initPermutation();    /* initialize permutation of customer numbers */
                for (int o_id = 1; o_id <= TpccConstants.ORD_PER_DIST; o_id++) {

                    Transaction<String, MyObject> t = Environment.newTransaction();

                    String d_key = KeysUtils.DistrictPrimaryKey(w_id, d_id);
                    District district = t.get(w_id, d_key).deepCopy().getDistrict();
                    district.d_next_o_id++;
                    t.put(w_id, d_key, MyObject.district(district));

                    /* Generate Order Data */
                    Order o = new Order();
                    o.o_c_id = Util.getPermutation();
                    o.o_carrier_id = Util.randomNumber(1, 10);
                    o.o_ol_cnt = Util.randomNumber(5, 15);

                    Date date = new java.sql.Date(System.currentTimeMillis());
                    o.o_entry_d = date.toString();

                    if (o_id > 2100) {    /* the last 900 orders have not been delivered */
                        o.o_carrier_id = 0;

                        t.put(w_id, OrderPrimaryKey(o_w_id, o_d_id, o_id), MyObject.order(o));

                        /* Put order on Secondary index */
                        String o_key_sec = OrderSecundaryKey(o_w_id, o_d_id, o.o_c_id);
                        t.put(w_id, o_key_sec, MyObject.Integer(o_id));

                        /* Put new Order */
                        t.put(w_id, NewOrderPrimaryKey(o_w_id, o_d_id, o_id), MyObject.NULL(true));
                        if (o_id==2101){
                            // This is the oldest undelivered order data of that district.
                            t.put(w_id, NewOrderSecundaryKey(o_w_id, o_d_id), MyObject.Integer(2101));
                        }
                    } else {

                        t.put(w_id, OrderPrimaryKey(o_w_id, o_d_id, o_id), MyObject.order(o));

                        /* Put order on Secondary index */
                        String o_key_sec = OrderSecundaryKey(o_w_id, o_d_id, o.o_c_id);
                        t.put(w_id, o_key_sec, MyObject.Integer(o_id));
                    }

                    if (option_debug)
                        System.out.printf("OID = %d, CID = %d, DID = %d, WID = %d\n",
                                o_id, o.o_c_id, o_d_id, o_w_id);

                    for (int ol_i = 1; ol_i <= o.o_ol_cnt; ol_i++) {
                        /* Generate Order Line Data */
                        OrderLine ol = new OrderLine();
                        ol.ol_i_id = Util.randomNumber(1, TpccConstants.MAXITEMS);
                        ol.ol_supply_w_id = o_w_id;
                        ol.ol_quantity = 5;
                        ol.ol_amount = 0.0;

                        ol.ol_dist_info = Util.makeAlphaString(24, 24);

                        if (o_id > 2100) {
                            t.put(w_id, OrderLinePrimaryKey(o_w_id, o_d_id, o_id, ol_i), MyObject.orderline(ol));
                        } else {

                            ol.ol_amount = ((float) (Util.randomNumber(10, 10000)) / 100.0);
                            ol.ol_delivery_d = date.toString();

                            t.put(w_id, OrderLinePrimaryKey(o_w_id, o_d_id, o_id, ol_i), MyObject.orderline(ol));
                        }

                        if (option_debug)
                            System.out.printf("OL = %d, IID = %d, QUAN = %d, AMT = %8.2f\n",
                                    ol_i, ol.ol_i_id, ol.ol_quantity, ol.ol_amount);
                    }


                    if ((o_id % 100) == 0 && verbose)
                    {
                        System.out.print(".");

                        if ((o_id % 1000) == 0) {
                            System.out.printf(" %d\n", o_id);
                        }
                    }

                    t.commit();
                }
            }

            System.out.println("Orders Done.");
        }
    }

    /*
     * Loads the Customer Table
     */
    private static void LoadCust() {

        Transaction t = Environment.newTransaction();
        for (int w_id = 1; w_id <= numWare; w_id++) {
            for (int d_id = 1; d_id <= TpccConstants.DIST_PER_WARE; d_id++) {
                if(verbose)
                    System.out.printf("Loading Customer for DID=%d, WID=%d\n", d_id, w_id);

                HashMap<String, Integer> countLastname = new HashMap<>();
                for (int c_id = 1; c_id <= TpccConstants.CUST_PER_DIST; c_id++) {
                    Customer c = new Customer();
                    String c_key = CustomerPrimaryKey(w_id, d_id, c_id);

                    c.c_first = Util.makeAlphaString(8, 16);
                    c.c_middle = "OE";

                    if (c_id <= 1000) {
                        c.c_last = Util.lastName(c_id - 1);
                    } else {
                        c.c_last = Util.lastName(Util.nuRand(255, 0, 999));
                    }

                    if (countLastname.containsKey(c.c_last)) {
                        int n = countLastname.get(c.c_last) + 1;
                        countLastname.put(c.c_last, n);
                    } else
                        countLastname.put(c.c_last, 1);

                    c.c_street_1 = Util.makeAlphaString(10, 20);
                    c.c_street_2 = Util.makeAlphaString(10, 20);
                    c.c_city = Util.makeAlphaString(10, 20);
                    c.c_state = Util.makeAlphaString(2, 2);
                    c.c_zip = Util.makeAlphaString(9, 9);

                    c.c_phone = Util.makeNumberString(16, 16);

                    if (Util.randomNumber(0, 1) == 1)
                        c.c_credit = "G";
                    else
                        c.c_credit = "B";
                    c.c_credit += "C";

                    c.c_credit_lim = 50000;
                    c.c_discount = (float) (((float) Util.randomNumber(0, 50)) / 100.0);
                    c.c_balance = (float) -10.0;

                    c.c_data = Util.makeAlphaString(300, 500);

                    t.put(w_id, c_key, MyObject.customer(c));

                    History h = new History();
                    h.h_c_id = c_id;
                    h.h_c_d_id = d_id;
                    h.h_c_w_id = w_id;
                    h.h_d_id = d_id;
                    h.h_w_id = w_id;
                    h.h_amount = 10.0;
                    h.h_data = Util.makeAlphaString(12, 24);

                    Calendar calendar = Calendar.getInstance();
                    Date date = new java.sql.Date(calendar.getTimeInMillis());
                    h.h_date = date.toString();

                    String h_key = HistoryPrimaryKey(h);
                    t.put(w_id, h_key, MyObject.history(h));

                    if (option_debug) {
                        System.out.printf("CID = %d, LST = %s, P# = %s\n",
                                c_id, c.c_last, c.c_phone);
                    }
                    if ((c_id % 100) == 0 && verbose) {
                        System.out.print(".");
                        if ((c_id % 1000) == 0)
                            System.out.printf(" %d\n", c_id);
                    }
                }

                // Set the primary key to use when search by lastname
                for (Map.Entry<String,Integer> e : countLastname.entrySet()){
                    t.put(w_id, CustomerSecundaryKey(w_id, d_id, e.getKey()), MyObject.Integer(e.getValue()));
                }
            }
        }
        t.commit();

        System.out.println("Customer Done.");
    }

    /*
     * Loads the Item table
     */
    private static void LoadItems() {

        int[] orig = new int[TpccConstants.MAXITEMS + 1];
        int pos = 0;
        int i = 0;

        if(verbose)
            System.out.printf("Loading Item \n");

        for (i = 0; i < TpccConstants.MAXITEMS / 10; i++) {
            do {
                pos = Util.randomNumber(0, TpccConstants.MAXITEMS-1);
            } while (orig[pos] != 0);
            orig[pos] = 1;
        }

        Transaction t = Environment.newTransaction();
        for (int i_id = 1; i_id <= TpccConstants.MAXITEMS; i_id++) {

            /* Generate Item Data */
            Item it = new Item();
            String it_key = ItemPrimaryKey(i_id);
            it.i_im_id = Util.randomNumber(1, 10000);

            it.i_name = Util.makeAlphaString(14, 24);
            it.i_price = ((double) (Util.randomNumber(100, 10000)) / 100.0);

            it.i_data = Util.makeAlphaString(26, 50);
            if (orig[i_id] != 0) {
                pos = Util.randomNumber(0, it.i_data.length() - 8);
                it.i_data = it.i_data.substring(0, pos) + "original" + it.i_data.substring(pos + 8);
            }

            if(option_debug)
                System.out.printf("IID = %d, Name= %s, Price = %.2f\n",
                          i_id, it.i_name, it.i_price);

            t.put(ITEM_TABLE, it_key, MyObject.item(it));

            if ((i_id % 100) == 0 && verbose) {
                System.out.printf(".");
                if ((i_id % 5000) == 0)
                    System.out.printf(" %d\n", i_id);
            }
        }
        t.commit();

        System.out.printf("Item Done. \n");
    }

    /*
     * Loads the Warehouse table
     * Loads Stock, District as Warehouses are created
     */
    private static void LoadWare() {
        if(verbose)
            System.out.println("Loading Warehouse");

        for (int w_id = 1; w_id <= numWare; w_id++) {

            Warehouse ware = new Warehouse();
            /* Generate Warehouse Data */
            ware.w_name = makeAlphaString(6, 10);
            ware.w_street_1 = makeAlphaString(10, 20);
            ware.w_street_2 = makeAlphaString(10, 20);
            ware.w_city = makeAlphaString(10, 20);
            ware.w_state = makeAlphaString(2, 2);
            ware.w_zip = makeAlphaString(9, 9);
            ware.w_tax = ((double)randomNumber(10, 20))/100;
            ware.w_ytd = 3000000.00;

            String wareKey = WarehousePrimaryKey(w_id);

            if (option_debug){
                System.out.println("WID="+wareKey+", Name="+ware.w_name+", Tax="+ware.w_tax);
            }

            Transaction t = Environment.newTransaction();
            t.put(w_id, wareKey, MyObject.warehouse(ware));
            t.commit();
            
            LoadStock(w_id);
            LoadDistrict(w_id);
        }
        System.out.printf("Warehouse Done. \n");
    }

    private static void LoadStock(int w_id) {
        if(verbose)
            System.out.println("Loading Stock="+w_id);

        long orig[] = new long[TpccConstants.MAXITEMS];
        int pos;
        for (int i = 0; i < TpccConstants.MAXITEMS/10; i++) {
            do{
                pos = randomNumber(0, TpccConstants.MAXITEMS-1);
            } while (orig[pos]==1);
            orig[pos] = 1;
        }

        Transaction t = Environment.newTransaction();
        for (int s_i_id = 1; s_i_id <= TpccConstants.MAXITEMS; s_i_id++) {

            Stock s = new Stock();
            String s_key = StockPrimaryKey(w_id, s_i_id);

            /* Generate Stock Data */
            s.s_quantity = Util.randomNumber(10, 100);
            s.s_dist_01 = Util.makeAlphaString(24, 24);
            s.s_dist_02 = Util.makeAlphaString(24, 24);
            s.s_dist_03 = Util.makeAlphaString(24, 24);
            s.s_dist_04 = Util.makeAlphaString(24, 24);
            s.s_dist_05 = Util.makeAlphaString(24, 24);
            s.s_dist_06 = Util.makeAlphaString(24, 24);
            s.s_dist_07 = Util.makeAlphaString(24, 24);
            s.s_dist_08 = Util.makeAlphaString(24, 24);
            s.s_dist_09 = Util.makeAlphaString(24, 24);
            s.s_dist_10 = Util.makeAlphaString(24, 24);
            s.s_ytd = 0;
            s.s_order_cnt = 0;
            s.s_remote_cnt = 0;

            s.s_data = Util.makeAlphaString(26, 50);
            int sdatasize = s.s_data.length();
            if (orig[s_i_id-1] != 0) {
                pos = Util.randomNumber(0, sdatasize - 8);
                s.s_data = s.s_data.substring(0, pos)+"original"+s.s_data.substring(pos+8);
            }

            t.put(w_id, s_key, MyObject.stock(s));

            if (option_debug){
                System.out.println("SID=" + s_i_id + ", WID=" + w_id + ", Quan=" + s.s_quantity);
            }
            if (s_i_id % 100 == 0 && verbose) {
                System.out.print(".");
                if (s_i_id % 5000 ==0)
                    System.out.println(s_i_id);
            }

        }
        t.commit();
        System.out.printf("Stock Done. \n");
    }

    private static void LoadDistrict(int w_id) {
        if(verbose)
            System.out.println("Loading District");

        Transaction t = Environment.newTransaction();
        for (int d_id = 1; d_id <= TpccConstants.DIST_PER_WARE; d_id++) {
            District d = new District();
            String d_key = DistrictPrimaryKey(w_id, d_id);

            d.d_name = makeAlphaString(6, 10);
            d.d_street_1 = makeAlphaString(10, 20);
            d.d_street_2 = makeAlphaString(10, 20);
            d.d_city = makeAlphaString(10, 20);
            d.d_state = makeAlphaString(2, 2);
            d.d_zip = makeAlphaString(9, 9);
            d.d_tax = ((double)randomNumber(10,20))/100;

            t.put(w_id, d_key, MyObject.district(d));

            if (option_debug){
                System.out.println("DID="+d_key+", WID="+w_id+", Name="+d.d_name+", Tax="+d.d_tax);
            }
        }
        t.commit();
        System.out.printf("District Done. \n");
    }

}
