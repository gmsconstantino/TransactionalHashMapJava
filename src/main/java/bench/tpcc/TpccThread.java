package bench.tpcc;

import fct.thesis.database.Transaction;
import fct.thesis.database.TransactionException;
import net.openhft.affinity.Affinity;
import net.openhft.affinity.AffinityLock;
import thrift.tpcc.schema.*;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static bench.tpcc.KeysUtils.*;
import static bench.tpcc.TpccConstants.*;

public class TpccThread implements Runnable {

    public static final int P_MIX = 45;
    public static final int NO_MIX = 43;
    public static final int OS_MIX = 4;
    public static final int D_MIX  = 4;
    public static final int SL_MIX = 4;

    /* definitions for new order transaction */
    public static final int MAX_NUM_ITEMS = 15;
    public static final int MAX_ITEM_LEN = 24;

    public static final int ITEM_TABLE = 0;
    public static int NOTFOUND = TpccConstants.MAXITEMS + 1;

    public volatile boolean start = false;

    int n_worker;
    int num_warehouses;
    int use_ware;

    int count = 0;
    int commits = 0;
    int aborts = 0;
    double latency = 0;

    int th_w_id = -1;

    boolean shouldload = false;
    boolean loaded = false;
    
    int core;
    private int d_id;
    private int w_id;
    private int o_id;
    private int i;
    private int ol_id;
    private int o_c_id;
    private int o_carrier_id;
    private long begintime;
    private long endtime;
    private int c_id;
    private int byname;
    private int c_w_id;
    private int c_d_id;

    MyObject object = null;
    private NewOrderItemInfo order_data[] = new NewOrderItemInfo[15];

    //So para conseguir perceber onde deu erro
    private static int indexErr = 0;

    public TpccThread(int n_worker, int use_ware, int num_warehouses, boolean bindWarehouse, int core, boolean shouldload) {

        this.n_worker = n_worker;
        this.num_warehouses = num_warehouses;
        this.use_ware = use_ware;

        this.shouldload = shouldload;

        this.core = core;

        if (bindWarehouse) {
            th_w_id = use_ware;
        }

        for (int i = 0; i < 15; i++) {
            order_data[i] = new NewOrderItemInfo();
        }
    }

    public void run() {
        TpccEmbeded.init_signal.incrementAndGet();

        if (shouldload){
            TpccLoad.LoadWare(use_ware);
            TpccLoad.LoadCust(use_ware);
            TpccLoad.LoadOrd(use_ware);
        }

        TpccEmbeded.signal.getAndIncrement();

        while (!start);

        while (!TpccEmbeded.stop) {
            doNextTransaction();
            count++;
        }

    }

    private void doNextTransaction() {
        int r = ThreadLocalRandom.current().nextInt(100);
        try {
            if (r <= P_MIX) { // 45%
                doPayment();
            } else if (r <= P_MIX + NO_MIX) { // 88%
                doNewOrder();
            } else if (r <= P_MIX + NO_MIX + OS_MIX) { // 92%
                doOrdstat();
            } else if (r <= P_MIX + NO_MIX + OS_MIX + D_MIX) { // 96%
                doDelivery();
            } else {
                doSlev();
            }
            commits++;
        } catch (TransactionException e){
            aborts++;
        }
    }

    private void doSlev() {
        i = 0;
        o_id = 0;
        w_id = 0;
        d_id = 0;
        int threshold = 0;

        if (th_w_id != -1)
            w_id = th_w_id;
        else
            w_id = Util.randomNumber(1, num_warehouses);

        d_id = Util.randomNumber(1, DIST_PER_WARE);
        threshold = Util.randomNumber(10, 20);

        Set<Integer> set = new HashSet<>();

        begintime = System.currentTimeMillis();

        Transaction<String, MyObject> t = Environment.newTransaction();

        /* The row in the DISTRICT table with matching D_W_ID and D_ID
         * is selected and D_NEXT_O_OID is retrieved.
         */
        String d_key = DistrictPrimaryKey(w_id, d_id);
        object = t.get(w_id, d_key);
        if (object == null){
            t.abort();
            throw new TransactionException("Some error "+(indexErr++));
        }
        District d_data = object.deepCopy().getDistrict();

        /* All rows in the ORDER_LINE table with matching OL_W_ID (equals W_ID),
         * OL_D_ID (equals D_ID), and OL_O_ID (lower than D_NEXT_O_ID and greater
         * than or equal to D_NEXT_O_ID minus 20) are selected. They are the items
         * for 20 recent orders of the district.
         */
        int init = (d_data.d_next_o_id-20) < 0 ? 0 : d_data.d_next_o_id-20;
        for (o_id = init; o_id < d_data.d_next_o_id; o_id++) {
            String o_key = OrderPrimaryKey(w_id,d_id,o_id);
            object = t.get(w_id, o_key);
            if(object == null){
                t.abort();
//                System.out.println("Slev order key: "+o_key);
                throw new TransactionException("Order Line not found.");
            }
            Order o_data = object.deepCopy().getOrder();

            for (i=1; i <= o_data.o_ol_cnt; i++){
                String ol_key = OrderLinePrimaryKey(w_id,d_id,o_id, i);
                object = t.get(w_id, ol_key);
                if(object == null){
                    t.abort();
                    throw new TransactionException("Order Line not found.");
                }
                OrderLine ol_data = object.deepCopy().getOrderline();
                set.add(ol_data.ol_i_id);
            }
        }

        /* All rows in the STOCK table with matching S_I_ID (equals OL_I_ID) and S_W_ID
         * (equals W_ID) from the list of distinct item numbers and with S_QUANTITY lower than
         * threshold are counted (giving low_stock).
         */
        int count = 0;
        for (Integer i_id : set){
            String s_key = StockPrimaryKey(w_id, i_id);
            object = t.get(w_id, s_key);
            if (object == null){
                t.abort();
                throw new TransactionException("Some error "+(indexErr++));
            }
            Stock s_data = object.deepCopy().getStock();

            if(s_data.s_quantity < threshold)
                count++;
        }

        t.commit();

        endtime = System.currentTimeMillis();
        latency = latency==0? endtime-begintime : (latency+(endtime-begintime))/2;
    }

    /*
      * execute delivery transaction
      */
    private void doDelivery() {
        ol_id = 0;
        d_id = 0;
        o_c_id = 0;
        w_id = 0;
        o_carrier_id = 0;

        if (th_w_id != -1)
            w_id = th_w_id;
        else
            w_id = Util.randomNumber(1, num_warehouses);
        o_carrier_id = Util.randomNumber(1, 10);

        //Timestamp
        java.sql.Timestamp time = new Timestamp(System.currentTimeMillis());
        String timeStamp = time.toString();

        begintime = System.currentTimeMillis();

        Transaction<String, MyObject> t = Environment.newTransaction();

        /* For a given warehouse number (W_ID), for each of the 10 districts
         * (D_W_ID, D_ID) within that warehouse, and for a given carrier number
         * O_CARRIER_ID:
         */
        for (d_id=1; d_id <= DIST_PER_WARE; d_id++){

            /* The row int the NEW-ORDER table with matching NO_W_ID (equals W_ID),
             * and NO_D_ID (equals D_ID) and with the lowest NO_O_ID value is
             * selected. This is the oldest undelivered order data of that district.
             */
            String no_key_sec = NewOrderSecundaryKey(w_id, d_id);
            object = t.get(w_id, no_key_sec);
            if (object == null){
                t.abort();
                throw new TransactionException("Some error "+(indexErr++));
            }
            Integer min_o_id = object.deepCopy().getInteger();

            // Should delete this object from database,
            // the primary key of new order to the last order not delivery
            String no_key = NewOrderPrimaryKey(w_id,d_id,min_o_id);

            /* The row in the ORDER table with matching O_W_ID (equals W_ID), O_D_ID (equals D_ID),
             * and O_ID (equals NO_O_ID) is selected. O_C_ID, the customer number, is retrieved, and
             * O_CARRIER_ID is updated.
             */
            String o_key = OrderPrimaryKey(w_id,d_id, min_o_id);
            object = t.get(w_id, o_key);
            if (object == null){
                t.abort();
                throw new TransactionException("Some error "+(indexErr++));
            }
            Order o_data = object.deepCopy().getOrder();

            o_c_id = o_data.o_c_id;
            o_data.o_carrier_id = o_carrier_id;

            t.put(w_id, o_key, MyObject.order(o_data));

            /* All rows in ORDER-LINE table with matching OL_W_ID (equals
             * O_W_ID), OL_D_ID, * equals (O_D_ID), and OL_O_ID (equals O_ID)
             * are selected. All OL_DELIVERY_D, * the delivery dates, are
             * updated to the current system time as returned by the *
             * operating system and the sum OL_AMOUNT is retrieved.  */
            int sum_ol_amount = 0;
            for (ol_id = 1; ol_id <= o_data.o_ol_cnt; ol_id++){
                String ol_key = OrderLinePrimaryKey(w_id,d_id,min_o_id, ol_id);
                object = t.get(w_id, ol_key);
                if (object == null){
                    t.abort();
                    throw new TransactionException("Some error "+(indexErr++));
                }
                OrderLine ol_data = object.deepCopy().getOrderline();

                sum_ol_amount += ol_data.ol_amount;

                ol_data.ol_delivery_d = timeStamp;
                t.put(w_id, ol_key, MyObject.orderline(ol_data));
            }

            /* The row in the customer table with matching C_W_ID (equals W_ID), C_D_ID
             * (equals (D_ID), and C_ID (equals O_C_ID) is selected and C_BALANCE is increased
             * by the sum of all order-line amounts (OL_AMOUNT) previously retrieved.
             * C_DELIVERY_CNT is incremented by 1.
             */
            String c_key = CustomerPrimaryKey(w_id,d_id,o_c_id);
            object = t.get(w_id, c_key);
            if (object == null){
                t.abort();
                throw new TransactionException("Some error "+(indexErr++));
            }
            Customer c_data = object.deepCopy().getCustomer(); //TODO: deu null pointer

            c_data.c_balance += sum_ol_amount;
            c_data.c_delivery_cnt++;
            t.put(w_id, c_key, MyObject.customer(c_data));

            t.put(w_id, no_key_sec, MyObject.Integer(min_o_id++));
        }

        t.commit();

        endtime = System.currentTimeMillis();
        latency = latency==0? endtime-begintime : (latency+(endtime-begintime))/2;
    }

    /*
      * prepare data and execute order status transaction
      */
    private void doOrdstat() {
        i = 0;
        byname = 0;
        w_id = 0;
        d_id = 0;
        c_id = 0;
        String c_last = null;

        if (th_w_id != -1)
            w_id = th_w_id;
        else
            w_id = Util.randomNumber(1, num_warehouses);

        d_id = Util.randomNumber(1, DIST_PER_WARE);
        c_id = Util.nuRand(1023, 1, CUST_PER_DIST);
        c_last = Util.lastName(Util.nuRand(255, 0, 999));
        if (Util.randomNumber(1, 100) <= 60) {
            byname = 1; /* select by last name */
        } else {
            byname = 0; /* select by customer id */
        }

        OrderStatusInfo order_data[] = new OrderStatusInfo[MAX_NUM_ITEMS];

        /*
         * Begining the Transaction
         */
        long begintime = System.currentTimeMillis();
        int ret = ProcessOrderStatus(byname, w_id, d_id, c_id, c_last, order_data);
        long endtime = System.currentTimeMillis();
        latency = latency==0? endtime-begintime : (latency+(endtime-begintime))/2;
    }

    private int ProcessOrderStatus(int byname, int w_id, int d_id, int c_id, String c_last, OrderStatusInfo[] order_data) {
        i = 0;

        Transaction<String,MyObject> t = Environment.newTransaction();

        if (byname >= 0) {
            // Case 2: the customer is selected based on customer last name
            String c_key_sec = CustomerSecundaryKey(w_id, d_id, c_last);
            object = t.get(w_id, c_key_sec);
            if (object == null){
                t.abort();
                throw new TransactionException("Some error "+(indexErr++));
            }
            Integer count = object.deepCopy().getInteger();
            c_id = count / 2 + 1;
        }

        /* Case 1: the customer is selected based on customer number:
         * the row in the customer table with matching C_W_ID, C_D_ID
         * and C_ID is selected. C_BALANCE, C_FIRST, C_MIDDLE and C_LAST
         * are retrieved.
         */
        String c_key = CustomerPrimaryKey(w_id,d_id,c_id);
        object = t.get(w_id, c_key);
        if (object == null){
            t.abort();
            throw new TransactionException("Some error "+(indexErr++));
        }
        Customer c_data = object.deepCopy().getCustomer();

        String c_first = c_data.c_first;
        String c_middle = c_data.c_middle;
        c_last = c_data.c_last;
        double c_balance = c_data.c_balance;


        /* The row in the ORDER table with matching O_W_ID (equals C_W_ID),
         * O_D_ID (equals C_D_ID), O_C_ID (equals C_ID), and with the largest
         * existing O_ID is selected. This is the most recent order placed by
         * that customer. O_ID, O_ENTRY_D, and O_CARRIER_ID are retrieved.
         */
        String o_key_sec = OrderSecundaryKey(w_id,d_id,c_id);
        object = t.get(w_id, o_key_sec);
        if (object == null){
            t.abort();
            throw new TransactionException("Some error "+(indexErr++));
        }
        Integer o_id = object.deepCopy().getInteger();

        // How many orderlines exists?
        String o_key = OrderPrimaryKey(w_id,d_id,o_id);
        object = t.get(w_id, o_key);
        if (object == null){
            t.abort();
            throw new TransactionException("Some error "+(indexErr++));
        }
        Order o_data = object.deepCopy().getOrder();

        String o_entry_d = o_data.o_entry_d;
        int o_carrier_id = o_data.o_carrier_id;
        long o_ol_cnt = o_data.o_ol_cnt;

        /* All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID),
         * OL_D_ID (equals O_D_ID), and OL_O_ID (equals O_ID) are selected and the
         * corresponding sets of OL_I_ID, OL_SUPPLY_W_ID,
         * OL_QUANTITY, OL_AMOUNT, and OL_DELIVERY_D are retrieved.
         */
        for (i=0; i < o_ol_cnt; i++) {
            order_data[i] = new OrderStatusInfo();

            String ol_key = OrderLinePrimaryKey(w_id, d_id, o_id, i + 1);
            object = t.get(w_id, ol_key);
            if (object == null){
                t.abort();
                System.out.println("OrderStatus order line key: "+ol_key);
                throw new TransactionException("Order Line not found.");
            }
            OrderLine ol_data = object.deepCopy().getOrderline();

            order_data[i].ol_supply_w_id = ol_data.ol_supply_w_id;
            order_data[i].ol_i_id = ol_data.ol_i_id;
            order_data[i].ol_quantity = ol_data.ol_quantity;
            order_data[i].ol_amount = ol_data.ol_amount;
            order_data[i].ol_delivery_d = ol_data.ol_delivery_d;
        }

        t.commit();
        return 1;
    }

    private void doPayment() {
        byname = 0;

        w_id = 0;
        d_id = 0;
        c_w_id = 0;
        c_d_id = 0;
        c_id = 0;
        int h_amount = 0;
        String c_last = null;

        if (th_w_id != -1)
            w_id = th_w_id;
        else
            w_id = Util.randomNumber(1, num_warehouses);

        d_id = Util.randomNumber(1, DIST_PER_WARE);
        c_id = Util.nuRand(1023, 1, CUST_PER_DIST);
        c_last = Util.lastName(Util.nuRand(255, 0, 999));
        h_amount = Util.randomNumber(1, 5000);
        if (Util.randomNumber(1, 100) <= 60) {
            byname = 1; /* select by last name */
        } else {
            byname = 0; /* select by customer id */
        }

        if (Util.randomNumber(1, 100) <= 85 || num_warehouses ==1) {
            c_w_id = w_id;
            c_d_id = d_id;
        } else {
            c_w_id = otherWare(w_id);
            c_d_id = Util.randomNumber(1, DIST_PER_WARE);
        }

        //Timestamp
        java.sql.Timestamp time = new Timestamp(System.currentTimeMillis());
        String timeStamp = time.toString();

        /*
         * Begining the Transaction
         */
        long begintime = System.currentTimeMillis();
        int ret = ProcessPayment(h_amount, c_last, timeStamp);
        long endtime = System.currentTimeMillis();
        latency = latency==0? endtime-begintime : (latency+(endtime-begintime))/2;
    }

    private int ProcessPayment(int h_amount, String c_last, String timeStamp) {
        Transaction<String,MyObject> t = Environment.newTransaction();

        /* The row in the WAREHOUSE table with matching W_ID is selected.
         * W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, and W_ZIP are
         * retrieved and W_YTD, the warehouse's year-to-date balance is
         * increased by H_AMOUNT.
         */
        String w_key = WarehousePrimaryKey(w_id);
        object = t.get(w_id, w_key);
        if (object == null){
            t.abort();
            throw new TransactionException("Some error "+(indexErr++));
        }
        Warehouse w_data = object.deepCopy().getWarehouse();

        String w_name = w_data.w_name;
        String w_street_1 = w_data.w_street_1;
        String w_street_2 = w_data.w_street_2;
        String w_city = w_data.w_city;
        String w_state = w_data.w_state;
        String w_zip = w_data.w_zip;

        w_data.w_ytd += h_amount;

        t.put(w_id, w_key, MyObject.warehouse(w_data));

        /* The row in the DISTRICT table with matching D_W_ID and D_ID is selected.
         * D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are retrieved
         * and D_YTD, the district's year-to-date balance, is increased by H_AMOUNT
         */
        String d_key = KeysUtils.DistrictPrimaryKey(w_id, d_id);
        object = t.get(w_id, d_key);
        if (object == null){
            t.abort();
            throw new TransactionException("Some error "+(indexErr++));
        }
        District d_data = object.deepCopy().getDistrict();

        String d_name = d_data.d_name;
        String d_street_1 = d_data.d_street_1;
        String d_street_2 = d_data.d_street_2;
        String d_city = d_data.d_city;
        String d_state = d_data.d_state;
        String d_zip = d_data.d_zip;

        d_data.d_ytd += h_amount;

        t.put(w_id, d_key, MyObject.district(d_data));

        if (byname >= 1) {
            /* Case 2: the customer is selected based on customer last name
             * C_BALANCE is decreased by H_AMOUNT. C_YTD_PAYMENT is increased
             * by H_AMOUNT. C_PAYMENT_CNT is incremented by 1.
             */

            String c_key_sec = CustomerSecundaryKey(c_w_id, c_d_id, c_last);
            object = t.get(c_w_id, c_key_sec);
            if (object == null){
                t.abort();
                throw new TransactionException("Some error "+(indexErr++));
            }
            Integer count = object.deepCopy().getInteger();
            c_id = count / 2 + 1;
        }

        /* Case 1: the customer is selected based on customer number:
         * the row in the customer table with matching C_W_ID, C_D_ID
         * and C_ID is selected. C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
         * C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT,
         * C_CREDIT_LIM, C_DISCOUNT, and C_BALANCE are retrieved.
         * C_BALANCE is decreased by H_AMOUNT. C_YTD_PAYMENT is increased
         * by H_AMOUNT. C_PAYMENT_CNT is incremented by 1.
         */
        String c_key = CustomerPrimaryKey(c_w_id,c_d_id,c_id);
        object = t.get(c_w_id, c_key);
        if (object == null){
            t.abort();
            throw new TransactionException("Some error "+(indexErr++));
        }
        Customer c_data = object.deepCopy().getCustomer();

        String c_first = c_data.c_first;
        String c_middle = c_data.c_middle;
        c_last = c_data.c_last;
        String c_street_1 = c_data.c_street_1;
        String c_street_2 = c_data.c_street_2;
        String c_city = c_data.c_city;
        String c_state = c_data.c_state;
        String c_zip = c_data.c_zip;
        String c_phone = c_data.c_phone;
        String c_since = c_data.c_since;
        String c_credit = c_data.c_credit;

        c_data.c_balance -= h_amount;
        c_data.c_ytd_payment += h_amount;
        c_data.c_payment_cnt++;

        /* If the value of C_CREDIT is equal to "BC", then C_DATA is also
         * retrieved from the selected customer and the following history
         * information: C_ID, C_D_ID, C_W_ID, D_ID W_ID, and H_AMOUNT are
         * inserted at the left  of the C_DATA field by shifting the
         * existing content fo C_DATA to the right side of the C_DATA field.
         * The content of the C_DATA field never exceeds 500 characters.
         * The selected customer is updated with the new C_DATA field.
         */
        if (c_data.c_credit.equals("BC")){
            String c_new_data = String.format("| %d %d %d %d %d $%d %s", c_id, c_d_id, c_w_id, d_id, w_id, h_amount, timeStamp);

            int free = 500-c_data.c_data.length();
            int needspace = (c_new_data.length() - free) <= 0 ? 0 : c_new_data.length() - free;
            c_new_data = (c_new_data + c_data.c_data.substring(0, (c_data.c_data.length() - needspace)));

            c_data.c_data = c_new_data;
        }

        t.put(w_id, c_key, MyObject.customer(c_data));

        String h_data = w_name+"    "+d_name;
        String h_key = HistoryPrimaryKey(c_id, c_d_id, c_w_id, d_id, w_id, timeStamp, h_amount, h_data);
        t.put(w_id, h_key, MyObject.NULL(true));

        t.commit();
        return 1;
    }

    private void doNewOrder() {
        i = 0;
        int o_ol_cnt;

        o_ol_cnt = Util.randomNumber(5, 15);

        if (th_w_id != -1)
            w_id = th_w_id;
        else
            w_id = Util.randomNumber(1, num_warehouses);

        d_id = Util.randomNumber(1, TpccConstants.DIST_PER_WARE);
        c_id = Util.nuRand(1023, 1, TpccConstants.CUST_PER_DIST);



//        String status;
        double total = 0.0;
        int all_local = 1;

        //Timestamp
        java.sql.Timestamp time = new Timestamp(System.currentTimeMillis());
        String timeStamp = time.toString();

        /* Remember OL_DIST_INFO fields, which we retrieve in the
         * middle of transaction, but don't actually write until
         * the end. */
        String ol_dist_info[] = new String[15];

        int rbk = Util.randomNumber(1, 100);
        for (i = 0; i < o_ol_cnt; i++) {
            order_data[i] = new NewOrderItemInfo();
            order_data[i].ol_i_id = Util.nuRand(8191, 1, TpccConstants.MAXITEMS);

            /* Generate unused item */
            if ((i == o_ol_cnt - 1) && (rbk == 1)) {
                order_data[i].ol_i_id = NOTFOUND;
            }

            if (Util.randomNumber(1, 100) != 1 || num_warehouses ==1) {
                order_data[i].ol_supply_w_id = w_id;
            } else {
                /* Select warehouse other than home */
                order_data[i].ol_supply_w_id = otherWare(w_id);
                all_local = 0;
            }
            order_data[i].ol_quantity = Util.randomNumber(1, 10);
        }
        /* sort orders to avoid DeadLock */
        Arrays.sort(order_data, new Comparator<NewOrderItemInfo>() {
            @Override
            public int compare(NewOrderItemInfo o1, NewOrderItemInfo o2) {
                return Integer.compare(o1.ol_i_id,o2.ol_i_id);
            }
        });

        /*
         * Begining the Transaction
         */
        long begintime = System.currentTimeMillis();
        int ret = ProcessNewOrder(o_ol_cnt, total, all_local, timeStamp, ol_dist_info);
        long endtime = System.currentTimeMillis();
        latency = latency==0? endtime-begintime : (latency+(endtime-begintime))/2;
    }

    private int ProcessNewOrder(int o_ol_cnt, double total, int all_local, String timeStamp, String[] ol_dist_info) {
        double w_tax;
        double d_tax;
        int d_next_o_oid;
        int i;

        Transaction<String,MyObject> t = Environment.newTransaction();

        /* The row in the WAREHOUSE table with matching W_ID is selected and W_TAX
         * rate is retrieved. */
        String w_key = KeysUtils.WarehousePrimaryKey(w_id);
        object = t.get(w_id, w_key);
        if (object == null){
            t.abort();
            throw new TransactionException("Some error "+(indexErr++));
        }
        Warehouse ware = object.deepCopy().getWarehouse();
        w_tax = ware.w_tax;

        /* The row in the DISTRICT table with matching D_W_ID and D_ID is selected,
         * D_TAX, the district tax rate is retrieved, and D_NEXT_OID, the next available
         * order number for the district, is retireved and incremented by one.
         */
        String d_key = KeysUtils.DistrictPrimaryKey(w_id, d_id);
        object = t.get(w_id, d_key);
        if (object == null){
            t.abort();
            throw new TransactionException("Some error "+(indexErr++));
        }
        District district = object.deepCopy().getDistrict();
        d_tax = district.d_tax;
        d_next_o_oid = district.d_next_o_id;

        district.d_next_o_id++;
        t.put(w_id, d_key, MyObject.district(district));

        /* The row in the customer table with matching C_W_ID, C_D_ID and C_ID
             * is selected
             * and C_DISCOUNT, the customer's discount rate, C_LAST, the customer's
             * last name,
             * and C_CREDIT, the customer's credit status are retrieved.
             */
        String c_key = KeysUtils.CustomerPrimaryKey(w_id,d_id,c_id);
        object = t.get(w_id, c_key);
        if (object == null){
            t.abort();
            throw new TransactionException("Some error "+(indexErr++));
        }
        Customer customer = object.deepCopy().getCustomer();


        /* A new row is inserted into both the NEW_ORDER table and the ORDER
         * table to reflect the creation of the new order. O_CARRIER_ID is set
         * to a null value. If the order includes only home order-lines, then
         * O_ALL_LOCAL is set to 1, otherwise O_ALL_LOCAL is set to 0.
         */
        Order order = new Order();
        order.o_c_id = c_id;
        order.o_carrier_id = 0;
        order.o_entry_d = timeStamp;
        order.o_ol_cnt = o_ol_cnt;
        order.o_all_local = all_local;



        /* For each O_OL_CNT item in the order */
        for(i = 0; i < o_ol_cnt; i++) {
            /* The row in the ITEM table with matching I_ID
             * (equals OL_I_ID) is selected and I_PRICE, the price
             * of the item, I_NAME, the name of the item, and I_DATA
             * are retrieved. If I_ID has an unused value (see
             * Clause 2.4.1.5), a "not-found" condition is signaled,
             * resulting in a rollback of the database transaction.
             */
            if(order_data[i].ol_i_id == NOTFOUND){
                t.abort();
                throw new TransactionException("Item key not found.");
            }

            String i_key = KeysUtils.ItemPrimaryKey(order_data[i].ol_i_id);
            object = t.get(ITEM_TABLE, i_key);
            if (object == null){
                t.abort();
                throw new TransactionException("Some error "+(indexErr++));
            }
            Item item = object.deepCopy().getItem();

            /* The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and
             * S_W_ID (equals OL_SUPPLY_W_ID) is selected. S_QUANTITY, the quantity in stock,
             * S_DIST_xx, where xx represents the district number, and S_DATA are retrieved.
             */
            String s_key = KeysUtils.StockPrimaryKey(order_data[i].ol_supply_w_id, order_data[i].ol_i_id);
            object = t.get(order_data[i].ol_supply_w_id, s_key);
            if (object == null){
                t.abort();
                throw new TransactionException("Some error "+(indexErr++));
            }
            Stock stock = object.deepCopy().getStock();

            /* If the retrieved value for S_QUANTITY exceeds OL_QUANTITY by 10 or more, then
             * S_QUANTITY is decreased by OL_QUANTITY; otherwise S_QUANTITY is updated to
             * (S_QUANTITY - OL_QUANTITY) + 91. S_YTD is increased
             * by OL_QUANTITY and S_ORDER_CNT is incremented by 1. If the order-line is remote,
             * then S_REMOTE_CNT is incremented by 1.
             */
            if(stock.s_quantity - order_data[i].ol_quantity >= 10)
            {
                stock.s_quantity -= order_data[i].ol_quantity;
            }
            else
            {
                stock.s_quantity = (stock.s_quantity - order_data[i].ol_quantity) +91;
            }
            stock.s_ytd += order_data[i].ol_i_id;
            stock.s_order_cnt++;

            if(order_data[i].ol_supply_w_id != w_id)
            {
                stock.s_remote_cnt++;
            }

            t.put(w_id, s_key, MyObject.stock(stock));

            /* The amount for the item in the order (OL_AMOUNT) is computed as:
	         * OL_QUANTITY * I_PRICE */
            OrderLine orderLine = new OrderLine();
            orderLine.ol_amount = order_data[i].ol_quantity*item.i_price;

            total += orderLine.ol_amount;

            /* The strings I_DATA and S_DATA are examined. If they both include
             * the string "ORIGINAL", the brand-generic field for that item is
             * set to "B", otherwise, the brand-generic field is set to "G".
             */
            if(item.i_data.contains("original") && stock.s_data.contains("original"))
            {
                order_data[i].brand = 'B';
            }
            else
            {
                order_data[i].brand = 'G';
            }

            ol_dist_info[i] = stock.s_dist_01 + (d_id-1)*25;

            order_data[i].ol_supply_w_id = orderLine.ol_supply_w_id;
            order_data[i].i_name = item.i_name;
            order_data[i].ol_quantity = orderLine.ol_quantity;
            order_data[i].s_quantity = stock.s_quantity;
            order_data[i].i_price = item.i_price;
            order_data[i].ol_amount = orderLine.ol_amount;
        }

        /*************************/
        /* A new row is inserted into the ORDER_LINE table to reflect the
         * item on the order. OL_DELIVERY_D is set to a null value, OL_NUMBER
         * is set to a unique value within all the ORDER_LINE rows that have
         * the same OL_O_ID value, and OL_DIST_INFO is set to the content of
         * S_DIST_xx, where xx represents the district number OL_D_ID.
         */
        for (i = 0; i < o_ol_cnt; i++) {
            String ol_key = KeysUtils.OrderLinePrimaryKey(w_id,d_id, d_next_o_oid, i+1);
            OrderLine orderLine = new OrderLine(order_data[i].ol_i_id, order_data[i].ol_supply_w_id,
                    "", order_data[i].ol_quantity, 0, ol_dist_info[i]);
            t.put(w_id, ol_key, MyObject.orderline(orderLine));
        }

        t.put(w_id, KeysUtils.NewOrderPrimaryKey(w_id, d_id, d_next_o_oid), MyObject.NULL(true));

        t.put(w_id, KeysUtils.OrderPrimaryKey(w_id, d_id, d_next_o_oid), MyObject.order(order));

        String o_key_sec = OrderSecundaryKey(w_id, d_id, c_id);
        t.put(w_id, o_key_sec, MyObject.Integer(d_next_o_oid));

        t.commit();
        return 1;
    }

    private class NewOrderItemInfo {
        int ol_supply_w_id;
        int ol_i_id;
        String i_name;
        int ol_quantity;
        int s_quantity;
        char brand;
        double i_price;
        double ol_amount;
    }

    private class OrderStatusInfo {
        int ol_supply_w_id;
        int ol_i_id;
        int ol_quantity;
        double ol_amount;
        String ol_delivery_d;
    }


    /*
     * produce the id of a valid warehouse other than home_ware
     * (assuming there is one)
     */
    private int otherWare(int home_ware) {
        int tmp;
        while ((tmp = Util.randomNumber(1, num_warehouses)) == home_ware) ;
        return tmp;
    }

}

