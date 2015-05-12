namespace java thrift.tpcc.schema  // defines the namespace

typedef i32 int  //typedefs to get convenient names for your types
typedef i64 long

struct Customer {
     1:string c_first;
     2:string c_middle;
     3:string c_last;
     4:string c_street_1;
     5:string c_street_2;
     6:string c_city;
     7:string c_state;
     8:string c_zip;
     9:string c_phone;
     10:string c_since;
     11:string c_credit;
     12:double c_credit_lim;
     13:double c_discount;
     14:double c_balance;
     15:long c_ytd_payment;
     16:int c_payment_cnt;
     17:int c_delivery_cnt;
     18:string c_data;
}

struct District {
    1:string d_name;
    2:string d_street_1;
    3:string d_street_2;
    4:string d_city;
    5:string d_state;
    6:string d_zip;
    7:double d_tax;
    8:long d_ytd;
    9:int d_next_o_id;
}

struct History {
    1:int h_c_id;
    2:int h_c_d_id;
    3:int h_c_w_id;
    4:int h_d_id;
    5:int h_w_id;
    6:string h_date;
    7:double h_amount;
    8:string h_data;
}

struct Item {
    1:int i_im_id;
    2:string i_name;
    3:double i_price;
    4:string i_data;
}

struct Order {
    1:int o_c_id;
    2:string o_entry_d;
    3:int o_carrier_id;
    4:long o_ol_cnt;
    5:string o_all_local;
}

struct OrderLine {
    1:int ol_i_id;
    2:int ol_supply_w_id;
    3:string ol_delivery_d;
    4:long ol_quantity;
    5:double ol_amount;
    6:string ol_dist_info;
}

struct Stock {
    1:int s_quantity;
    2:string s_dist_01;
    3:string s_dist_02;
    4:string s_dist_03;
    5:string s_dist_04;
    6:string s_dist_05;
    7:string s_dist_06;
    8:string s_dist_07;
    9:string s_dist_08;
    10:string s_dist_09;
    11:string s_dist_10;
    12:long s_ytd;
    13:int s_order_cnt;
    14:int s_remote_cnt;
    15:string s_data;
}

struct Warehouse {
    1:string w_name;
    2:string w_street_1;
    3:string w_street_2;
    4:string w_city;
    5:string w_state;
    6:string w_zip;
    7:double w_tax;
    8:double w_ytd;
}

union MyObject {
    1:Warehouse warehouse
    2:Stock stock
    3:OrderLine orderline
    4:Order order
    5:Item item
    6:History history
    7:District district
    8:Customer customer
}