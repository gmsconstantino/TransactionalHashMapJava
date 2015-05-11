package bench.tpcc;

import bench.tpcc.schema.District;
import bench.tpcc.schema.Stock;
import bench.tpcc.schema.Warehouse;
import fct.thesis.database.Transaction;
import org.apache.commons.cli.*;

import java.util.Random;

/**
 * Created by gomes on 11/05/15.
 */
public class tpccLoad {

    static int do_autocommit = 0;
    static int count_ware = 0;
    static boolean option_debug = false;

    static Random random;

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption(Option.builder("w")
                .hasArg(true)
                .desc("number of warehouses - usually 10. Change to control the size of the database")
                .required()
                .build());
        options.addOption("d", false, "debug - produces more output");
        options.addOption("t", true, "databases transactional algorithm");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse( options, args);
        } catch (ParseException e) {
            System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
            // automatically generate the help statement
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "tpccLoad", options );
            System.exit(0);
        }

        if(cmd.hasOption("w")) {
            count_ware = Integer.valueOf(cmd.getOptionValue("w"));
            if (count_ware <= 0){
                System.out.println("Invalid Warehouse Count.");
                System.exit(-1);
            }
        }
        if (cmd.hasOption("d")){
            option_debug = true;
        }

        random = new Random(0); //TODO: Remover a seed estatica

        long t_clock = System.currentTimeMillis();

        System.out.println("TPCC Data Load Started...");
        EnvDB.getInstance();
//        LoadItems();
        LoadWare();
//        LoadCust();
//        LoadOrd();
    }

    /*
     * Loads the Warehouse table
     * Loads Stock, District as Warehouses are created
     */
    private static void LoadWare() {

        System.out.println("Loading Warehouse");
        for (int w_id = 0; w_id < count_ware; w_id++) {

            Warehouse ware = new Warehouse();
            /* Generate Warehouse Data */
            ware.w_name = MakeAlphaString(6, 10);
            ware.w_street_1 = MakeAlphaString(10,20);
            ware.w_street_2 = MakeAlphaString(10,20);
            ware.w_city = MakeAlphaString(10,20);
            ware.w_state = MakeAlphaString(2,2);
            ware.w_zip = MakeAlphaString(9,9);
            ware.w_tax = ((double)random1(10,20))/100;
            ware.w_ytd = 3000000.00;

            String wareKey = Warehouse.getPrimaryKey(w_id);

            if (option_debug){
                System.out.println("WID="+wareKey+", Name="+ware.w_name+", Tax="+ware.w_tax);
            }
            
            Transaction t = EnvDB.getInstance().db_warehouse.newTransaction(EnvDB.getTransactionType());
            t.put(wareKey, ware);
            t.commit();
            
            LoadStock(w_id);
            LoadDistrict(w_id);
        }
    }

    private static void LoadStock(int w_id) {
        System.out.println("Loading Stock="+w_id);

        long orig[] = new long[TpccConstants.MAXITEMS];
        int pos;
        for (int i = 0; i < TpccConstants.MAXITEMS/10; i++) {
            do{
                pos = random1(0,TpccConstants.MAXITEMS);
            } while (orig[pos]==1);
            orig[pos] = 1;
        }

        for (int s_i_id = 1; s_i_id <= TpccConstants.MAXITEMS; s_i_id++) {

            Stock s = new Stock();
        }
    }

    private static void LoadDistrict(int w_id) {
        System.out.println("Loading District");

        for (int d_id = 0; d_id < TpccConstants.DIST_PER_WARE; d_id++) {
            District d = new District();
            String d_key = District.getPrimaryKey(w_id, d_id);

            d.d_name = MakeAlphaString(6, 10);
            d.d_street_1 = MakeAlphaString(10,20);
            d.d_street_2 = MakeAlphaString(10,20);
            d.d_city = MakeAlphaString(10,20);
            d.d_state = MakeAlphaString(2,2);
            d.d_zip = MakeAlphaString(9,9);
            d.d_tax = ((double)random1(10,20))/100;

            Transaction t = EnvDB.getInstance().db_district.newTransaction(EnvDB.getTransactionType());
            t.put(d_key, d);
            t.commit();

            if (option_debug){
                System.out.println("DID="+d_key+", WID="+w_id+", Name="+d.d_name+", Tax="+d.d_tax);
            }
        }

    }

    private static String MakeAlphaString(int min, int max) {
        String str = new String();
        final String character = "abcedfghijklmnopqrstuvwxyz";
        int length;
        int i;

        length = random1(min, max);

        for (i = 0; i < length; i++) {
            str += character.charAt(random1(0, character.length() - 2));
        }

        return str;
    }

    private static int random1(int min, int max){
        return (min==max)?min:random.nextInt(max-min)+min;
    }

}
