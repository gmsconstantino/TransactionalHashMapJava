import database.Database;

import java.io.ByteArrayOutputStream;

/**
 * Created by gomes on 10/03/15.
 */
public class TestCLI {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    Database<Integer,Integer> db;

    @org.junit.Before
    public void setUp() throws Exception {
        db = new Database<Integer, Integer>();
    }

    @org.junit.After
    public void tearDown() throws Exception {

    }

    @org.junit.Test
    public void testAutomaticCli() throws Exception {
//        baos
    }

}
