package fct.thesis.database;

/**
 * Created by Constantino Gomes on 17/07/15.
 */
public class ThreadCleaner <K,V> extends Thread {

    public volatile boolean stop = false;

}
