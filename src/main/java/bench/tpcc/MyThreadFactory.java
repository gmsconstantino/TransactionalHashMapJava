package bench.tpcc;


import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.AffinityStrategy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * This is a Mock ThreadFactory, doing nothing than assign a new name to the threads
 *
 * @author constantino gomes
 */

import java.util.concurrent.ThreadFactory;

public class MyThreadFactory implements ThreadFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyThreadFactory.class);

    private final String name;
    private final boolean daemon;
    private int id = 1;

    public MyThreadFactory(String name, AffinityStrategy... strategies) {
        this(name, true, strategies);
    }

    public MyThreadFactory(String name, boolean daemon, @NotNull AffinityStrategy... strategies) {
        this.name = name;
        this.daemon = daemon;
    }

    @NotNull
    @Override
    public synchronized Thread newThread(@NotNull final Runnable r) {
        String name2 = id <= 1 ? name : (name + '-' + id);
        id++;
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                    r.run();
            }
        }, name2);
        t.setDaemon(daemon);
        return t;
    }
}
