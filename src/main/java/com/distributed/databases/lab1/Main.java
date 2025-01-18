package com.distributed.databases.lab1;

import com.distributed.databases.lab1.model.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.distributed.databases.lab1.CounterTester.createDB;
import static com.distributed.databases.lab1.CounterTester.getFinalCounter;

/**
 * @author Oleksandr Havrylenko
 **/
public class Main {
    public static final int RUN_10_THREADS = 10;
    private final static Logger logger = LoggerFactory.getLogger(Main.class);
    public static final boolean DEFAULT_ISOLATION_LEVEL = false;
    public static final boolean SERIALIZABLE_ISOLATION_LEVEL = true;
    public static final int UP_TO_10_000_COUNTER = 10_000;

    public static void main(String[] args) {
        createDB();

        long start = System.nanoTime();

        logger.info("Test 1 Lost-Update with default Transaction Isolation level");
//        logger.info("Test 1 Lost-Update with SERIALIZABLE Transaction Isolation level");
        testDatabaseCounter(RUN_10_THREADS, () -> CounterTester.test1(UP_TO_10_000_COUNTER, DEFAULT_ISOLATION_LEVEL));
//        logger.info("Test 2 In-Place Update with default Transaction Isolation level");
//        testDatabaseCounter(RUN_10_THREADS, () -> CounterTester.test2(UP_TO_10_000_COUNTER));
//        logger.info("Test 3 Row-Level locking with default Transaction Isolation level");
//        testDatabaseCounter(RUN_10_THREADS, () -> CounterTester.test3(UP_TO_10_000_COUNTER));
//        logger.info("Test 4 Optimistic concurrency control with default Transaction Isolation level");
//        testDatabaseCounter(RUN_10_THREADS, () -> CounterTester.test4(UP_TO_10_000_COUNTER));

        long finish = System.nanoTime();
        logger.info("Duration: {} ms", (finish - start) / 1_000_000.0);
        Result finalCounter = getFinalCounter();
        logger.info("Final result counter = {}, version = {};", finalCounter.counter(), finalCounter.version());

    }

    private static void testDatabaseCounter(final int threadsNum, Runnable task) {
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadsNum; i++) {
            Thread thread = new Thread(task);
            thread.start();
            threads.add(thread);
        }

        threads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for thread completion.", e);
            }
        });
    }
}
