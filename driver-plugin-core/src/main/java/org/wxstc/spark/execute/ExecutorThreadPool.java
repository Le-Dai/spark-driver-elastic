package org.wxstc.spark.execute;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public enum ExecutorThreadPool {
    INSTANCE;

    private ExecutorService executorService = Executors.newCachedThreadPool();

    public void executeJob(Runnable runnable){
        executorService.execute(runnable);
    }
}
