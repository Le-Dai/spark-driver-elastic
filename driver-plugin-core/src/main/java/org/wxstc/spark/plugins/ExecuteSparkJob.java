package org.wxstc.spark.plugins;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ExecuteSparkJob extends Remote {
    void run(SparkJobRunnable runnable) throws RemoteException;
}
