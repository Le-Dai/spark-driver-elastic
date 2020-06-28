package org.wxstc.spark.plugins;

import org.apache.spark.sql.SparkSession;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * driver rmi execute impl
 */
public class DriverExecuteSparkImpl extends UnicastRemoteObject implements ExecuteSparkJob {
    SparkSession spark;
    public DriverExecuteSparkImpl(SparkSession spark) throws RemoteException {
        this.spark = spark;
    }

    @Override
    public void run(SparkJobRunnable runnable) throws RemoteException {
        runnable.initEnv(spark, null);
        runnable.run();
        System.out.println("driver execute");
    }
}
