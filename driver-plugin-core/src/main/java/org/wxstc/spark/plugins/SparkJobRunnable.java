package org.wxstc.spark.plugins;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

public interface SparkJobRunnable extends Remote, Serializable{

    void run() throws RemoteException;

    default void initEnv(SparkSession session, Map<String, Dataset> gobalTables){

    }
}
