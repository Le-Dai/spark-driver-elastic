package org.wxstc.spark.plugins;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public abstract class SparkJob implements SparkJobRunnable{
    public SparkSession spark = null;
    public Map<String, Dataset> gobalTables= null;

    public void initEnv(SparkSession session, Map<String, Dataset> gobalTables){
        this.gobalTables = gobalTables;
        this.spark = session;
    }
}
