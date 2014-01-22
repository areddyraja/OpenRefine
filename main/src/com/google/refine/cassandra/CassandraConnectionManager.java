package com.google.refine.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


public class CassandraConnectionManager {
    private static Cluster cluster;
    
    static{
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    }
    private CassandraConnectionManager()
    {
        
    }
  
    public static Session getConnetion()
    {
        
        return cluster.connect();
    }
    
    public static void shutDown()
    {
        cluster.shutdown();
    }
    
    
        
}
