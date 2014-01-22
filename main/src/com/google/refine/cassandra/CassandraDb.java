
package com.google.refine.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * A simple client application that illustrates connecting to a Cassandra
 * cluster. retrieving metadata, creating a schema, loading data into it, and
 * then querying it.
 */
public class CassandraDb {

    private static Session session;

    public CassandraDb() {
    }

    
   
    /**
     * Connects to the specified node.
     * 
     * @param node
     *            a host name or IP address of the node in the cluster
     */
    public Session connect(String node) {
        Cluster cluster = Cluster.builder().addContactPoint(node)
        // .withSSL() // uncomment if using client to node encryption
                .build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        return cluster.connect();
    }

    public void createSchema() {
        session.execute("CREATE KEYSPACE googlerefine WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':3};");
        // create songs and playlist tables
        session.execute("CREATE TABLE googlerefine.data (" + "exchange text," + "stock_symbol text," + "date text,"
                + "stock_price_open text," + "stock_price_high text," + "stock_price_low text"
                + "stock_price_close text" + "stock_volume text" + "stock_price_adj_close text"
                + "stock_price_adj_close text" + ");");
        session.execute("CREATE TABLE simplex.playlists (" + "id uuid," + "title text," + "album text, "
                + "artist text," + "song_id uuid," + "PRIMARY KEY (id, title, album, artist)" + ");");
        System.out.println("Simplex keyspace and schema created.");
    }

    public ResultSet querySchema() {
        ResultSet results = session.execute("SELECT * FROM googlerefine.data;");
        System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "exchange", "stock_symbol", "stock_volume",
                "-------------------------------+-----------------------+--------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("exchange"),
                    row.getString("stock_symbol"), row.getString("stock_volume")));
        }
        System.out.println();

        return results;
    }

    /**
     * Updates the songs table with a new song and then queries the table to
     * retrieve data.
     */
    public void updateSchema() {
        session.execute("UPDATE simplex.songs " + "SET tags = tags + { 'entre-deux-guerres' } "
                + "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");

        ResultSet results = session.execute("SELECT * FROM simplex.songs "
                + "WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");

        System.out
                .println(String
                        .format("%-30s\t%-20s\t%-20s%-30s\n%s", "title", "album", "artist", "tags",
                                "-------------------------------+-----------------------+--------------------+-------------------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"), row.getString("album"),
                    row.getString("artist"), row.getSet("tags", String.class)));
        }
    }

    public void dropSchema(String keyspace) {
        getSession().execute("DROP KEYSPACE " + keyspace);
    }

    public Session getSession() {
        return this.session;
    }

    void setSession(Session session) {
        this.session = session;
    }

    public void close() {
        session.shutdown();
        session.getCluster().shutdown();
    }

    public void cassandraConnect() {
        CassandraDb client = new CassandraDb();
        client.connect("127.0.0.1");
        // client.createSchema();
        // client.loadData();
        // return client.querySchema();
        // client.updateSchema();
        // client.dropSchema("simplex");

    }
}
