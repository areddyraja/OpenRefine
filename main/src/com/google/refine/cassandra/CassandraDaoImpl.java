
package com.google.refine.cassandra;

import java.util.Iterator;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.refine.ProjectMetadata;
import com.google.refine.model.Project;

public class CassandraDaoImpl {

    final Logger logger = LoggerFactory.getLogger("project_metadata");

    public void saveMetadata(ProjectMetadata metaData, long projectId)
            throws Exception {

        Session session = null;
        try {

            session = CassandraConnectionManager.getConnetion();

            // Add keyspace and table column to Project..
            PreparedStatement pstmt = session
                    .prepare("insert into metadata_keyspace.metadata_tbl(project_id,name,created,modified,custommetadata,password,encoding,encodingconfidence,preferences,keyspacename,tablename) values(?,?,'now','now',?,?,?,?,?,?,?)");

            BoundStatement boundStatement = new BoundStatement(pstmt);
            session.execute(boundStatement.bind(projectId, metaData.getName(),
                    CassandraUtil.getCustomMetaData(metaData), metaData.getPassword(), metaData.getEncoding(),
                    metaData.getEncodingConfidence(), CassandraUtil.getPreferences(metaData),
                    metaData.getkeyspaceName(), metaData.getTableName()));
            Map<String, String> customMetaData = CassandraUtil.getCustomMetaData(metaData);
            for (String str : customMetaData.keySet()) {
                logger.info(str + "::" + customMetaData.get(str));
            }
            logger.info("password:" + metaData.getPassword());
            logger.info("encoding:" + metaData.getEncoding());
            logger.info("encodingconfidence:" + metaData.getEncodingConfidence());
            Map<String, String> preferences = CassandraUtil.getPreferences(metaData);
            for (String str : preferences.keySet()) {
                logger.info(str + "::" + preferences.get(str));
            }

        } finally {
            if (session != null) session.shutdown();
        }

    }

    public JSONObject loadMetaData(long projectId)
            throws JSONException {

        Session session = null;
        try {

            session = CassandraConnectionManager.getConnetion();

            PreparedStatement pstmt = session
                    .prepare("select * from metadata_keyspace.metadata_tbl where project_id=?");

            BoundStatement boundStatement = new BoundStatement(pstmt);
            ResultSet resultSet = session.execute(boundStatement.bind(projectId));
            Iterator<Row> iterator = resultSet.iterator();
            JSONObject jsonObject = new JSONObject();
            if (iterator.hasNext()) {
                Row row = iterator.next();

                logger.info("name:" + row.getString("name"));
                Map<String, String> customMetaData = row.getMap("custommetadata", String.class, String.class);
                for (String str : customMetaData.keySet()) {
                    logger.info(str + "::" + customMetaData.get(str));
                }
                logger.info("password:" + row.getString("password"));
                logger.info("encoding:" + row.getString("encoding"));
                logger.info("encodingconfidence:" + row.getInt("encodingconfidence"));

                // logger.info("pre"+"::"+row.getString("preferences"));

                jsonObject.put("name", row.getString("name"));
                jsonObject.put("created", row.getDate("created"));
                jsonObject.put("modified", row.getDate("modified"));
                jsonObject.put("keyspaceName", row.getString("keyspacename"));
                jsonObject.put("tableName", row.getString("tablename"));
                jsonObject.put("custommetadata", row.getMap("custommetadata", String.class, String.class));
                jsonObject.put("password", row.getString("password"));
                jsonObject.put("encoding", row.getString("encoding"));
                jsonObject.put("encodingConfidence", row.getInt("encodingconfidence"));
                jsonObject.put("preferences", row.getMap("preferences", String.class, String.class));
            }

            return jsonObject;

        } finally {
            if (session != null) session.shutdown();
        }

    }

    public void saveProject(Project project)
            throws Exception {

        Session session = null;
        try {

            session = CassandraConnectionManager.getConnetion();

            PreparedStatement pstmt = session
                    .prepare("insert into metadata_keyspace.metadata_tbl(project_id,name,created,modified,custommetadata,password,encoding,encoding_confidence,preferences) values(?,,?,'now','now',?,?,?,?,?)");

        } finally {
            if (session != null) session.shutdown();
        }

    }

    public void get() {
        Session session = null;
        try {
            session = CassandraConnectionManager.getConnetion();
            ResultSet resultSet = session.execute("select * from metadata_keyspace.metadata_tbl where project_id='1'");
            Iterator<Row> iterator = resultSet.iterator();
            while (iterator.hasNext()) {
                Row row = iterator.next();
                // ColumnDefinitions colDef=row.getColumnDefinitions();
                //
                // for(Definition def:colDef.asList())
                // {
                // System.out.println(def.getName());
                // }
                System.out.println(row.getDate("created"));

            }
        } finally {
            session.shutdown();
        }

    }

    public void save() {

    }

    public void update() {

    }
}
