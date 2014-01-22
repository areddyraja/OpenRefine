package com.google.refine.cassandra;

import java.io.IOException;
import java.io.InputStream;

import org.apache.tools.tar.TarOutputStream;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.refine.ProjectMetadata;
import com.google.refine.history.HistoryEntryManager;
import com.google.refine.io.FileProjectManager;
import com.google.refine.model.Project;


public class CassandraProjectManager {
    final static Logger logger = LoggerFactory.getLogger("FileProjectManager");
    CassandraDaoImpl daoImpl;
    
//    static public synchronized void initialize() {
//        if (singleton != null) {
//            logger.warn("Overwriting singleton already set: " + singleton);
//        }
//        singleton = new CassandraProjectManager();
//        // This needs our singleton set, thus the unconventional control flow
//    }
    
    public CassandraProjectManager()
    {
        daoImpl=new CassandraDaoImpl();
      
    }

    public ProjectMetadata  loadProjectMetadata(long projectID) {
        // TODO Auto-generated method stub
        try {
            JSONObject jsonObject=daoImpl.loadMetaData(projectID);
            ProjectMetadata metaData=ProjectMetadata.loadFromJSON(jsonObject);
            return metaData;
           
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    protected Project loadProject(long id) {
        // TODO Auto-generated method stub
        return null;
    }

    public void importProject(long projectID, InputStream inputStream, boolean gziped)
            throws IOException {
        // TODO Auto-generated method stub
        
    }

    public void exportProject(long projectId, TarOutputStream tos)
            throws IOException {
        // TODO Auto-generated method stub
        
    }

    public void saveMetadata(ProjectMetadata metadata, long projectId)
            throws Exception {
        // TODO Auto-generated method stub
        daoImpl.saveMetadata(metadata, projectId);
        
    }

    protected void saveProject(Project project)
            throws IOException {
        // TODO Auto-generated method stub
        
    }

    protected void saveWorkspace() {
        // TODO Auto-generated method stub
        
    }

    public HistoryEntryManager getHistoryEntryManager() {
        // TODO Auto-generated method stub
        return null;
    }

    public void deleteProject(long projectID) {
        // TODO Auto-generated method stub
        
    }

}
