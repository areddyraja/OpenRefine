
package com.google.refine.cassandra;

/*

 *  Akrantha Inc

 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;
import org.apache.tools.tar.TarOutputStream;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import com.google.refine.ProjectManager;
import com.google.refine.ProjectMetadata;
import com.google.refine.history.HistoryEntryManager;
import com.google.refine.io.FileHistoryEntryManager;
import com.google.refine.io.ProjectMetadataUtilities;
import com.google.refine.io.ProjectUtilities;
import com.google.refine.model.Cell;
import com.google.refine.model.Column;
import com.google.refine.model.Project;
import com.google.refine.model.Recon;
import com.google.refine.model.Row;
import com.google.refine.preference.TopList;

public class CassandraProjectManager2 extends ProjectManager {

    final static protected String PROJECT_DIR_SUFFIX = ".project";

    static CassandraDaoImpl daoImpl;
    protected File _workspaceDir;

    final static Logger logger = LoggerFactory.getLogger("FileProjectManager");

    static public synchronized void initialize(File dir) {
        if (singleton != null) {
            logger.warn("Overwriting singleton already set: " + singleton);
        }
        logger.info("Using workspace directory: {}", dir.getAbsolutePath());
        singleton = new CassandraProjectManager2(dir);
        // This needs our singleton set, thus the unconventional control flow
        // TODO Commented for now.. requires thinking and recovering in case of
        // Cassandra Projects
        // ((CassandraProjectManager2) singleton).recover();
        daoImpl = new CassandraDaoImpl();
    }

    protected CassandraProjectManager2(File dir) {
        super();
        _workspaceDir = dir;
        if (!_workspaceDir.exists() && !_workspaceDir.mkdirs()) {
            logger.error("Failed to create directory : " + _workspaceDir);
            return;
        }

        load();
    }

    public File getWorkspaceDir() {
        return _workspaceDir;
    }

    static public File getProjectDir(File workspaceDir, long projectID) {
        File dir = new File(workspaceDir, projectID + PROJECT_DIR_SUFFIX);
        if (!dir.exists()) {
            dir.mkdir();
        }
        return dir;
    }

    public File getProjectDir(long projectID) {
        return getProjectDir(_workspaceDir, projectID);
    }

    /**
     * Import an external project that has been received as a .tar file,
     * expanded, and copied into our workspace directory.
     * 
     * @param projectID
     */
    @Override
    public boolean loadProjectMetadata(long projectID) {
        boolean cassandra = true;
        ProjectMetadata metadata = null;
        synchronized (this) {
            if (cassandra) {
                metadata = prepareCassandraMetaDataAndSave(projectID);
                try {
                    daoImpl.saveMetadata(metadata, projectID);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else {
                metadata = ProjectMetadataUtilities.load(getProjectDir(projectID));
                if (metadata == null) {
                    metadata = ProjectMetadataUtilities.recover(getProjectDir(projectID), projectID);
                }
            }

            if (metadata != null) {
                _projectsMetadata.put(projectID, metadata);
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public void importProject(long projectID, InputStream inputStream, boolean gziped)
            throws IOException {
        boolean cassandra = true;
        if (cassandra) return;
        File destDir = this.getProjectDir(projectID);
        destDir.mkdirs();

        if (gziped) {
            GZIPInputStream gis = new GZIPInputStream(inputStream);
            untar(destDir, gis);
        } else {
            untar(destDir, inputStream);
        }
    }

    protected void untar(File destDir, InputStream inputStream)
            throws IOException {
        TarInputStream tin = new TarInputStream(inputStream);
        TarEntry tarEntry = null;

        while ((tarEntry = tin.getNextEntry()) != null) {
            File destEntry = new File(destDir, tarEntry.getName());
            File parent = destEntry.getParentFile();

            if (!parent.exists()) {
                parent.mkdirs();
            }

            if (tarEntry.isDirectory()) {
                destEntry.mkdirs();
            } else {
                FileOutputStream fout = new FileOutputStream(destEntry);
                try {
                    tin.copyEntryContents(fout);
                } finally {
                    fout.close();
                }
            }
        }

        tin.close();
    }

    @Override
    public void exportProject(long projectId, TarOutputStream tos)
            throws IOException {
        File dir = this.getProjectDir(projectId);
        this.tarDir("", dir, tos);
    }

    protected void tarDir(String relative, File dir, TarOutputStream tos)
            throws IOException {
        File[] files = dir.listFiles();
        for (File file : files) {
            if (!file.isHidden()) {
                String path = relative + file.getName();

                if (file.isDirectory()) {
                    tarDir(path + File.separator, file, tos);
                } else {
                    TarEntry entry = new TarEntry(path);

                    entry.setMode(TarEntry.DEFAULT_FILE_MODE);
                    entry.setSize(file.length());
                    entry.setModTime(file.lastModified());

                    tos.putNextEntry(entry);

                    copyFile(file, tos);

                    tos.closeEntry();
                }
            }
        }
    }

    protected void copyFile(File file, OutputStream os)
            throws IOException {
        final int buffersize = 4096;

        FileInputStream fis = new FileInputStream(file);
        try {
            byte[] buf = new byte[buffersize];
            int count;

            while ((count = fis.read(buf, 0, buffersize)) != -1) {
                os.write(buf, 0, count);
            }
        } finally {
            fis.close();
        }
    }

    @Override
    protected void saveMetadata(ProjectMetadata metadata, long projectId)
            throws Exception {
        File projectDir = getProjectDir(projectId);
        ProjectMetadataUtilities.save(metadata, projectDir);
    }

    @Override
    protected void saveProject(Project project)
            throws IOException {
        ProjectUtilities.save(project);
    }

    @Override
    public Project loadProject(long id) {
        boolean cassandra = true;
        if (cassandra) {
            return loadFromCassandra(id);
        } else {
            return ProjectUtilities.load(getProjectDir(id), id);
        }
    }

    /**
     * Save the workspace's data out to file in a safe way: save to a temporary
     * file first and rename it to the real file.
     */
    @Override
    protected void saveWorkspace() {
        synchronized (this) {
            File tempFile = new File(_workspaceDir, "workspace.temp.json");
            try {
                if (!saveToFile(tempFile)) {
                    // If the save wasn't really needed, just keep what we had
                    tempFile.delete();
                    logger.info("Skipping unnecessary workspace save");
                    return;
                }
            } catch (Exception e) {
                e.printStackTrace();

                logger.warn("Failed to save workspace");
                return;
            }

            File file = new File(_workspaceDir, "workspace.json");
            File oldFile = new File(_workspaceDir, "workspace.old.json");

            if (oldFile.exists()) {
                oldFile.delete();
            }

            if (file.exists()) {
                file.renameTo(oldFile);
            }

            tempFile.renameTo(file);

            logger.info("Saved workspace");
        }
    }

    protected boolean saveToFile(File file)
            throws IOException, JSONException {
        FileWriter writer = new FileWriter(file);
        boolean saveWasNeeded = false;
        try {
            JSONWriter jsonWriter = new JSONWriter(writer);
            jsonWriter.object();
            jsonWriter.key("projectIDs");
            jsonWriter.array();
            for (Long id : _projectsMetadata.keySet()) {
                ProjectMetadata metadata = _projectsMetadata.get(id);
                if (metadata != null) {
                    jsonWriter.value(id);
                    if (metadata.isDirty()) {
                        ProjectMetadataUtilities.save(metadata, getProjectDir(id));
                        saveWasNeeded = true;
                    }
                }
            }
            jsonWriter.endArray();
            writer.write('\n');

            jsonWriter.key("preferences");
            saveWasNeeded |= _preferenceStore.isDirty();
            _preferenceStore.write(jsonWriter, new Properties());

            jsonWriter.endObject();
        } finally {
            writer.close();
        }
        return saveWasNeeded;
    }

    @Override
    public void deleteProject(long projectID) {
        synchronized (this) {
            removeProject(projectID);

            File dir = getProjectDir(projectID);
            if (dir.exists()) {
                deleteDir(dir);
            }
        }

        saveWorkspace();
    }

    static protected void deleteDir(File dir) {
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                deleteDir(file);
            } else {
                file.delete();
            }
        }
        dir.delete();
    }

    protected void load() {
        if (loadFromFile(new File(_workspaceDir, "workspace.json"))) {
            return;
        }
        if (loadFromFile(new File(_workspaceDir, "workspace.temp.json"))) {
            return;
        }
        if (loadFromFile(new File(_workspaceDir, "workspace.old.json"))) {
            return;
        }
        logger.error("Failed to load workspace from any attempted alternatives.");
    }

    protected boolean loadFromFile(File file) {
        logger.info("Loading workspace: {}", file.getAbsolutePath());

        _projectsMetadata.clear();

        boolean found = false;

        if (file.exists() || file.canRead()) {
            FileReader reader = null;
            try {
                reader = new FileReader(file);
                JSONTokener tokener = new JSONTokener(reader);
                JSONObject obj = (JSONObject) tokener.nextValue();

                JSONArray a = obj.getJSONArray("projectIDs");
                int count = a.length();
                for (int i = 0; i < count; i++) {
                    long id = a.getLong(i);

                    File projectDir = getProjectDir(id);
                    ProjectMetadata metadata = ProjectMetadataUtilities.load(projectDir);

                    _projectsMetadata.put(id, metadata);
                }

                if (obj.has("preferences") && !obj.isNull("preferences")) {
                    _preferenceStore.load(obj.getJSONObject("preferences"));
                }

                if (obj.has("expressions") && !obj.isNull("expressions")) { // backward
                                                                            // compatibility
                    ((TopList) _preferenceStore.get("scripting.expressions")).load(obj.getJSONArray("expressions"));
                }

                found = true;
            } catch (JSONException e) {
                logger.warn("Error reading file", e);
            } catch (IOException e) {
                logger.warn("Error reading file", e);
            } finally {
                try {
                    if (reader != null) {
                        reader.close();
                    }
                } catch (IOException e) {
                    logger.warn("Exception closing file", e);
                }
            }
        }

        return found;
    }

    protected void recover() {
        boolean recovered = false;
        for (File file : _workspaceDir.listFiles()) {
            if (file.isDirectory() && !file.isHidden()) {
                String dirName = file.getName();
                if (file.getName().endsWith(PROJECT_DIR_SUFFIX)) {
                    String idString = dirName.substring(0, dirName.length() - PROJECT_DIR_SUFFIX.length());
                    long id = -1;
                    try {
                        id = Long.parseLong(idString);
                    } catch (NumberFormatException e) {
                        // ignore
                    }

                    if (id > 0 && !_projectsMetadata.containsKey(id)) {
                        if (loadProjectMetadata(id)) {
                            logger.info("Recovered project named " + getProjectMetadata(id).getName()
                                    + " in directory " + dirName);
                            recovered = true;
                        } else {
                            logger.warn("Failed to recover project in directory " + dirName);

                            file.renameTo(new File(file.getParentFile(), dirName + ".corrupted"));
                        }
                    }
                }
            }
        }
        if (recovered) {
            saveWorkspace();
        }
    }

    @Override
    public HistoryEntryManager getHistoryEntryManager() {
        return new FileHistoryEntryManager();
    }

    /**
     * Akrantha Inc
     */

    public CassandraDaoImpl getCassandraDaoImpl() {
        return daoImpl;
    }

    public ProjectMetadata loadCassandraProjectMetadata(long projectID) {
        // TODO Auto-generated method stub
        try {
            JSONObject jsonObject = daoImpl.loadMetaData(projectID);
            ProjectMetadata metaData = ProjectMetadata.loadFromJSON(jsonObject);
            return metaData;

        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    private ProjectMetadata prepareCassandraMetaDataAndSave(long projectID) {
        ProjectMetadata metaData = new ProjectMetadata();
        metaData.setName("PROJ_".concat(new Long(projectID).toString()));
        metaData.setPassword("");
        metaData.setEncoding("UTF-8");
        metaData.setEncodingConfidence(0);
        // TODO Add Custom MetaData
        // TODO add Preference
        return metaData;
    }

    private Project loadFromCassandra(long id) {

        //long start = System.currentTimeMillis();
        

        Project project = new Project(id);
        loadColumnsFromCassandra(project);
        loadRowsFromCassandra(project);
        
        
        project.update();

        return project;

    }

    private void loadRowsFromCassandra(Project project) {
        CassandraDb cassandraDb = new CassandraDb();

        Session session = cassandraDb.connect("127.0.0.1");
        
        ResultSet results = session.execute("SELECT * FROM demodb.datasample;");

        List<com.datastax.driver.core.Row> rows = results.all();

        ListIterator<com.datastax.driver.core.Row> iterator = rows.listIterator();
        String[] fields = new String[9];
        // iterate through result set and get row

        int maxCellCount  = project.columnModel.columns.size();
        while (iterator.hasNext()) {
            // line = reader.readLine();
            com.datastax.driver.core.Row _row = iterator.next();

            fields[0] = _row.getString(0);
            fields[1] = _row.getString(1);
            fields[2] = _row.getString(2);
            fields[3] = _row.getString(3);
            fields[4] = _row.getString(4);
            fields[5] = _row.getString(5);
            fields[6] = _row.getString(6);
            fields[7] = _row.getString(7);
            fields[8] = _row.getString(8);

            project.rows.add(prepareRow(fields, project.columnModel.columns.size()));
            //Row row = Row.load(fields, pool);
            //project.rows.add(row);
            project.columnModel.setMaxCellIndex(maxCellCount - 1);

            //maxCellCount = Math.max(maxCellCount, project.cells.size());
            
        }
        
    }

    private Row prepareRow(String[] fields, int cellCount) {
        Row row = new Row(cellCount);
        Serializable value = null;
        Recon recon = null;
        // prepare cells
        for (String field : fields) {
            value = field;
            row.cells.add(new Cell(value, recon));
        }
        return row;
    }

    private void loadColumnsFromCassandra(Project project) {
        CassandraDb cassandraDb = new CassandraDb();

        Session session = cassandraDb.connect("127.0.0.1");
        
        ResultSet results = session.execute("SELECT * FROM demodb.datasample;");
        ColumnDefinitions columndef = results.getColumnDefinitions();

        // iterate through result set and get row

        for (int i = 0; i < columndef.size(); i++) {
            project.columnModel.columns.add(new Column(i, columndef.getName(i)));
        }
       
        
    }
}
