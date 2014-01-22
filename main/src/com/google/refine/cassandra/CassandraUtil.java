package com.google.refine.cassandra;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.refine.ProjectMetadata;


public class CassandraUtil {
    final static Logger logger = LoggerFactory.getLogger("CassandraUtil");
    public static Map<String,String> getCustomMetaData(ProjectMetadata metaData) throws JSONException
    {
        Map<String,String> map=new HashMap<String, String>();
        Writer writer=new StringWriter();
        metaData.write(new JSONWriter(writer));
        ByteArrayInputStream inputStream=new ByteArrayInputStream(((StringWriter)writer).getBuffer().toString().getBytes());
        Reader reader=new InputStreamReader(inputStream);
        logger.info(((StringWriter)writer).getBuffer().toString());

        JSONTokener tokener = new JSONTokener(reader);
        JSONObject obj = (JSONObject) tokener.nextValue();
        JSONObject custom=(JSONObject)obj.get("customMetadata");
        String[] names=JSONObject.getNames(custom);
        if(names!=null){
            for(String name:names)
            {
                map.put(name,custom.getString(name).toString());
            }
        }

        return map;
    }
    
    public static Map<String,String> getPreferences(ProjectMetadata metaData) throws JSONException
    {
        Map<String,String> map=new HashMap<String, String>();
        Writer writer=new StringWriter();
        metaData.write(new JSONWriter(writer));
        ByteArrayInputStream inputStream=new ByteArrayInputStream(((StringWriter)writer).getBuffer().toString().getBytes());
        Reader reader=new InputStreamReader(inputStream);
        

        JSONTokener tokener = new JSONTokener(reader);
        JSONObject obj = (JSONObject) tokener.nextValue();
        JSONObject custom=(JSONObject)obj.get("preferences");
        String[] names=JSONObject.getNames(custom);
        for(String name:names)
        {
            map.put(name,custom.getString(name).toString());
        }

        return map;
    }
}
