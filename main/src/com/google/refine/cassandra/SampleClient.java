package com.google.refine.cassandra;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;

//import javax.json.Json;
//import javax.json.JsonWriter;
//import javax.json.JsonWriterFactory;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.JSONWriter;

import com.google.refine.ProjectMetadata;
import com.google.refine.io.ProjectMetadataUtilities;
import com.google.refine.preference.TopList;


public class SampleClient {
    public static void main(String[] args) throws JSONException {
//           CassandraDaoImpl impl=new CassandraDaoImpl();
//           impl.get();
//           CassandraConnectionManager.shutDown();
           
           
           ProjectMetadata metadata=new ProjectMetadata();
           metadata.setName("name");
           metadata.setCustomMetadata("name1", "val1");
           metadata.setCustomMetadata("name2", "val2");
           
           ByteArrayOutputStream stream=new ByteArrayOutputStream();
           
           Writer writer=new StringWriter();
           
           metadata.write(new JSONWriter(writer));
           String str=((StringWriter)writer).getBuffer().toString();
           
           ByteArrayInputStream inputStream=new ByteArrayInputStream(((StringWriter)writer).getBuffer().toString().getBytes());
           Reader reader=new InputStreamReader(inputStream);
           
           
           
           
           

           JSONTokener tokener = new JSONTokener(reader);
           JSONObject obj = (JSONObject) tokener.nextValue();
           JSONObject custom=(JSONObject)obj.get("customMetadata");
           String[] names=JSONObject.getNames(custom);
           for(String name:names)
           {
               System.out.println(name+"::"+custom.getString(name));
           }
           System.out.println(obj.get("customMetadata"));
           ProjectMetadata m1= ProjectMetadata.loadFromJSON(obj);
       
           System.out.println(m1);
           
           
           
    }
}
