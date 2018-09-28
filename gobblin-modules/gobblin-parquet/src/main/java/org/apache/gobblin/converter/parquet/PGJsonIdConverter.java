
package org.apache.gobblin.converter.parquet;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import com.google.common.base.Optional;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

import gobblin.util.PGStatsClient;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;

import java.nio.charset.StandardCharsets;
import java.io.File;
import com.google.common.io.Resources;
import org.apache.commons.io.FileUtils;

import com.google.common.collect.Iterators;

public class PGJsonIdConverter extends Converter<String, JsonArray, byte [], JsonObject> {
  private static final Logger LOG = LoggerFactory.getLogger(PGJsonEventConverter.class);
  private String schema = "[{\"columnName\":\"timestamp\",\"dataType\":{\"type\":\"long\"}},{\"columnName\":\"value\",\"dataType\":{\"type\":\"string\"}}]";

  private JsonArray convertedSchema = new JsonParser().parse(schema).getAsJsonArray();
  private PGStatsClient statsClient;

  @Override
  public Converter<String, JsonArray, byte [], JsonObject> init(WorkUnitState workUnit) {
    statsClient = new PGStatsClient(workUnit.getProp(ConfigurationKeys.METRICS_HOST), workUnit.getProp(ConfigurationKeys.METRICS_PORT), workUnit.getProp(ConfigurationKeys.METRICS_ENABLED));
    return this;
  }

  @Override
  public void close() throws IOException {
    statsClient.flush();
  }

  @Override
  public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return convertedSchema;
  }

 

  private String processEvent(String event) {
    JsonObject eventObject;
    try{
      eventObject = new JsonParser().parse(event).getAsJsonObject();  
    }catch(Exception e){
      return null;
    }
    
    if(eventObject.has("pgid")){
      return eventObject.get("pgid").getAsString();
    }else{
      return null;
    }
  }

  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, byte[] inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    String topic = workUnit.getExtract().getTable();
    statsClient.pushCounter(topic+"_filtered", 1);
    
    String event = processEvent(new String(inputRecord));
    if(event == null){
      return new ArrayList<JsonObject>();
    }
    
    JsonObject outputRecord = new JsonObject();
    outputRecord.addProperty("timestamp", System.currentTimeMillis());
    outputRecord.addProperty("value", event);
    return new SingleRecordIterable<JsonObject>(outputRecord);
  }
}
