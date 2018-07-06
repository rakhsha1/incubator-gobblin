
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


import java.math.RoundingMode;
import java.math.BigDecimal;

import com.esri.core.geometry.OperatorImportFromGeoJson;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.GeoJsonImportFlags;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Geometry.GeometryAccelerationDegree;

import java.nio.file.Files;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.io.File;
import com.google.common.io.Resources;
import org.apache.commons.io.FileUtils;

import com.google.common.collect.Iterators;

public class PGJsonEventConverter extends Converter<String, JsonArray, byte [], JsonObject> {
  private static final Logger LOG = LoggerFactory.getLogger(PGJsonEventConverter.class);
  private PGStatsClient statsClient;
  private String schema = "[{\"columnName\":\"timestamp\",\"dataType\":{\"type\":\"long\"}},{\"columnName\":\"value\",\"dataType\":{\"type\":\"string\"}}]";
  private Set<String> locationTopics;

  private int locationPrecision;
  private Polygon geoFilter = null;
  private int shouldGeoFilter = 0;
  private OperatorContains containsOperator = null;
  private SpatialReference spatialRef = null;

  @Override
  public Converter<String, JsonArray, byte [], JsonObject> init(WorkUnitState workUnit) {
    statsClient = new PGStatsClient(workUnit.getProp(ConfigurationKeys.METRICS_HOST), workUnit.getProp(ConfigurationKeys.METRICS_PORT), workUnit.getProp(ConfigurationKeys.METRICS_ENABLED));
    String topics = workUnit.getProp(ConfigurationKeys.LOCATION_TOPICS, "");
    locationTopics = new HashSet<String>(Arrays.asList(topics.split(",")));
    locationPrecision = workUnit.getPropAsInt(ConfigurationKeys.LOCATION_PRECISION, 3);
    // String filterGeoJson = new String(Files.readAllBytes(Paths.get("file")), StandardCharsets.UTF_8);
    String geoFilterEnabled = workUnit.getProp(ConfigurationKeys.LOCATION_FILTER_ENABLED, "false");
    
    if(geoFilterEnabled.equals("true")){
      shouldGeoFilter=1;
      String filterGeoJson = null;
      if(workUnit.contains(ConfigurationKeys.LOCATION_FILTER_GEOJSON)){
        String filePath = workUnit.getProp(ConfigurationKeys.LOCATION_FILTER_GEOJSON);
        LOG.info(String.format("Using %s for geo filter.", filePath));
        try{
          filterGeoJson = FileUtils.readFileToString(new File(filePath));
        }catch(IOException e){
          LOG.error("Could not open geo filter file.", e);
        }
        
      }else{
        LOG.info("Using default (europe-simple.json) file for geo filter.");
        try{
          filterGeoJson = Resources.toString(Resources.getResource("europe-simple.json"), StandardCharsets.UTF_8);
        }catch(IOException e){
          LOG.error("Could not open geo filter file.", e);
        }
      }
      if(filterGeoJson != null){
        geoFilter= (Polygon) OperatorImportFromGeoJson
          .local()
          .execute(
            GeoJsonImportFlags.geoJsonImportDefaults, 
            Geometry.Type.Polygon, 
            filterGeoJson, 
            null
          )
          .getGeometry();

        spatialRef=SpatialReference.create(4326);
        containsOperator=OperatorContains.local();
        
        containsOperator
          .accelerateGeometry(
            geoFilter,
            spatialRef,
            GeometryAccelerationDegree.enumHot
          );

      }else{
        LOG.error("Error reading GeoJson file. Disabling geo filtering.");
      }

    }else{
      LOG.info("GeoFilter is disabled.");
    }
 
    return this;
  }

  @Override
  public void close() throws IOException {
    statsClient.flush();
  }

  @Override
  public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    JsonParser jsonParser = new JsonParser();
    JsonElement jsonSchema = jsonParser.parse(schema);
    return jsonSchema.getAsJsonArray();
  }

  private Boolean isEuropean(Double lat, Double lon){
    if(shouldGeoFilter==1 && geoFilter != null && containsOperator != null){
      return containsOperator.execute(geoFilter, new Point(lon, lat), spatialRef, null);
    }
    else{
      return false;
    }
  }

  private String processEvent(String event, String topic) {
    if(!locationTopics.contains(topic)){
      return event;
    }
    JsonObject eventObject;
    try{
      eventObject = new JsonParser().parse(event).getAsJsonObject();  
    }catch(Exception e){
      return null;
    }
    
    Double lat = (eventObject.has("lat")) ? eventObject.get("lat").getAsDouble() : 0.0;
    Double lon = (eventObject.has("lon")) ? eventObject.get("lon").getAsDouble() : 0.0;
   
    if(isEuropean(lat, lon)){
      return null;
    }

    Double roundedLat = new BigDecimal(lat).setScale(locationPrecision, RoundingMode.HALF_UP).doubleValue();
    if(roundedLat != 0.0){
      eventObject.addProperty("lat", roundedLat);
    }
    Double roundedLon = new BigDecimal(lon).setScale(locationPrecision, RoundingMode.HALF_UP).doubleValue();
    if(roundedLon != 0.0){
      eventObject.addProperty("lon", roundedLon);
    }

    return eventObject.toString(); 
  }

  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, byte[] inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    String topic = workUnit.getExtract().getTable();
    statsClient.pushCounter(topic, 1);
    
    String event = processEvent(new String(inputRecord), topic);
    if(event == null){
      return new ArrayList<JsonObject>();
    }
    
    JsonObject outputRecord = new JsonObject();
    outputRecord.addProperty("timestamp", System.currentTimeMillis());
    outputRecord.addProperty("value", event);
    return new SingleRecordIterable<JsonObject>(outputRecord);
  }
}
