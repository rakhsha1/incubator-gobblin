package org.apache.gobblin.fork;


import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.IOException;
import java.util.List;
import java.nio.charset.StandardCharsets;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.common.io.Resources;

import org.apache.commons.io.FileUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.configuration.ConfigurationKeys;

import com.esri.core.geometry.OperatorImportFromGeoJson;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.GeoJsonImportFlags;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Geometry.GeometryAccelerationDegree;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.exception.GeoIp2Exception;


public class PGLocationBasedForkOperator implements ForkOperator<String, byte []> {

  private static final int NUM_BRANCHES = 2;
  private static final Logger LOG = LoggerFactory.getLogger(PGLocationBasedForkOperator.class);
  
  private Set<String> locationTopics;
  private Polygon geoFilter = null;
  private Boolean shouldGeoFilter = false;
  private OperatorContains containsOperator = null;
  private SpatialReference spatialRef = null;
  private Set<String> euCountries = new HashSet(Arrays.asList("AT", "BE", "BG", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE"));

  private DatabaseReader geoIpFilter = null;

  public void initGeoFilter(WorkUnitState workUnit) {
    if(shouldGeoFilter){
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
        LOG.error("Error reading GeoJson file. Disabling Lat/Lon geo filtering.");
      }
    }
  }

  public void initIpFilter(WorkUnitState workUnit){
    if(shouldGeoFilter){
      String filePath = workUnit.getProp(ConfigurationKeys.LOCATION_FILTER_MAXMIND);
      LOG.info(String.format("Using %s for ip geo filter.", filePath));
      try{
        geoIpFilter = new DatabaseReader
          .Builder(new File(filePath))
          .withCache(new CHMCache())
          .build();
      }catch(IOException e){
        LOG.error("Could not open ip geo filter file.", e);
      }
      
    }
  }

  @Override
  public void init(WorkUnitState workUnit) {
    String topics = workUnit.getProp(ConfigurationKeys.LOCATION_TOPICS, "");
    locationTopics = new HashSet<String>(Arrays.asList(topics.split(",")));
    String geoFilterEnabled = workUnit.getProp(ConfigurationKeys.LOCATION_FILTER_ENABLED, "false");

    if(geoFilterEnabled.equals("true")){
      LOG.info("GeoFilter is enabled.");
      shouldGeoFilter=true;
      initGeoFilter(workUnit);
      initIpFilter(workUnit);
    }else{
      LOG.info("GeoFilter is disabled.");
    }
    
  }

  @Override
  public int getBranches(WorkUnitState workUnitState) {
    return NUM_BRANCHES;
  }

  @Override
  public List<Boolean> forkSchema(WorkUnitState workUnitState, String schema) {
    // The schema goes to both branches.
    return Arrays.asList(Boolean.TRUE, Boolean.TRUE);
  }

  private Boolean isEuropean(Double lat, Double lon){
    if(shouldGeoFilter && geoFilter != null && containsOperator != null){
      return containsOperator.execute(geoFilter, new Point(lon, lat), spatialRef, null);
    }
    else{
      return false;
    }
  }

  private Boolean isEuropean(String ipa){
    if(ipa == null || geoIpFilter == null)
      return false;

    try{
      CityResponse response = geoIpFilter.city(InetAddress.getByName(ipa));
      String country = response.getCountry().getIsoCode();
      return country != null && euCountries.contains(country);
    }catch(IOException e){
      return false;
    }catch(GeoIp2Exception e){
      return false;
    }
    
  }

  private Boolean shouldFilter(String event, String topic) {
    if(!locationTopics.contains(topic)){
      return false;
    }
    JsonObject eventObject;
    try{
      eventObject = new JsonParser().parse(event).getAsJsonObject();  
    }catch(Exception e){
      return true;
    }
    
    Double lat = (eventObject.has("lat")) ? eventObject.get("lat").getAsDouble() : 0.0;
    Double lon = (eventObject.has("lon")) ? eventObject.get("lon").getAsDouble() : 0.0;

    if(isEuropean(lat, lon))
      return true;

    String ipa = (eventObject.has("ipa")) ? eventObject.get("ipa").getAsString() : null;
    return isEuropean(ipa);
  }

  @Override
  public List<Boolean> forkDataRecord(WorkUnitState workUnit, byte [] record) {
    if (shouldFilter(new String(record), workUnit.getExtract().getTable())) {
      return Arrays.asList(Boolean.FALSE, Boolean.TRUE);      
    }

    return Arrays.asList(Boolean.TRUE, Boolean.FALSE);
  }

  @Override
  public void close() throws IOException {
  }
}