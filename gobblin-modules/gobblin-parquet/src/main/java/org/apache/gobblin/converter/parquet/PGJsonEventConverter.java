
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

public class PGJsonEventConverter extends Converter<String, JsonArray, byte [], JsonObject> {
  private static final Logger LOG = LoggerFactory.getLogger(PGJsonEventConverter.class);
  private PGStatsClient statsClient;
  private String schema = "[{\"columnName\":\"timestamp\",\"dataType\":{\"type\":\"long\"}},{\"columnName\":\"value\",\"dataType\":{\"type\":\"string\"}}]";

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
    JsonParser jsonParser = new JsonParser();
    JsonElement jsonSchema = jsonParser.parse(schema);
    return jsonSchema.getAsJsonArray();
  }

  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, byte[] inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    String topic = workUnit.getExtract().getTable();
    statsClient.pushCounter(topic, 1);
    JsonObject outputRecord = new JsonObject();
    outputRecord.addProperty("timestamp", System.currentTimeMillis());
    outputRecord.addProperty("value", new String(inputRecord));
    return new SingleRecordIterable<JsonObject>(outputRecord);
  }
}
