
package gobblin.converter.string;

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

public class PGEventConverter extends Converter<String, String, byte [], byte[]> {
  private static final Logger LOG = LoggerFactory.getLogger(PGEventConverter.class);
  private PGStatsClient statsClient;


  @Override
  public Converter<String, String, byte [], byte[]> init(WorkUnitState workUnit) {
    statsClient = new PGStatsClient(workUnit.getProp(ConfigurationKeys.METRICS_HOST), workUnit.getProp(ConfigurationKeys.METRICS_PORT), workUnit.getProp(ConfigurationKeys.METRICS_ENABLED));
    return this;
  }

  @Override
  public void close() throws IOException {
    statsClient.flush();
  }

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<byte[]> convertRecord(String outputSchema, byte[] inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    String timestamp = String.valueOf(System.currentTimeMillis())+"\t";
    byte[] prepend = timestamp.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING);
    byte[] encoded = new byte[prepend.length + inputRecord.length];
    System.arraycopy(prepend, 0, encoded, 0, prepend.length);
    System.arraycopy(inputRecord, 0, encoded, prepend.length, inputRecord.length);
    statsClient.pushCounter(outputSchema, 1);
    return new SingleRecordIterable<byte[]>(encoded);
  }
}
