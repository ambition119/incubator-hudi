package com.uber.hoodie.io.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.hoodie.avro.HoodieAvroWriteSupport;
import com.uber.hoodie.common.BloomFilter;
import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.TestRawTripPayload;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestHoodieOrcWriterV2 {

  private JavaSparkContext jsc = null;
  private String basePath = null;
  private transient FileSystem fs;
  private String schemaStr;
  private Schema schema;

  private TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void init() throws IOException {
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("TestHoodieBloomIndex"));
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    basePath = folder.getRoot().getAbsolutePath();
    fs = FSUtils.getFs(basePath, jsc.hadoopConfiguration());
    HoodieTestUtils.init(jsc.hadoopConfiguration(), basePath);
    schemaStr = IOUtils.toString(getClass().getResourceAsStream("/exampleSchema.txt"), "UTF-8");
    schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(schemaStr));
    temporaryFolder.create();
  }

  @Test
  public void testWriteOrcFile() throws Exception {
    // Create some records to use
    String recordStr1 = "{\"_row_key\":\"1eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"3eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    String recordStr4 = "{\"_row_key\":\"4eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":32}";
    TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
    HoodieRecord record1 = new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()),
        rowChange1);
    TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
    HoodieRecord record2 = new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()),
        rowChange2);
    TestRawTripPayload rowChange3 = new TestRawTripPayload(recordStr3);
    HoodieRecord record3 = new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()),
        rowChange3);
    TestRawTripPayload rowChange4 = new TestRawTripPayload(recordStr4);
    HoodieRecord record4 = new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()),
        rowChange4);

    // We write record1, record2 to a orc file, but the bloom filter contains (record1,
    // record2, record3).
    BloomFilter filter = new BloomFilter(10000, 0.0000001);
    filter.add(record3.getRecordKey());

    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(new AvroSchemaConverter().convert(schema), schema,
        filter);
    String commitTime = HoodieTestUtils.makeNewCommitTime();

    HoodieOrcConfig config = new HoodieOrcConfig(writeSupport,  HoodieTestUtils.getDefaultHadoopConf(),
        CompressionKind.ZLIB, 268435456, 67108864, 262144, 10000);

    Path path = new Path("/Users/meitu/code/hudi/hudi_dev/hoodie-client/"
        + "src/test/java/com/uber/hoodie/io/storage/orc_dc.orc");
    TypeDescription orcSchema = TypeDescription.fromString("struct<_row_key:string,time:string,number:int>");
    HoodieOrcWriterV2 writer = new HoodieOrcWriterV2(commitTime,
        path,
        config,
        orcSchema
    );

    List<HoodieRecord> records = Arrays.asList(record1, record2, record3, record4);

    ObjectMapper mapper = new ObjectMapper();

    int seqId = 1;
    for (HoodieRecord record : records) {
      GenericRecord avroRecord = (GenericRecord) record.getData().getInsertValue(schema).get();
      Map<String, Object> jsonRecordMap = mapper.readValue(avroRecord.toString(), Map.class);

      System.out.println("dddd-> " + jsonRecordMap);

      HoodieAvroUtils.addCommitMetadataToRecord(avroRecord, commitTime, "" + seqId++);
      HoodieAvroUtils.addHoodieKeyToRecord(avroRecord, record.getRecordKey(), record.getPartitionPath(), "orc");
      writer.writeAvro(record.getRecordKey(), avroRecord);
      filter.add(record.getRecordKey());
    }
    writer.close();

    // value has been written to orc, read it out and verify.
    Reader reader = org.apache.hadoop.hive.ql.io.orc.OrcFile.createReader(
        path, org.apache.hadoop.hive.ql.io.orc.OrcFile.readerOptions(
            HoodieTestUtils.getDefaultHadoopConf()));

    Object row = null;
    RecordReader rows = reader.rows();
    System.out.println(rows.getRowNumber());
    while (rows.hasNext()) {
      row = rows.next(row);
      row.toString();
      System.out.println(row);
    }
  }
}
