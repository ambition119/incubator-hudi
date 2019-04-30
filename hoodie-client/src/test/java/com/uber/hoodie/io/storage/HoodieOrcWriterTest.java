/*
 * Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io.storage;

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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HoodieOrcWriterTest {

  private JavaSparkContext jsc = null;
  private String basePath = null;
  private transient FileSystem fs;
  private String schemaStr;
  private Schema schema;

  private TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void init() throws IOException {
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("TestHoodieOrcWriter"));
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

    Path path = new Path("/Users/meitu/code/hudi/hudi_dev/hoodie-client/src"
        + "/test/java/com/uber/hoodie/io/storage/orc_dc");
    HoodieOrcWriter writer = new HoodieOrcWriter(commitTime,
        path,
        config,
        GenericData.Record.class
    );

    List<HoodieRecord> records = Arrays.asList(record1, record2, record3, record4);
    int seqId = 1;
    for (HoodieRecord record : records) {
      GenericRecord avroRecord = (GenericRecord) record.getData().getInsertValue(schema).get();
      HoodieAvroUtils.addCommitMetadataToRecord(avroRecord, commitTime, "" + seqId++);
      HoodieAvroUtils.addHoodieKeyToRecord(avroRecord, record.getRecordKey(), record.getPartitionPath(), "orc");
      writer.writeAvro(record.getRecordKey(), avroRecord);
      filter.add(record.getRecordKey());
    }
    writer.close();

    // value has been written to orc, read it out and verify.
    Reader reader = OrcFile
        .createReader(path, OrcFile.readerOptions(HoodieTestUtils.getDefaultHadoopConf()));
    StructObjectInspector inspector = (StructObjectInspector) reader.getObjectInspector();

    Object row = null;
    RecordReader rows = reader.rows();
    System.out.println(rows.getRowNumber());
    while (rows.hasNext()) {
      row = rows.next(row);
      System.out.println(row);
    }
  }

}