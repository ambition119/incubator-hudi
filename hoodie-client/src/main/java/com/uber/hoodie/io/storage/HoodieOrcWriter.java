/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.spark.TaskContext;

/**
 * HoodieOrcWriter use hive's Writer to help limit the size of underlying file. Provides
 * a way to check if the current file can take more records with the <code>canWrite()</code>
 */
public class HoodieOrcWriter<T extends HoodieRecordPayload, R extends IndexedRecord> implements HoodieStorageWriter<R> {

  private static AtomicLong recordIndex = new AtomicLong(1);

  private final Path file;
  private final HoodieWrapperFileSystem fs;
  private final long stripeSize;
  private final HoodieAvroWriteSupport writeSupport;
  private final String commitTime;
  private final Writer writer;

  public HoodieOrcWriter(String commitTime, Path file, HoodieOrcConfig orcConfig,
      Class<IndexedRecord> clazz) throws IOException {
    this.file = HoodieWrapperFileSystem.convertToHoodiePath(file, orcConfig.getHadoopConf());
    this.fs = (HoodieWrapperFileSystem) this.file.getFileSystem(registerFileSystem(file, orcConfig.getHadoopConf()));
    this.writeSupport = orcConfig.getWriteSupport();
    this.commitTime = commitTime;
    this.stripeSize = orcConfig.getStripeSize();

    StructObjectInspector inspector =
        (StructObjectInspector) ObjectInspectorFactory.getReflectionObjectInspector(clazz,
            ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    this.writer = OrcFile.createWriter(
        this.fs,
        this.file,
        registerFileSystem(file, orcConfig.getHadoopConf()),
        inspector,
        orcConfig.getStripeSize(),
        orcConfig.getCompressionKind(),
        orcConfig.getBufferSize(),
        orcConfig.getRowIndexStride()
    );
  }

  public static Configuration registerFileSystem(Path file, Configuration conf) {
    Configuration returnConf = new Configuration(conf);
    String scheme = FSUtils.getFs(file.toString(), conf).getScheme();
    returnConf.set("fs." + HoodieWrapperFileSystem.getHoodieScheme(scheme) + ".impl",
        HoodieWrapperFileSystem.class.getName());

    return returnConf;
  }

  @Override
  public void writeAvroWithMetadata(R avroRecord, HoodieRecord record) throws IOException {
    String seqId = HoodieRecord.generateSequenceId(commitTime, TaskContext.getPartitionId(),
        recordIndex.getAndIncrement());

    HoodieAvroUtils.addHoodieKeyToRecord((GenericRecord) avroRecord, record.getRecordKey(),
        record.getPartitionPath(), file.getName());

    HoodieAvroUtils.addCommitMetadataToRecord((GenericRecord) avroRecord, commitTime, seqId);

    this.writer.addRow(avroRecord);
    writeSupport.add(record.getRecordKey());
  }

  @Override
  public boolean canWrite() {
    return fs.getBytesWritten(file) < stripeSize;
  }

  @Override
  public void writeAvro(String key, R object) throws IOException {
    this.writer.addRow(object);
    writeSupport.add(key);
  }

  @Override
  public void close() throws IOException {
    if (null != this.writer) {
      this.writer.close();
    }
  }
}
