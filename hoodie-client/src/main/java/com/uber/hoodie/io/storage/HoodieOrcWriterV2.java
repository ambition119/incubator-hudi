package com.uber.hoodie.io.storage;

import com.uber.hoodie.avro.HoodieAvroWriteSupport;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.spark.TaskContext;

/**
 * @Author: wpl
 */
public class HoodieOrcWriterV2<T extends HoodieRecordPayload, R extends IndexedRecord>
    implements HoodieStorageWriter<R> {
  private static AtomicLong recordIndex = new AtomicLong(1);

  private final Path file;
  private final HoodieWrapperFileSystem fs;
  private final long stripeSize;
  private final HoodieAvroWriteSupport writeSupport;
  private final String commitTime;
  private final Writer writer;
  private final TypeDescription orcSchema;
  private final List<String> orcFieldNames;
  private final VectorizedRowBatch orcBatch;

  public HoodieOrcWriterV2(String commitTime, Path file, HoodieOrcConfig orcConfig,
      TypeDescription orcSchema) throws IOException {
    this.file = HoodieWrapperFileSystem.convertToHoodiePath(file, orcConfig.getHadoopConf());
    this.fs = (HoodieWrapperFileSystem) this.file.getFileSystem(registerFileSystem(file, orcConfig.getHadoopConf()));
    this.writeSupport = orcConfig.getWriteSupport();
    this.commitTime = commitTime;
    this.stripeSize = orcConfig.getStripeSize();

    this.orcSchema = orcSchema;
    // hoodie meta field
    this.orcSchema.addField("_hoodie_commit_time", TypeDescription.createString())
        .addField("_hoodie_commit_seqno", TypeDescription.createString())
        .addField("_hoodie_record_key", TypeDescription.createString())
        .addField("_hoodie_partition_path", TypeDescription.createString())
        .addField("_hoodie_file_name", TypeDescription.createString());

    this.writer = OrcFile.createWriter(
        file,
        OrcFile.writerOptions(orcConfig.getHadoopConf())
            .setSchema(this.orcSchema)
            .stripeSize(orcConfig.getStripeSize())
            .bufferSize(orcConfig.getBufferSize())
            .blockSize(orcConfig.getBlockSize())
            .compress(CompressionKind.ZLIB)
            .version(org.apache.orc.OrcFile.Version.V_0_12));

    orcFieldNames = this.orcSchema.getFieldNames();
    orcBatch = this.orcSchema.createRowBatch();
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
    GenericRecord genericRecord = (GenericRecord) avroRecord;
    String seqId = HoodieRecord.generateSequenceId(commitTime, TaskContext.getPartitionId(),
        recordIndex.getAndIncrement());

    HoodieAvroUtils.addHoodieKeyToRecord(genericRecord, record.getRecordKey(),
        record.getPartitionPath(), file.getName());

    GenericRecord metadataToRecord = HoodieAvroUtils
        .addCommitMetadataToRecord(genericRecord, commitTime, seqId);

    Schema avroSchema = metadataToRecord.getSchema();

    int row = orcBatch.size++;
    setOrcFiledValue((R) avroRecord, avroSchema, row);

    if (orcBatch.size !=  0) {
      writer.addRowBatch(orcBatch);
      orcBatch.reset();
    }
    writeSupport.add(record.getRecordKey());
  }

  @Override
  public boolean canWrite() {
    return fs.getBytesWritten(file) < stripeSize;
  }

  @Override
  public void writeAvro(String key, R avroRecord) throws IOException {
    Schema avroSchema = avroRecord.getSchema();

    int row = orcBatch.size++;
    setOrcFiledValue((R) avroRecord, avroSchema, row);

    if (orcBatch.size !=  0) {
      writer.addRowBatch(orcBatch);
      orcBatch.reset();
    }

    writeSupport.add(key);
  }

  @Override
  public void close() throws IOException {
    if (null != this.writer) {
      this.writer.close();
    }
  }

  private void setOrcFiledValue(R avroRecord, Schema avroSchema, int row) throws IOException {
    for (int i = 0; i < orcFieldNames.size(); i++) {
      //orc
      String fieldName = orcFieldNames.get(i);
      TypeDescription.Category fieldType = orcSchema.getChildren().get(i).getCategory();

      //avro
      Schema.Field field = avroSchema.getField(fieldName);
      Object fieldValue = avroRecord.get(field.pos());

      switch (fieldType) {
        case CHAR:
        case VARCHAR:
        case BINARY:
        case STRING:
          byte[] bytes = String.valueOf(fieldValue).getBytes();
          ((BytesColumnVector) orcBatch.cols[i]).setVal(row, bytes, 0, bytes.length);
          break;
        case BOOLEAN:
        case BYTE:
        case DATE:
        case INT:
        case LONG:
        case SHORT:
          ((LongColumnVector) orcBatch.cols[i]).vector[row] = Long.valueOf(String.valueOf(fieldValue));
          break;
        case DOUBLE:
        case FLOAT:
          ((DoubleColumnVector) orcBatch.cols[i]).vector[row] = Double.valueOf(String.valueOf(fieldValue));
          break;
        default:
          throw new IllegalArgumentException("Not support type " + fieldType);
      }

      if (orcBatch.size == orcBatch.getMaxSize()) {
        writer.addRowBatch(orcBatch);
        orcBatch.reset();
      }
    }
  }

}
