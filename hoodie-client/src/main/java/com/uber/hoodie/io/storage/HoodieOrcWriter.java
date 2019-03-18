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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.spark.TaskContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HoodieOrcWriter
 */
public class HoodieOrcWriter<T extends HoodieRecordPayload, R extends IndexedRecord> implements HoodieStorageWriter<R> {

   private static AtomicLong recordIndex = new AtomicLong(1);

   private final Path file;
   private final TypeDescription orcSchema;
   private final HoodieWrapperFileSystem fs;
   private final long maxFileSize;
   private final HoodieAvroWriteSupport writeSupport;
   private final String commitTime;
   private final Writer writer;

   public HoodieOrcWriter(String commitTime, Path file, TypeDescription orcSchema, HoodieOrcConfig orcConfig) throws IOException {
      this.file = HoodieWrapperFileSystem.convertToHoodiePath(file, orcConfig.getHadoopConf());
      this.fs = (HoodieWrapperFileSystem) this.file
            .getFileSystem(registerFileSystem(file, orcConfig.getHadoopConf()));
      this.orcSchema = orcSchema;
      this.maxFileSize = orcConfig.getMaxFileSize() + Math
            .round(orcConfig.getMaxFileSize() * orcConfig.getCompressionRatio());
      this.writeSupport = orcConfig.getWriteSupport();
      this.commitTime = commitTime;

      this.writer = OrcFile.createWriter(file,
            OrcFile.writerOptions(registerFileSystem(file, orcConfig.getHadoopConf())).setSchema(orcSchema));

//      this.writerImpl =  new WriterImpl(this.fs, this.file, null);
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

      List<String> orcFieldNames = orcSchema.getFieldNames();
      VectorizedRowBatch orcBatch = orcSchema.createRowBatch();
      Map<String, ColumnVector> orcRow = new HashMap<String, ColumnVector>();
      for (int i = 0; i < orcFieldNames.size(); i++) {
         String fieldName = orcFieldNames.get(i);
         TypeDescription.Category fieldType = orcSchema.getChildren().get(i).getCategory();
         ColumnVector fieldNameColumnVector = getColumnVector(i, fieldType, orcBatch);
         orcRow.put(fieldName, fieldNameColumnVector);
      }

      //TODO: Assign value to orc batch, the current implementation may be a bit issue
      Schema avroRecordSchema = avroRecord.getSchema();
      if (CollectionUtils.isEmpty(orcRow.keySet())) {
         for (String fieldName: orcRow.keySet()) {
            int row = orcBatch.size++;
            Schema.Field field = avroRecordSchema.getField(fieldName);
            Object fieldValue = avroRecord.get(field.pos());
            if (null != fieldValue) {
               ColumnVector columnVector = orcRow.get(fieldName);
               if (columnVector instanceof BytesColumnVector) {
                  ((BytesColumnVector) columnVector).vector[row] = String.valueOf(fieldValue).getBytes();
                  continue;
               }

               if (columnVector instanceof LongColumnVector) {
                  ((LongColumnVector) columnVector).vector[row] = Long.valueOf(String.valueOf(fieldValue));
                  continue;
               }

               if (columnVector instanceof DoubleColumnVector) {
                  ((DoubleColumnVector) columnVector).vector[row] = Double.valueOf(String.valueOf(fieldValue));
               }
            }

            if (orcBatch.size == orcBatch.getMaxSize()) {
               writer.addRowBatch(orcBatch);
               orcBatch.reset();
            }
         }
      }

      writeSupport.add(record.getRecordKey());
   }

   @Override
   public boolean canWrite() {
      return false;
   }

   @Override
   public void close() throws IOException {

   }

   @Override
   public void writeAvro(String key, R oldRecord) throws IOException {

   }

   public ColumnVector getColumnVector(int pos, TypeDescription.Category fieldType, VectorizedRowBatch orcBatch){
      switch(fieldType) {
         case CHAR:
         case VARCHAR:
         case BINARY:
         case STRING:
            return (BytesColumnVector) orcBatch.cols[pos];
//         case DECIMAL:
//            return (DecimalColumnVector) orcBatch.cols[pos];
         case BOOLEAN:
         case BYTE:
         case DATE:
         case INT:
         case LONG:
         case SHORT:
            return (LongColumnVector) orcBatch.cols[pos];
         case DOUBLE:
         case FLOAT:
            return (DoubleColumnVector) orcBatch.cols[pos];
//         case TIMESTAMP:
//            return (TimestampColumnVector) orcBatch.cols[pos];
         default:
            throw new IllegalArgumentException("Unknown type " + fieldType);
      }
   }
}
