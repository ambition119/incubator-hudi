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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;

public class HoodieOrcConfig {
  private HoodieAvroWriteSupport writeSupport;
  private int blockSize;
  private Configuration hadoopConf;
  // OrcFile param
  private long stripeSize;
  private int bufferSize;
  private int rowIndexStride;
  private CompressionKind compressionKind;

  public HoodieOrcConfig(HoodieAvroWriteSupport writeSupport, Configuration hadoopConf, CompressionKind compressionKind,
      int blockSize, long stripeSize, int bufferSize, int rowIndexStride) {
    this.writeSupport = writeSupport;
    this.blockSize = blockSize;
    this.hadoopConf = hadoopConf;
    this.compressionKind = compressionKind;
    this.stripeSize = stripeSize;
    this.bufferSize = bufferSize;
    this.rowIndexStride = rowIndexStride;
  }

  public HoodieAvroWriteSupport getWriteSupport() {
    return writeSupport;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public long getStripeSize() {
    return stripeSize;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public int getRowIndexStride() {
    return rowIndexStride;
  }

  public CompressionKind getCompressionKind() {
    return compressionKind;
  }
}
