package com.uber.hoodie.io.storage;

import com.uber.hoodie.avro.HoodieAvroWriteSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.CompressionKind;

/**
 * @Author: wpl
 */
public class HoodieOrcConfigV2 {

  private HoodieAvroWriteSupport writeSupport;
  private int blockSize;
  private Configuration hadoopConf;
  // OrcFile param
  private long stripeSize;
  private int bufferSize;
  private int rowIndexStride;
  private CompressionKind compressionKind;

  public HoodieOrcConfigV2(HoodieAvroWriteSupport writeSupport, Configuration hadoopConf,
      CompressionKind compressionKind,
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
