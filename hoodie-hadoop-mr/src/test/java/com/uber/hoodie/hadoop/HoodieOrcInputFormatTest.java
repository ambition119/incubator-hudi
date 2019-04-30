/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.hadoop;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HoodieOrcInputFormatTest {
  private HoodieOrcInputFormat inputFormat;
  private JobConf jobConf;

  @Before
  public void setUp() {
    inputFormat = new HoodieOrcInputFormat();
    jobConf = new JobConf();
    jobConf.set("hive.exec.orc.split.strategy","ETL");
    inputFormat.setConf(jobConf);
  }

  @Rule
  public TemporaryFolder basePath = new TemporaryFolder();

  @Test
  public void testInputFormatLoad() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareOrcDataset(basePath, 10, "100");
    InputFormatTestUtil.commit(basePath, "100");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    InputSplit[] inputSplits = inputFormat.getSplits(jobConf, 10);
  }

  @Test
  public void testIncrementalSimple() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareOrcDataset(basePath, 10, "100");
    InputFormatTestUtil.commit(basePath, "100");

    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());

    InputFormatTestUtil.setupIncremental(jobConf, "100", 1);
  }

  @Test
  public void testIncrementalWithMultipleCommits() throws IOException {
    // initial commit
    File partitionDir = InputFormatTestUtil.prepareOrcDataset(basePath, 10, "100");
    InputFormatTestUtil.commit(basePath, "100");
    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
    // update files
    InputFormatTestUtil.simulateUpdates(partitionDir, "100", 5, "200", false);
    InputFormatTestUtil.commit(basePath, "200");

    InputFormatTestUtil.simulateUpdates(partitionDir, "100", 4, "300", false);
    InputFormatTestUtil.commit(basePath, "300");

    InputFormatTestUtil.simulateUpdates(partitionDir, "100", 3, "400", false);
    InputFormatTestUtil.commit(basePath, "400");

    InputFormatTestUtil.simulateUpdates(partitionDir, "100", 2, "500", false);
    InputFormatTestUtil.commit(basePath, "500");

    InputFormatTestUtil.simulateUpdates(partitionDir, "100", 1, "600", false);
    InputFormatTestUtil.commit(basePath, "600");

    InputFormatTestUtil.setupIncremental(jobConf, "100", 1);
  }

}
