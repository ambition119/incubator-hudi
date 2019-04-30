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

package com.uber.hoodie.func;

import com.uber.hoodie.common.util.queue.BoundedInMemoryQueue;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.hive.ql.io.orc.Reader;

/**
 * This class wraps a orc reader and provides an iterator based api to
 * read from a orc file. This is used in {@link BoundedInMemoryQueue}
 */
public class OrcReaderIterator<T> implements Iterator<T> {

  // Orc reader for an existing orc file
  private final Reader orcReader;
  // Holds the next entry returned by the orc reader
  private T next;

  public OrcReaderIterator(Reader orcReader) {
    this.orcReader = orcReader;
  }

  @Override
  public boolean hasNext() {
    // To handle when hasNext() is called multiple times for idempotency and/or the first time
    if (this.next == null) {
      //  this.next = orcReader.hasMetadataValue()
    }
    return this.next != null;
  }

  @Override
  public T next() {
    return null;
  }

  public void close() throws IOException {

  }
}
