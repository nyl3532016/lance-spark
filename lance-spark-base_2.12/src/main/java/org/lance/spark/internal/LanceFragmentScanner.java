/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.internal;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ReadOptions;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.namespace.LanceNamespaceStorageOptionsProvider;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.read.LanceInputPartition;

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LanceFragmentScanner implements AutoCloseable {
  private final Dataset dataset;
  private final LanceScanner scanner;
  private final int fragmentId;
  private final boolean withFragemtId;
  private final LanceInputPartition inputPartition;

  private LanceFragmentScanner(
      Dataset dataset,
      LanceScanner scanner,
      int fragmentId,
      boolean withFragmentId,
      LanceInputPartition inputPartition) {
    this.dataset = dataset;
    this.scanner = scanner;
    this.fragmentId = fragmentId;
    this.withFragemtId = withFragmentId;
    this.inputPartition = inputPartition;
  }

  public static LanceFragmentScanner create(int fragmentId, LanceInputPartition inputPartition) {
    Dataset dataset = null;
    try {
      LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
      Map<String, String> merged =
          LanceRuntime.mergeStorageOptions(
              readOptions.getStorageOptions(), inputPartition.getInitialStorageOptions());
      LanceNamespaceStorageOptionsProvider provider =
          LanceRuntime.getOrCreateStorageOptionsProvider(
              inputPartition.getNamespaceImpl(),
              inputPartition.getNamespaceProperties(),
              readOptions.getTableId());

      ReadOptions.Builder builder = new ReadOptions.Builder().setStorageOptions(merged);
      if (provider != null) {
        builder.setStorageOptionsProvider(provider);
      }

      dataset =
          Dataset.open()
              .allocator(LanceRuntime.allocator())
              .uri(readOptions.getDatasetUri())
              .readOptions(builder.build())
              .build();
      Fragment fragment = dataset.getFragment(fragmentId);
      if (fragment == null) {
        throw new IllegalStateException("Fragment " + fragmentId + " not found");
      }

      ScanOptions.Builder scanOptions = new ScanOptions.Builder();
      scanOptions.columns(getColumnNames(inputPartition.getSchema()));
      if (inputPartition.getWhereCondition().isPresent()) {
        scanOptions.filter(inputPartition.getWhereCondition().get());
      }
      scanOptions.batchSize(readOptions.getBatchSize());
      scanOptions.withRowId(getWithRowId(inputPartition.getSchema()));
      scanOptions.withRowAddress(getWithRowAddress(inputPartition.getSchema()));
      scanOptions.prefilter(readOptions.isPrefilter());

      if (readOptions.getNearest() != null) {
        scanOptions.nearest(readOptions.getNearest());
        // We can allow fragment scan if the input to nearest is a prefilter.
        scanOptions.prefilter(true);
      }
      if (inputPartition.getLimit().isPresent()) {
        scanOptions.limit(inputPartition.getLimit().get());
      }
      if (inputPartition.getOffset().isPresent()) {
        scanOptions.offset(inputPartition.getOffset().get());
      }
      if (inputPartition.getTopNSortOrders().isPresent()) {
        scanOptions.setColumnOrderings(inputPartition.getTopNSortOrders().get());
      }
      boolean withFragmentId =
          inputPartition.getSchema().getFieldIndex(LanceConstant.FRAGMENT_ID).nonEmpty();
      return new LanceFragmentScanner(
          dataset,
          fragment.newScan(scanOptions.build()),
          fragmentId,
          withFragmentId,
          inputPartition);
    } catch (Throwable throwable) {
      if (dataset != null) {
        try {
          dataset.close();
        } catch (Throwable closeError) {
          throwable.addSuppressed(closeError);
        }
      }
      throw new RuntimeException(throwable);
    }
  }

  /**
   * @return the arrow reader. The caller is responsible for closing the reader
   */
  public ArrowReader getArrowReader() {
    return scanner.scanBatches();
  }

  @Override
  public void close() throws IOException {
    Throwable primary = null;
    if (scanner != null) {
      try {
        scanner.close();
      } catch (Throwable t) {
        primary = t;
      }
    }
    if (dataset != null) {
      try {
        dataset.close();
      } catch (Throwable t) {
        if (primary != null) {
          primary.addSuppressed(t);
        } else {
          primary = t;
        }
      }
    }
    if (primary != null) {
      if (primary instanceof IOException) {
        throw (IOException) primary;
      }
      throw new IOException(primary);
    }
  }

  public int fragmentId() {
    return fragmentId;
  }

  public boolean withFragemtId() {
    return withFragemtId;
  }

  public LanceInputPartition getInputPartition() {
    return inputPartition;
  }

  private static List<String> getColumnNames(StructType schema) {
    return Arrays.stream(schema.fields())
        .map(StructField::name)
        .filter(
            name ->
                !name.equals(LanceConstant.FRAGMENT_ID)
                    && !name.equals(LanceConstant.ROW_ID)
                    && !name.equals(LanceConstant.ROW_ADDRESS)
                    && !name.endsWith(LanceConstant.BLOB_POSITION_SUFFIX)
                    && !name.endsWith(LanceConstant.BLOB_SIZE_SUFFIX))
        .collect(Collectors.toList());
  }

  private static boolean getWithRowId(StructType schema) {
    return Arrays.stream(schema.fields())
        .map(StructField::name)
        .anyMatch(name -> name.equals(LanceConstant.ROW_ID));
  }

  private static boolean getWithRowAddress(StructType schema) {
    return Arrays.stream(schema.fields())
        .map(StructField::name)
        .anyMatch(name -> name.equals(LanceConstant.ROW_ADDRESS));
  }
}
