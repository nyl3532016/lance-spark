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
package org.lance.spark.read;

import org.lance.Dataset;
import org.lance.ipc.Query;
import org.lance.ipc.ScanOptions;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.internal.LanceFragmentColumnarBatchScanner;
import org.lance.spark.utils.Optional;
import org.lance.spark.utils.QueryUtils;

import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.lance.spark.TestUtils.getDatasetUri;

public class LanceFragmentColumnarBatchScannerTest {

  @Test
  public void scanner() throws IOException {
    LanceSparkReadOptions readOptions;
    Query.Builder builder = new Query.Builder();
    float[] key = new float[32];
    for (int i = 0; i < 32; i++) {
      key[i] = (float) (i + 32);
    }
    builder.setK(5);
    builder.setColumn("vec");
    //    builder.setRefineFactor(2);
    //    builder.setKey(
    //        new float[]
    // {-1.164561f,1.4541137f,0.6925452f,-0.17859168f,0.422055f,1.3849078f,0.5930743f,-1.0516955f,-0.4660289f,0.4688979f
    //        });
    builder.setKey(key);
    builder.setUseIndex(false);

    Query query = builder.build();

    String str = QueryUtils.queryToString(query);

    String datasetUri = getDatasetUri(TestUtils.TestTable1Config.dbPath, "test_dataset7");
    Map<String, String> properties = new HashMap<>();

    properties.put(LanceSparkReadOptions.CONFIG_NEAREST, str);
    properties.put(LanceSparkReadOptions.CONFIG_BATCH_SIZE, "1");
    properties.put(LanceSparkReadOptions.CONFIG_PREFILTER, "true");
    readOptions = LanceSparkReadOptions.from(properties, datasetUri);
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("i", DataTypes.IntegerType, true),
              DataTypes.createStructField("s", DataTypes.StringType, true),
              DataTypes.createStructField(
                  "vec", DataTypes.createArrayType(DataTypes.FloatType), true),
            });

    System.out.println("niuyulin in LanceFragmentColumnarBatchScannerTest");
    System.out.println(QueryUtils.queryToString(readOptions.getNearest()));
    LanceInputPartition inputPartition =
        new LanceInputPartition(
            schema,
            0 /* partitionId */,
            new LanceSplit(Arrays.asList(0)),
            readOptions,
            org.lance.spark.utils.Optional.empty() /* whereCondition */,
            org.lance.spark.utils.Optional.empty() /* limit */,
            org.lance.spark.utils.Optional.empty() /* offset */,
            org.lance.spark.utils.Optional.empty() /* topNSortOrders */,
            Optional.empty() /* pushedAggregation */,
            "test" /* scanId */,
            null /* initialStorageOptions */,
            null /* namespaceImpl */,
            null /* namespaceProperties */);

    List<List<Long>> expectedValues = TestUtils.TestTable1Config.expectedValues;
    int rowIndex = 0;
    int fragmentId = 0;
    while (fragmentId < 1) {
      try (LanceFragmentColumnarBatchScanner scanner =
          LanceFragmentColumnarBatchScanner.create(fragmentId, inputPartition)) {
        while (scanner.loadNextBatch()) {
          try (ColumnarBatch batch = scanner.getCurrentBatch()) {
            Iterator<InternalRow> rows = batch.rowIterator();
            while (rows.hasNext()) {
              InternalRow row = rows.next();
              assertNotNull(row);
              //              for (int colIndex = 0; colIndex < row.numFields(); colIndex++) {
              //                long actualValue = row.getLong(colIndex);
              //                long expectedValue = expectedValues.get(rowIndex).get(colIndex);
              //                assertEquals(
              //                    expectedValue,
              //                    actualValue,
              //                    "Mismatch at row " + rowIndex + " column " + colIndex);
              //              }
              rowIndex++;
            }
            System.out.println("row size is " + rowIndex);
          }
        }
      }
      fragmentId++;
    }
  }

  @Test
  void test_knn() throws Exception {
    String datasetUri = getDatasetUri(TestUtils.TestTable1Config.dbPath, "test_dataset7");
    Dataset dataset = Dataset.open(datasetUri);

    float[] key = new float[32];
    for (int i = 0; i < 32; i++) {
      key[i] = (float) (i + 32);
    }
    ScanOptions options =
        new ScanOptions.Builder()
            .fragmentIds(Collections.singletonList(0))
            .prefilter(true)
            .nearest(
                new Query.Builder().setColumn("vec").setKey(key).setK(5).setUseIndex(false).build())
            .build();
    System.out.println("scan option is " + options);

    try (Scanner scanner = dataset.newScan(options)) {
      try (ArrowReader reader = scanner.scanBatches()) {
        System.out.println("reader class is " + reader.getClass());
        System.out.println("scanner class is " + scanner.getClass());

        VectorSchemaRoot root = reader.getVectorSchemaRoot();

        System.out.println("Schema:");
        assertTrue(reader.loadNextBatch(), "Expected at least one batch");
        System.out.println("niuyulin row size  is " + root.getRowCount());
        System.out.println("niuyulin row schema " + root.getSchema());
        assertEquals(5, root.getRowCount(), "Expected 5 results");

        assertEquals(4, root.getSchema().getFields().size(), "Expected 4 columns");
        assertEquals("i", root.getSchema().getFields().get(0).getName());
        assertEquals("s", root.getSchema().getFields().get(1).getName());
        assertEquals("vec", root.getSchema().getFields().get(2).getName());
        assertEquals("_distance", root.getSchema().getFields().get(3).getName());

        IntVector iVector = (IntVector) root.getVector("i");
        Set<Integer> expectedI = new HashSet<>(Arrays.asList(1, 81, 161, 241, 321));
        Set<Integer> actualI = new HashSet<>();
        for (int i = 0; i < iVector.getValueCount(); i++) {
          actualI.add(iVector.get(i));
        }
        assertEquals(expectedI, actualI, "Unexpected values in 'i' column");

        Float4Vector distanceVector = (Float4Vector) root.getVector("_distance");
        float prevDistance = Float.NEGATIVE_INFINITY;
        for (int i = 0; i < distanceVector.getValueCount(); i++) {
          float distance = distanceVector.get(i);
          assertTrue(distance >= prevDistance, "Distances should be in ascending order");
          prevDistance = distance;
        }

        assertFalse(reader.loadNextBatch(), "Expected only one batch");
      }
    }
  }
}
