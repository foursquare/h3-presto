/*
 * Copyright 2022 Foursquare Labs, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.foursquare.presto.h3;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

@TestInstance(Lifecycle.PER_CLASS)
public class H3PluginTest {
  public static final double EPSILON = 1e-6;

  @Test
  public void testConstructor() {
    assertNotNull(new H3Plugin());
  }

  @Test
  public void TestH3Plugin() {
    try (DistributedQueryRunner queryRunner = createQueryRunner()) {
      System.out.println(queryRunner.execute("SELECT 1=1"));
    }
  }

  public static <T> void assertQueryResults(
      QueryRunner queryRunner, String sql, List<List<T>> expected) {
    MaterializedResult results = queryRunner.execute(sql);
    List<MaterializedRow> rows = results.getMaterializedRows().stream().collect(toImmutableList());
    assertEquals(expected.size(), rows.size(), String.format("%s: expected number of rows", sql));
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(
          expected.get(i).size(),
          rows.get(i).getFieldCount(),
          String.format("%s: row %d: expected number of columns", sql, i));
      for (int j = 0; j < expected.get(i).size(); j++) {
        Object expectedVal = expected.get(i).get(j);
        Object actualVal = rows.get(i).getField(j);
        compareFieldValues(
            expectedVal, actualVal, String.format("%s: row %d: column %d", sql, i, j));
      }
    }
  }

  private static void compareFieldValues(Object expectedVal, Object actualVal, String message) {
    if (expectedVal instanceof Float && actualVal instanceof Float) {
      assertEquals(
          ((Float) expectedVal).floatValue(),
          ((Float) actualVal).floatValue(),
          EPSILON,
          String.format("%s: value matches within epsilon", message));
    } else if (expectedVal instanceof Double && actualVal instanceof Double) {
      assertEquals(
          ((Double) expectedVal).doubleValue(),
          ((Double) actualVal).doubleValue(),
          EPSILON,
          String.format("%s: value matches within epsilon", message));
    } else if (expectedVal instanceof Geometry && actualVal instanceof String) {
      try {
        GeometryFactory geometryFactory = new GeometryFactory();
        WKTReader wktReader = new WKTReader(geometryFactory);
        Geometry actualGeometry = wktReader.read((String) actualVal);
        assertTrue(
            ((Geometry) expectedVal).equalsExact(actualGeometry, EPSILON),
            String.format(
                "%s: value matches within epsilon (expected: %s actual: %s)",
                message, expectedVal, actualGeometry));
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }
    } else if (expectedVal instanceof List && actualVal instanceof List) {
      List<?> expectedValList = (List<?>) expectedVal;
      List<?> actualValList = (List<?>) actualVal;
      assertEquals(
          expectedValList.size(),
          actualValList.size(),
          String.format(
              "%s: list lengths match (expected: %s actual: %s)",
              message, expectedValList.toString(), actualValList.toString()));
      for (int k = 0; k < expectedValList.size(); k++) {
        compareFieldValues(
            expectedValList.get(k),
            actualValList.get(k),
            String.format(
                "%s: list item %d (expected: %s actual: %s)",
                message, k, expectedValList.toString(), actualValList.toString()));
      }
    } else {
      assertEquals(expectedVal, actualVal, String.format("%s: value matches", message));
    }
  }

  public static DistributedQueryRunner createQueryRunner() {
    try {
      Session session = testSessionBuilder().build();
      Map<String, String> properties = ImmutableMap.of();
      DistributedQueryRunner queryRunner =
          DistributedQueryRunner.builder(session)
              .setNodeCount(1)
              .setExtraProperties(properties)
              .build();

      try {
        queryRunner.installPlugin(new H3Plugin());
        return queryRunner;
      } catch (Exception e) {
        queryRunner.close();
        throw e;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
