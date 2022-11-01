package com.foursquare.presto.h3;

import static com.foursquare.presto.h3.H3PluginTest.assertQueryResults;
import static com.foursquare.presto.h3.H3PluginTest.createQueryRunner;

import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class CellToBoundaryFunctionTest {
  @Test
  public void testCellToBoundary() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_boundary(578536630256664575)",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(
                      11.545295975414755,
                      -4.013998443470464,
                      6.270965136275774,
                      -13.708146703917999,
                      -4.467031609784516,
                      -11.66474754212643,
                      -5.889921754313907,
                      -0.7828391751055211,
                      3.968796976609579,
                      3.9430361557864506))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_boundary(null)",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_boundary(-1)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }
}
