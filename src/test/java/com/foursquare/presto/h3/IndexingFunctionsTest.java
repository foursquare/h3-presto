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
public class IndexingFunctionsTest {
  @Test
  public void testLatLngToCell() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_latlng_to_cell(0,0,0) hex",
          ImmutableList.of(ImmutableList.of(0x8075fffffffffffL)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_latlng_to_cell(null, 0, 4) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_latlng_to_cell(0, 0, null) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_latlng_to_cell(0, 0, -1) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_latlng_to_cell(nan(), 0, 0) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCellToLatLng() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_latlng(from_base('8075fffffffffff', 16))",
          ImmutableList.of(
              ImmutableList.of(ImmutableList.of(2.300882111626747, -5.245390296777327))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_latlng(null)",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_latlng(-1)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCellToBoundary() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_boundary(from_base('8075fffffffffff', 16))",
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
