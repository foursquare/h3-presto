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
public class LatLngToCellFunctionTest {
  @Test
  public void testLatLngToCell() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_latlng_to_cell(0,0,0) hex",
          ImmutableList.of(ImmutableList.of(578536630256664575L)));

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
}
