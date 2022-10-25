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
public class CellToLatLngFunctionTest {
  @Test
  public void testCellToLatLng() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_latlng(578536630256664575)",
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
}
