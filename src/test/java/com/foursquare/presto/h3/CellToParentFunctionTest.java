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
public class CellToParentFunctionTest {
  @Test
  public void testCellToParent() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_parent(from_base('85283473fffffff', 16), 4) hex",
          ImmutableList.of(ImmutableList.of(0x8428347ffffffffL)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_parent(0, 4) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_parent(null, 4) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_parent(from_base('85283473fffffff', 16), null) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_parent(from_base('85283473fffffff', 16), -1) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }
}
