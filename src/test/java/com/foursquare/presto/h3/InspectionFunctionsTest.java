package com.foursquare.presto.h3;

import static com.foursquare.presto.h3.H3PluginTest.assertQueryResults;
import static com.foursquare.presto.h3.H3PluginTest.createQueryRunner;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class InspectionFunctionsTest {
  @Test
  public void testConstructor() {
    assertNotNull(new InspectionFunctions());
  }

  @Test
  public void testGetResolution() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_resolution(from_base('85283473fffffff', 16))",
          ImmutableList.of(ImmutableList.of(5)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_get_resolution(null)",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_resolution(-1)",
          ImmutableList.of(Collections.singletonList(15)));
    }
  }

  @Test
  public void testGetBaseCellNumber() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_base_cell_number(from_base('85283473fffffff', 16))",
          ImmutableList.of(ImmutableList.of(20)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_get_base_cell_number(null)",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_base_cell_number(-1)",
          ImmutableList.of(Collections.singletonList(127)));
    }
  }

  @Test
  public void testStringToH3() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner, "SELECT h3_string_to_h3('0')", ImmutableList.of(ImmutableList.of(0L)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_string_to_h3('85283473fffffff')",
          ImmutableList.of(ImmutableList.of(0x85283473fffffffL)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_string_to_h3(null)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testH3ToString() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner, "SELECT h3_h3_to_string(0)", ImmutableList.of(ImmutableList.of("0")));
      assertQueryResults(
          queryRunner,
          "SELECT h3_h3_to_string(599686042433355775)",
          ImmutableList.of(ImmutableList.of("85283473fffffff")));

      assertQueryResults(
          queryRunner,
          "SELECT h3_h3_to_string(null)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testIsValidCell() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner, "SELECT h3_is_valid_cell(0)", ImmutableList.of(ImmutableList.of(false)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_is_valid_cell(from_base('85283473fffffff', 16))",
          ImmutableList.of(ImmutableList.of(true)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_is_valid_cell(null)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testIsResClassIII() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_is_res_class_iii(0), h3_is_res_class_iii(-1)",
          ImmutableList.of(ImmutableList.of(false, true)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_is_res_class_iii(from_base('85283473fffffff', 16))",
          ImmutableList.of(ImmutableList.of(true)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_is_res_class_iii(h3_cell_to_parent(from_base('85283473fffffff', 16), 4))",
          ImmutableList.of(ImmutableList.of(false)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_is_res_class_iii(null)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testIsPentagon() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_is_pentagon(0), h3_is_pentagon(-1)",
          ImmutableList.of(ImmutableList.of(false, false)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_is_pentagon(from_base('801dfffffffffff', 16)), h3_is_pentagon(from_base('85283473fffffff', 16))",
          ImmutableList.of(ImmutableList.of(true, false)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_is_pentagon(null)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGetIcosahedronFaces() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_icosahedron_faces(from_base('801dfffffffffff', 16)), h3_get_icosahedron_faces(from_base('85283473fffffff', 16))",
          ImmutableList.of(
              ImmutableList.of(ImmutableList.of(1, 6, 11, 7, 2), ImmutableList.of(7))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_get_icosahedron_faces(null)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }
}
