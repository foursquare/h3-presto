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
public class MiscellaneousFunctionsTest {
  @Test
  public void testGetHexagonAreaAvg() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_hexagon_area_avg(0, 'km2'), h3_get_hexagon_area_avg(1, 'm2'), h3_get_hexagon_area_avg(2, 'km2'), h3_get_hexagon_area_avg(3, 'm2'), h3_get_hexagon_area_avg(4, 'm2'), h3_get_hexagon_area_avg(15, 'km2')",
          ImmutableList.of(
              ImmutableList.of(
                  4357449.416078383,
                  609788441794.1339,
                  86801.7803989972,
                  12393434655.08818,
                  1770347654.491309,
                  8.95311590760579e-7)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_get_hexagon_area_avg(null, 'km2')",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_hexagon_area_avg(0, 'invalid')",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_hexagon_area_avg(-1, 'km2')",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_hexagon_area_avg(255, 'km2')",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCellArea() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_area(from_base('85283473fffffff', 16), 'km2'), h3_cell_area(from_base('85283473fffffff', 16), 'm2'), h3_cell_area(from_base('85283473fffffff', 16), 'rads2')",
          ImmutableList.of(
              ImmutableList.of(265.0925581282742, 2.6509255812827918E8, 0.000006531025010641534)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_area(null, 'km2')",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_area(0, 'invalid')",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_area(-1, 'km2')",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGetHexagonEdgeLengthAvg() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_hexagon_edge_length_avg(0, 'km'), h3_get_hexagon_edge_length_avg(1, 'm'), h3_get_hexagon_edge_length_avg(2, 'km'), h3_get_hexagon_edge_length_avg(3, 'm'), h3_get_hexagon_edge_length_avg(4, 'km'), h3_get_hexagon_edge_length_avg(15, 'm')",
          ImmutableList.of(
              ImmutableList.of(
                  1107.712591, 418676.0055, 158.2446558, 59810.85794, 22.6063794, 0.509713273)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_get_hexagon_edge_length_avg(null, 'km')",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_hexagon_edge_length_avg(0, 'invalid')",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_hexagon_edge_length_avg(-1, 'km')",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_hexagon_edge_length_avg(255, 'km')",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testEdgeLength() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_edge_length(from_base('115283473fffffff', 16), 'km')",
          ImmutableList.of(ImmutableList.of(10.294736086198531)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_edge_length(null, 'km')",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_edge_length(0, 'invalid')",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_edge_length(-1, 'km')",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGreatCircleDistance() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_great_circle_distance(-10, 0, 10, 0, 'km')",
          ImmutableList.of(ImmutableList.of(2223.9010395045884)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_great_circle_distance(null, null, null, null, 'km')",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_great_circle_distance(-10, 0, 10, 0, 'invalid')",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGetNumCells() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_num_cells(0), h3_get_num_cells(15)",
          ImmutableList.of(ImmutableList.of(122L, 569707381193162L)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_get_num_cells(null)",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_num_cells(-10)",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_num_cells(255)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGetRes0Cells() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_res0_cells()",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(
                      0x8001fffffffffffL,
                      0x8003fffffffffffL,
                      0x8005fffffffffffL,
                      0x8007fffffffffffL,
                      0x8009fffffffffffL,
                      0x800bfffffffffffL,
                      0x800dfffffffffffL,
                      0x800ffffffffffffL,
                      0x8011fffffffffffL,
                      0x8013fffffffffffL,
                      0x8015fffffffffffL,
                      0x8017fffffffffffL,
                      0x8019fffffffffffL,
                      0x801bfffffffffffL,
                      0x801dfffffffffffL,
                      0x801ffffffffffffL,
                      0x8021fffffffffffL,
                      0x8023fffffffffffL,
                      0x8025fffffffffffL,
                      0x8027fffffffffffL,
                      0x8029fffffffffffL,
                      0x802bfffffffffffL,
                      0x802dfffffffffffL,
                      0x802ffffffffffffL,
                      0x8031fffffffffffL,
                      0x8033fffffffffffL,
                      0x8035fffffffffffL,
                      0x8037fffffffffffL,
                      0x8039fffffffffffL,
                      0x803bfffffffffffL,
                      0x803dfffffffffffL,
                      0x803ffffffffffffL,
                      0x8041fffffffffffL,
                      0x8043fffffffffffL,
                      0x8045fffffffffffL,
                      0x8047fffffffffffL,
                      0x8049fffffffffffL,
                      0x804bfffffffffffL,
                      0x804dfffffffffffL,
                      0x804ffffffffffffL,
                      0x8051fffffffffffL,
                      0x8053fffffffffffL,
                      0x8055fffffffffffL,
                      0x8057fffffffffffL,
                      0x8059fffffffffffL,
                      0x805bfffffffffffL,
                      0x805dfffffffffffL,
                      0x805ffffffffffffL,
                      0x8061fffffffffffL,
                      0x8063fffffffffffL,
                      0x8065fffffffffffL,
                      0x8067fffffffffffL,
                      0x8069fffffffffffL,
                      0x806bfffffffffffL,
                      0x806dfffffffffffL,
                      0x806ffffffffffffL,
                      0x8071fffffffffffL,
                      0x8073fffffffffffL,
                      0x8075fffffffffffL,
                      0x8077fffffffffffL,
                      0x8079fffffffffffL,
                      0x807bfffffffffffL,
                      0x807dfffffffffffL,
                      0x807ffffffffffffL,
                      0x8081fffffffffffL,
                      0x8083fffffffffffL,
                      0x8085fffffffffffL,
                      0x8087fffffffffffL,
                      0x8089fffffffffffL,
                      0x808bfffffffffffL,
                      0x808dfffffffffffL,
                      0x808ffffffffffffL,
                      0x8091fffffffffffL,
                      0x8093fffffffffffL,
                      0x8095fffffffffffL,
                      0x8097fffffffffffL,
                      0x8099fffffffffffL,
                      0x809bfffffffffffL,
                      0x809dfffffffffffL,
                      0x809ffffffffffffL,
                      0x80a1fffffffffffL,
                      0x80a3fffffffffffL,
                      0x80a5fffffffffffL,
                      0x80a7fffffffffffL,
                      0x80a9fffffffffffL,
                      0x80abfffffffffffL,
                      0x80adfffffffffffL,
                      0x80affffffffffffL,
                      0x80b1fffffffffffL,
                      0x80b3fffffffffffL,
                      0x80b5fffffffffffL,
                      0x80b7fffffffffffL,
                      0x80b9fffffffffffL,
                      0x80bbfffffffffffL,
                      0x80bdfffffffffffL,
                      0x80bffffffffffffL,
                      0x80c1fffffffffffL,
                      0x80c3fffffffffffL,
                      0x80c5fffffffffffL,
                      0x80c7fffffffffffL,
                      0x80c9fffffffffffL,
                      0x80cbfffffffffffL,
                      0x80cdfffffffffffL,
                      0x80cffffffffffffL,
                      0x80d1fffffffffffL,
                      0x80d3fffffffffffL,
                      0x80d5fffffffffffL,
                      0x80d7fffffffffffL,
                      0x80d9fffffffffffL,
                      0x80dbfffffffffffL,
                      0x80ddfffffffffffL,
                      0x80dffffffffffffL,
                      0x80e1fffffffffffL,
                      0x80e3fffffffffffL,
                      0x80e5fffffffffffL,
                      0x80e7fffffffffffL,
                      0x80e9fffffffffffL,
                      0x80ebfffffffffffL,
                      0x80edfffffffffffL,
                      0x80effffffffffffL,
                      0x80f1fffffffffffL,
                      0x80f3fffffffffffL))));
    }
  }

  @Test
  public void testGetPentagons() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_pentagons(0), h3_get_pentagons(15)",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(
                      0x8009fffffffffffL,
                      0x801dfffffffffffL,
                      0x8031fffffffffffL,
                      0x804dfffffffffffL,
                      0x8063fffffffffffL,
                      0x8075fffffffffffL,
                      0x807ffffffffffffL,
                      0x8091fffffffffffL,
                      0x80a7fffffffffffL,
                      0x80c3fffffffffffL,
                      0x80d7fffffffffffL,
                      0x80ebfffffffffffL),
                  ImmutableList.of(
                      0x8f0800000000000L,
                      0x8f1c00000000000L,
                      0x8f3000000000000L,
                      0x8f4c00000000000L,
                      0x8f6200000000000L,
                      0x8f7400000000000L,
                      0x8f7e00000000000L,
                      0x8f9000000000000L,
                      0x8fa600000000000L,
                      0x8fc200000000000L,
                      0x8fd600000000000L,
                      0x8fea00000000000L))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_get_pentagons(null)",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_pentagons(-10)",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_pentagons(255)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }
}
