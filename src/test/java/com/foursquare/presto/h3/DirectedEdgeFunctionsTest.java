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

import static com.foursquare.presto.h3.H3PluginTest.assertQueryResults;
import static com.foursquare.presto.h3.H3PluginTest.createQueryRunner;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

@TestInstance(Lifecycle.PER_CLASS)
public class DirectedEdgeFunctionsTest {
  @Test
  public void testConstructor() {
    assertNotNull(new DirectedEdgeFunctions());
  }

  @Test
  public void testAreNeighborCells() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_are_neighbor_cells(from_base('85283473fffffff', 16), from_base('8528347bfffffff', 16))",
          ImmutableList.of(ImmutableList.of(true)));
      // Non-neighbors
      assertQueryResults(
          queryRunner,
          "SELECT h3_are_neighbor_cells(from_base('85283473fffffff', 16), from_base('852836b7fffffff', 16))",
          ImmutableList.of(ImmutableList.of(false)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_are_neighbor_cells(0, from_base('85283473fffffff', 16)) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_are_neighbor_cells(null, from_base('85283473fffffff', 16)) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_are_neighbor_cells(from_base('85283473fffffff', 16), null) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_are_neighbor_cells(from_base('85283473fffffff', 16), -1) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCellsToDirectedEdge() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cells_to_directed_edge(from_base('85283473fffffff', 16), from_base('8528347bfffffff', 16))",
          ImmutableList.of(ImmutableList.of(0x125283473fffffffL)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cells_to_directed_edge(0, from_base('85283473fffffff', 16)) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cells_to_directed_edge(null, from_base('85283473fffffff', 16)) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cells_to_directed_edge(from_base('85283473fffffff', 16), null) hex",
          ImmutableList.of(Collections.singletonList(null)));
      // Non-neighboring cells return an error
      assertQueryResults(
          queryRunner,
          "SELECT h3_cells_to_directed_edge(from_base('85283473fffffff', 16), from_base('852836b7fffffff', 16)) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGetDirectedEdgeOrigin() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_directed_edge_origin(h3_cells_to_directed_edge(from_base('85283473fffffff', 16), from_base('8528347bfffffff', 16)))",
          ImmutableList.of(ImmutableList.of(0x85283473fffffffL)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_get_directed_edge_origin(0)",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_directed_edge_origin(null)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGetDirectedEdgeDestination() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_directed_edge_destination(h3_cells_to_directed_edge(from_base('85283473fffffff', 16), from_base('8528347bfffffff', 16)))",
          ImmutableList.of(ImmutableList.of(0x8528347bfffffffL)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_get_directed_edge_destination(0)",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_get_directed_edge_destination(null)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testDirectedEdgeToCells() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_directed_edge_to_cells(h3_cells_to_directed_edge(from_base('85283473fffffff', 16), from_base('8528347bfffffff', 16)))",
          ImmutableList.of(
              ImmutableList.of(ImmutableList.of(0x85283473fffffffL, 0x8528347bfffffffL))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_directed_edge_to_cells(0)",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_directed_edge_to_cells(null)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testOriginToDirectedEdges() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_origin_to_directed_edges(from_base('85283473fffffff', 16))",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(
                      0x115283473fffffffL,
                      0x125283473fffffffL,
                      0x135283473fffffffL,
                      0x145283473fffffffL,
                      0x155283473fffffffL,
                      0x165283473fffffffL))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_origin_to_directed_edges(0)",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(
                      0x1100000000000000L,
                      0x1200000000000000L,
                      0x1300000000000000L,
                      0x1400000000000000L,
                      0x1500000000000000L,
                      0x1600000000000000L))));
      assertQueryResults(
          queryRunner,
          "SELECT h3_origin_to_directed_edges(null)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testDirectedEdgeToBoundary() throws ParseException {
    try (QueryRunner queryRunner = createQueryRunner()) {
      GeometryFactory geometryFactory = new GeometryFactory();
      WKTReader wktReader = new WKTReader(geometryFactory);
      Geometry expectedLineString =
          wktReader.read(
              "LINESTRING (-121.86222328902491 37.353926450852256, -121.92354999630156 37.42834118609436)");
      assertQueryResults(
          queryRunner,
          "SELECT ST_AsText(h3_directed_edge_to_boundary(h3_cells_to_directed_edge(from_base('85283473fffffff', 16), from_base('8528347bfffffff', 16))))",
          ImmutableList.of(ImmutableList.of(expectedLineString)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_directed_edge_to_boundary(0)",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_directed_edge_to_boundary(null)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }
}
