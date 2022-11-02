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

import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class DirectedEdgeFunctionsTest {
  @Test
  public void testAreNeighborCells() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_are_neighbor_cells(from_base('85283473fffffff', 16), from_base('8528347bfffffff', 16)), h3_are_neighbor_cells(from_base('85283473fffffff', 16), from_base('852836b7fffffff', 16))",
          ImmutableList.of(ImmutableList.of(true, false)));

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
          ImmutableList.of(ImmutableList.of(1320261982812635135L)));

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
                      1248204388774707199L,
                      1320261982812635135L,
                      1392319576850563071L,
                      1464377170888491007L,
                      1536434764926418943L,
                      1608492358964346879L))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_origin_to_directed_edges(0)",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(
                      1224979098644774912L,
                      1297036692682702848L,
                      1369094286720630784L,
                      1441151880758558720L,
                      1513209474796486656L,
                      1585267068834414592L))));
      assertQueryResults(
          queryRunner,
          "SELECT h3_origin_to_directed_edges(null)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testDirectedEdgeToBoundary() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_directed_edge_to_boundary(h3_cells_to_directed_edge(from_base('85283473fffffff', 16), from_base('8528347bfffffff', 16)))",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(
                      37.353926450852256,
                      -121.86222328902491,
                      37.42834118609436,
                      -121.92354999630156))));

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
