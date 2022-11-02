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
public class TraversalFunctionsTest {
  @Test
  public void testGridDisk() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk(from_base('85283473fffffff', 16), 0), h3_grid_disk(from_base('85283473fffffff', 16), 2)",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(0x85283473fffffffL),
                  ImmutableList.of(
                      599686042433355775L,
                      599686030622195711L,
                      599686044580839423L,
                      599686038138388479L,
                      599686043507097599L,
                      599686015589810175L,
                      599686014516068351L,
                      599686034917163007L,
                      599686029548453887L,
                      599686032769679359L,
                      599686198125920255L,
                      599686040285872127L,
                      599686041359613951L,
                      599686039212130303L,
                      599686023106002943L,
                      599686027400970239L,
                      599686013442326527L,
                      599686012368584703L,
                      599686018811035647L))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk(0, 4) hex",
          ImmutableList.of(ImmutableList.of(ImmutableList.of())));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk(null, 4) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk(from_base('85283473fffffff', 16), null) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk(from_base('85283473fffffff', 16), -1) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGridDiskUnsafe() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_unsafe(from_base('85283473fffffff', 16), 0), h3_grid_disk_unsafe(from_base('85283473fffffff', 16), 2)",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(0x85283473fffffffL),
                  ImmutableList.of(
                      599686042433355775L,
                      599686030622195711L,
                      599686044580839423L,
                      599686038138388479L,
                      599686043507097599L,
                      599686015589810175L,
                      599686014516068351L,
                      599686034917163007L,
                      599686029548453887L,
                      599686032769679359L,
                      599686198125920255L,
                      599686040285872127L,
                      599686041359613951L,
                      599686039212130303L,
                      599686023106002943L,
                      599686027400970239L,
                      599686013442326527L,
                      599686012368584703L,
                      599686018811035647L))));
      // Pentagon:
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_unsafe(from_base('811c3ffffffffff', 16), 1) hex",
          ImmutableList.of(Collections.singletonList(null)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_unsafe(0, 4) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_unsafe(null, 4) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_unsafe(from_base('85283473fffffff', 16), null) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_disk_unsafe(from_base('85283473fffffff', 16), -1) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGridRingUnsafe() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring_unsafe(from_base('85283473fffffff', 16), 0), h3_grid_ring_unsafe(from_base('85283473fffffff', 16), 2)",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(0x85283473fffffffL),
                  ImmutableList.of(
                      599686018811035647L,
                      599686034917163007L,
                      599686029548453887L,
                      599686032769679359L,
                      599686198125920255L,
                      599686040285872127L,
                      599686041359613951L,
                      599686039212130303L,
                      599686023106002943L,
                      599686027400970239L,
                      599686013442326527L,
                      599686012368584703L))));
      // Pentagon:
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring_unsafe(from_base('811c3ffffffffff', 16), 1) hex",
          ImmutableList.of(Collections.singletonList(null)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring_unsafe(0, 4) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring_unsafe(null, 4) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring_unsafe(from_base('85283473fffffff', 16), null) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_ring_unsafe(from_base('85283473fffffff', 16), -1) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGridPathCells() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_path_cells(from_base('85283473fffffff', 16), from_base('8528342ffffffff', 16)) hex",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(
                      0x85283473fffffffL,
                      0x85283477fffffffL,
                      0x8528342bfffffffL,
                      0x8528342ffffffffL))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_path_cells(0, from_base('8528342ffffffff', 16)) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_path_cells(null, from_base('8528342ffffffff', 16)) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_path_cells(from_base('8528342ffffffff', 16), 0) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_path_cells(from_base('8528342ffffffff', 16), null) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testGridDistance() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_distance(from_base('85283473fffffff', 16), from_base('8528342ffffffff', 16)) hex",
          ImmutableList.of(ImmutableList.of(3L)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_distance(0, from_base('8528342ffffffff', 16)) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_distance(null, from_base('8528342ffffffff', 16)) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_distance(from_base('8528342ffffffff', 16), 0) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_grid_distance(from_base('8528342ffffffff', 16), null) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCellToLocalIj() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_local_ij(from_base('85283473fffffff', 16), from_base('8528342ffffffff', 16)) hex",
          ImmutableList.of(ImmutableList.of(ImmutableList.of(24, 12))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_local_ij(0, from_base('8528342ffffffff', 16)) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_local_ij(null, from_base('8528342ffffffff', 16)) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_local_ij(from_base('8528342ffffffff', 16), 0) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_local_ij(from_base('8528342ffffffff', 16), null) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testLocalIjToCell() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_local_ij_to_cell(from_base('85283473fffffff', 16), 0, 0) hex",
          ImmutableList.of(ImmutableList.of(599682438955794431L)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_local_ij_to_cell(null, 0, 0) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_local_ij_to_cell(from_base('8528342ffffffff', 16), null, 0) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_local_ij_to_cell(from_base('8528342ffffffff', 16), 0, null) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_local_ij_to_cell(from_base('8528342ffffffff', 16), null, null) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }
}
