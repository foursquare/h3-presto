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
public class RegionFunctionsTest {
  @Test
  public void testPolygonToCells() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_polygon_to_cells(ST_GeometryFromText('POLYGON ((0 0, 1 1, 1 0, 0 0))'), 4) hex",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(
                      596538848238895103L,
                      596538813879156735L,
                      596538685030137855L,
                      596538693620072447L))));
      // TODO: Test with holes

      assertQueryResults(
          queryRunner,
          "SELECT h3_polygon_to_cells(null, 4) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_polygon_to_cells(ST_GeometryFromText('POLYGON ((0 0, 1 1, 1 0, 0 0))'), null) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCellsToMultiPolygon() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT ST_AsText(h3_cells_to_multi_polygon(ARRAY [from_base('85283473fffffff', 16)])) multipolygon",
          ImmutableList.of(
              ImmutableList.of(
                  "MULTIPOLYGON (((-121.92354999630156 37.42834118609436, -121.86222328902491 37.353926450852256, -121.91508032705622 37.2713558667319, -122.02910130918998 37.26319797461824, -122.090428929044 37.33755608435299, -122.03773496427027 37.42012867767779, -121.92354999630156 37.42834118609436)))")));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cells_to_multi_polygon(null) multipolygon",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT ST_AsText(h3_cells_to_multi_polygon(ARRAY [])) multipolygon",
          ImmutableList.of(ImmutableList.of("MULTIPOLYGON EMPTY")));
    }
  }
}
