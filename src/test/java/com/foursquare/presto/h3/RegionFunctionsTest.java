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
          "SELECT h3_polygon_to_cells(ARRAY [0, 0, 1, 1, 1,  0], ARRAY [], 4) hex",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(
                      596538805289222143L,
                      596538856828829695L,
                      596537920525959167L,
                      596538839648960511L))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_polygon_to_cells(null, null, 4) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_polygon_to_cells(ARRAY [0, 0, 1, 1, 2, 0], ARRAY [ARRAY [0, 0, 1, 1, 0, 1]], null) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  // TODO
  //   @Test
  //   public void testCellsToMultiPolygon() {
  //     try (QueryRunner queryRunner = createQueryRunner()) {
  //       assertQueryResults(
  //           queryRunner,
  //           "SELECT h3_cells_to_multi_polygon(ARRAY [from_base('85283473fffffff', 16)], true)
  // multipolygon",
  //           ImmutableList.of(
  //               ImmutableList.of(
  //                   ImmutableList.of(
  //                     // TODO
  //                   ))));

  //       assertQueryResults(
  //           queryRunner,
  //           "SELECT h3_cells_to_multi_polygon(null, true) multipolygon",
  //           ImmutableList.of(Collections.singletonList(null)));
  //       assertQueryResults(
  //           queryRunner,
  //           "SELECT h3_cells_to_multi_polygon(ARRAY [from_base('85283473fffffff', 16)], null)
  // multipolygon",
  //           ImmutableList.of(Collections.singletonList(null)));
  //     }
  //   }
}
