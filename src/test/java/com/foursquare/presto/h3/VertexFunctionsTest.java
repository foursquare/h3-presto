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
public class VertexFunctionsTest {
  @Test
  public void testCellToVertex() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertex(from_base('85283473fffffff', 16), 0), h3_cell_to_vertex(from_base('85283473fffffff', 16), 3)",
          ImmutableList.of(ImmutableList.of(0x22528340bfffffffL, 0x255283463fffffffL)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertex(null, 4)",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertex(from_base('85283473fffffff', 16), null)",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertex(from_base('85283473fffffff', 16), -1)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCellToVertexes() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertexes(from_base('85283473fffffff', 16))",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(
                      0x22528340bfffffffL,
                      0x235283447fffffffL,
                      0x205283463fffffffL,
                      0x255283463fffffffL,
                      0x22528340ffffffffL,
                      0x23528340bfffffffL))));
      // Pentagon:
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertexes(from_base('811c3ffffffffff', 16))",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(
                      0x2011c3ffffffffffL,
                      0x2111c3ffffffffffL,
                      0x2211c3ffffffffffL,
                      0x2311c3ffffffffffL,
                      0x2411c3ffffffffffL))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertexes(0) hex",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(
                      0x2000000000000000L,
                      0x2100000000000000L,
                      0x2200000000000000L,
                      0x2300000000000000L,
                      0x2400000000000000L,
                      0x2500000000000000L))));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_vertexes(null) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testVertexToLatLng() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_vertex_to_latlng(from_base('255283463fffffff', 16))",
          ImmutableList.of(
              ImmutableList.of(ImmutableList.of(37.42012867767778, -122.03773496427027))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_vertex_to_latlng(0)",
          ImmutableList.of(
              ImmutableList.of(ImmutableList.of(68.92995788193981, 31.831280499087402))));
      assertQueryResults(
          queryRunner,
          "SELECT h3_vertex_to_latlng(null)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testIsValidVertex() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_is_valid_vertex(from_base('85283473fffffff', 16)), h3_is_valid_vertex(from_base('255283463fffffff', 16))",
          ImmutableList.of(ImmutableList.of(false, true)));

      assertQueryResults(
          queryRunner, "SELECT h3_is_valid_vertex(0)", ImmutableList.of(ImmutableList.of(false)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_is_valid_vertex(null)",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }
}
