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
public class HierarchyFunctionsTest {
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

  @Test
  public void testCellToChildren() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_children(from_base('85283473fffffff', 16), 6) hex",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(
                      0x862834707ffffffL,
                      0x86283470fffffffL,
                      0x862834717ffffffL,
                      0x86283471fffffffL,
                      0x862834727ffffffL,
                      0x86283472fffffffL,
                      0x862834737ffffffL))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_children(0, 4) hex",
          ImmutableList.of(Collections.singletonList(Collections.emptyList())));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_children(null, 4) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_children(from_base('85283473fffffff', 16), null) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_children(from_base('85283473fffffff', 16), -1) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCellToCenterChild() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_center_child(from_base('85283473fffffff', 16), 6) hex",
          ImmutableList.of(ImmutableList.of(0x862834707ffffffL)));

      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_center_child(0, 4) hex",
          ImmutableList.of(Collections.singletonList(0x40000000000000L)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_center_child(null, 4) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_center_child(from_base('85283473fffffff', 16), null) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_cell_to_center_child(from_base('85283473fffffff', 16), -1) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testCompactCells() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_compact_cells(h3_cell_to_children(from_base('85283473fffffff', 16), 7)) hex",
          ImmutableList.of(ImmutableList.of(ImmutableList.of(0x85283473fffffffL))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_compact_cells(ARRAY []) hex",
          ImmutableList.of(ImmutableList.of(ImmutableList.of())));
      assertQueryResults(
          queryRunner,
          "SELECT h3_compact_cells(null) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }

  @Test
  public void testUncompactCells() {
    try (QueryRunner queryRunner = createQueryRunner()) {
      assertQueryResults(
          queryRunner,
          "SELECT h3_uncompact_cells(ARRAY [from_base('85283473fffffff', 16), from_base('85283477fffffff', 16)], 7) hex",
          ImmutableList.of(
              ImmutableList.of(
                  ImmutableList.of(
                      0x872834700ffffffL,
                      0x872834701ffffffL,
                      0x872834702ffffffL,
                      0x872834703ffffffL,
                      0x872834704ffffffL,
                      0x872834705ffffffL,
                      0x872834706ffffffL,
                      0x872834708ffffffL,
                      0x872834709ffffffL,
                      0x87283470affffffL,
                      0x87283470bffffffL,
                      0x87283470cffffffL,
                      0x87283470dffffffL,
                      0x87283470effffffL,
                      0x872834710ffffffL,
                      0x872834711ffffffL,
                      0x872834712ffffffL,
                      0x872834713ffffffL,
                      0x872834714ffffffL,
                      0x872834715ffffffL,
                      0x872834716ffffffL,
                      0x872834718ffffffL,
                      0x872834719ffffffL,
                      0x87283471affffffL,
                      0x87283471bffffffL,
                      0x87283471cffffffL,
                      0x87283471dffffffL,
                      0x87283471effffffL,
                      0x872834720ffffffL,
                      0x872834721ffffffL,
                      0x872834722ffffffL,
                      0x872834723ffffffL,
                      0x872834724ffffffL,
                      0x872834725ffffffL,
                      0x872834726ffffffL,
                      0x872834728ffffffL,
                      0x872834729ffffffL,
                      0x87283472affffffL,
                      0x87283472bffffffL,
                      0x87283472cffffffL,
                      0x87283472dffffffL,
                      0x87283472effffffL,
                      0x872834730ffffffL,
                      0x872834731ffffffL,
                      0x872834732ffffffL,
                      0x872834733ffffffL,
                      0x872834734ffffffL,
                      0x872834735ffffffL,
                      0x872834736ffffffL,
                      0x872834740ffffffL,
                      0x872834741ffffffL,
                      0x872834742ffffffL,
                      0x872834743ffffffL,
                      0x872834744ffffffL,
                      0x872834745ffffffL,
                      0x872834746ffffffL,
                      0x872834748ffffffL,
                      0x872834749ffffffL,
                      0x87283474affffffL,
                      0x87283474bffffffL,
                      0x87283474cffffffL,
                      0x87283474dffffffL,
                      0x87283474effffffL,
                      0x872834750ffffffL,
                      0x872834751ffffffL,
                      0x872834752ffffffL,
                      0x872834753ffffffL,
                      0x872834754ffffffL,
                      0x872834755ffffffL,
                      0x872834756ffffffL,
                      0x872834758ffffffL,
                      0x872834759ffffffL,
                      0x87283475affffffL,
                      0x87283475bffffffL,
                      0x87283475cffffffL,
                      0x87283475dffffffL,
                      0x87283475effffffL,
                      0x872834760ffffffL,
                      0x872834761ffffffL,
                      0x872834762ffffffL,
                      0x872834763ffffffL,
                      0x872834764ffffffL,
                      0x872834765ffffffL,
                      0x872834766ffffffL,
                      0x872834768ffffffL,
                      0x872834769ffffffL,
                      0x87283476affffffL,
                      0x87283476bffffffL,
                      0x87283476cffffffL,
                      0x87283476dffffffL,
                      0x87283476effffffL,
                      0x872834770ffffffL,
                      0x872834771ffffffL,
                      0x872834772ffffffL,
                      0x872834773ffffffL,
                      0x872834774ffffffL,
                      0x872834775ffffffL,
                      0x872834776ffffffL))));

      assertQueryResults(
          queryRunner,
          "SELECT h3_uncompact_cells(ARRAY [], 5) hex",
          ImmutableList.of(ImmutableList.of(ImmutableList.of())));
      assertQueryResults(
          queryRunner,
          "SELECT h3_uncompact_cells(null, 5) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_uncompact_cells(ARRAY [from_base('85283473fffffff', 16)], null) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_uncompact_cells(ARRAY [from_base('85283473fffffff', 16)], -1) hex",
          ImmutableList.of(Collections.singletonList(null)));
      assertQueryResults(
          queryRunner,
          "SELECT h3_uncompact_cells(ARRAY [from_base('85283473fffffff', 16)], 16) hex",
          ImmutableList.of(Collections.singletonList(null)));
    }
  }
}
