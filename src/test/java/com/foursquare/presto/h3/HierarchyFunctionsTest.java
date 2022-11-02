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
          ImmutableList.of(Collections.singletonList(18014398509481984L)));
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
                      608693240631132159L,
                      608693240647909375L,
                      608693240664686591L,
                      608693240681463807L,
                      608693240698241023L,
                      608693240715018239L,
                      608693240731795455L,
                      608693240765349887L,
                      608693240782127103L,
                      608693240798904319L,
                      608693240815681535L,
                      608693240832458751L,
                      608693240849235967L,
                      608693240866013183L,
                      608693240899567615L,
                      608693240916344831L,
                      608693240933122047L,
                      608693240949899263L,
                      608693240966676479L,
                      608693240983453695L,
                      608693241000230911L,
                      608693241033785343L,
                      608693241050562559L,
                      608693241067339775L,
                      608693241084116991L,
                      608693241100894207L,
                      608693241117671423L,
                      608693241134448639L,
                      608693241168003071L,
                      608693241184780287L,
                      608693241201557503L,
                      608693241218334719L,
                      608693241235111935L,
                      608693241251889151L,
                      608693241268666367L,
                      608693241302220799L,
                      608693241318998015L,
                      608693241335775231L,
                      608693241352552447L,
                      608693241369329663L,
                      608693241386106879L,
                      608693241402884095L,
                      608693241436438527L,
                      608693241453215743L,
                      608693241469992959L,
                      608693241486770175L,
                      608693241503547391L,
                      608693241520324607L,
                      608693241537101823L,
                      608693241704873983L,
                      608693241721651199L,
                      608693241738428415L,
                      608693241755205631L,
                      608693241771982847L,
                      608693241788760063L,
                      608693241805537279L,
                      608693241839091711L,
                      608693241855868927L,
                      608693241872646143L,
                      608693241889423359L,
                      608693241906200575L,
                      608693241922977791L,
                      608693241939755007L,
                      608693241973309439L,
                      608693241990086655L,
                      608693242006863871L,
                      608693242023641087L,
                      608693242040418303L,
                      608693242057195519L,
                      608693242073972735L,
                      608693242107527167L,
                      608693242124304383L,
                      608693242141081599L,
                      608693242157858815L,
                      608693242174636031L,
                      608693242191413247L,
                      608693242208190463L,
                      608693242241744895L,
                      608693242258522111L,
                      608693242275299327L,
                      608693242292076543L,
                      608693242308853759L,
                      608693242325630975L,
                      608693242342408191L,
                      608693242375962623L,
                      608693242392739839L,
                      608693242409517055L,
                      608693242426294271L,
                      608693242443071487L,
                      608693242459848703L,
                      608693242476625919L,
                      608693242510180351L,
                      608693242526957567L,
                      608693242543734783L,
                      608693242560511999L,
                      608693242577289215L,
                      608693242594066431L,
                      608693242610843647L))));

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
