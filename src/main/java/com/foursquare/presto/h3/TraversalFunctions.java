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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.collect.ImmutableList;
import com.uber.h3core.util.CoordIJ;
import java.util.List;
import java.util.stream.Collectors;

/** Wraps https://h3geo.org/docs/api/traversal */
public final class TraversalFunctions {
  @ScalarFunction(value = "h3_grid_disk")
  @Description("Finds all nearby cells in a disk around the origin")
  @SqlType("ARRAY(BIGINT)")
  public static Block gridDisk(
      @SqlType(StandardTypes.BIGINT) long origin, @SqlType(StandardTypes.INTEGER) long k) {
    try {
      List<Long> disk = H3Plugin.h3.gridDisk(origin, H3Plugin.longToInt(k));
      return H3Plugin.longListToBlock(disk);
    } catch (Exception e) {
      return null;
    }
  }

  // TODO: gridDiskDistances

  @ScalarFunction(value = "h3_grid_disk_unsafe")
  @Description(
      "Efficiently finds all nearby cells in a disk around the origin, but will return null if a pentagon is encountered")
  @SqlType("ARRAY(BIGINT)")
  public static Block gridDiskUnsafe(
      @SqlType(StandardTypes.BIGINT) long origin, @SqlType(StandardTypes.INTEGER) long k) {
    try {
      List<Long> disk =
          H3Plugin.h3.gridDiskUnsafe(origin, H3Plugin.longToInt(k)).stream()
              .flatMap(List::stream)
              .collect(Collectors.toList());
      return H3Plugin.longListToBlock(disk);
    } catch (Exception e) {
      return null;
    }
  }

  // TODO: gridDiskDistancesUnsafe
  // TODO: gridDiskDistancesSafe

  @ScalarFunction(value = "h3_grid_ring_unsafe")
  @Description(
      "Efficiently finds nearby cells in a ring of distance k around the origin, but will return null if a pentagon is encountered")
  @SqlType("ARRAY(BIGINT)")
  public static Block gridRingUnsafe(
      @SqlType(StandardTypes.BIGINT) long origin, @SqlType(StandardTypes.INTEGER) long k) {
    try {
      List<Long> disk = H3Plugin.h3.gridRingUnsafe(origin, H3Plugin.longToInt(k));
      return H3Plugin.longListToBlock(disk);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_grid_path_cells")
  @Description("Finds cells comprising a path between origin and destination, in order")
  @SqlType("ARRAY(BIGINT)")
  public static Block gridPathCells(
      @SqlType(StandardTypes.BIGINT) long origin, @SqlType(StandardTypes.BIGINT) long destination) {
    try {
      List<Long> path = H3Plugin.h3.gridPathCells(origin, destination);
      return H3Plugin.longListToBlock(path);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_grid_distance")
  @Description("Finds distance in grid cells between origin and destination")
  @SqlType(StandardTypes.BIGINT)
  public static Long gridDistance(
      @SqlType(StandardTypes.BIGINT) long origin, @SqlType(StandardTypes.BIGINT) long destination) {
    try {
      return H3Plugin.h3.gridDistance(origin, destination);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_cell_to_local_ij")
  @Description("Finds local IJ coordinates for a cell")
  @SqlType("ARRAY(INTEGER)")
  public static Block cellToLocalIj(
      @SqlType(StandardTypes.BIGINT) long origin, @SqlType(StandardTypes.BIGINT) long cell) {
    try {
      // TODO: Return as ROW(i INTEGER, j INTEGER)
      CoordIJ ij = H3Plugin.h3.cellToLocalIj(origin, cell);
      return H3Plugin.intListToBlock(ImmutableList.of(ij.i, ij.j));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_local_ij_to_cell")
  @Description("Finds cell given local IJ coordinates")
  @SqlType(StandardTypes.BIGINT)
  public static Long localIjToCell(
      @SqlType(StandardTypes.BIGINT) long origin,
      @SqlType(StandardTypes.INTEGER) long i,
      @SqlType(StandardTypes.INTEGER) long j) {
    try {
      // TODO: Accept as ROW(i INTEGER, j INTEGER)
      CoordIJ ij = new CoordIJ(H3Plugin.longToInt(i), H3Plugin.longToInt(j));
      return H3Plugin.h3.localIjToCell(origin, ij);
    } catch (Exception e) {
      return null;
    }
  }
}
