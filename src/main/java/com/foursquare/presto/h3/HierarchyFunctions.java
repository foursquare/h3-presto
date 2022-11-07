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
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import java.util.List;

/** Function wrapping {@link com.uber.h3core.H3Core#cellToParent(long, int)} */
public final class HierarchyFunctions {
  @ScalarFunction(value = "h3_cell_to_parent")
  @Description("Truncate H3 index to parent")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long cellToParent(
      @SqlType(StandardTypes.BIGINT) long cell, @SqlType(StandardTypes.INTEGER) Long resNullable) {
    try {
      int res;
      if (resNullable != null) {
        res = H3Plugin.longToInt(resNullable);
      } else {
        res = H3Plugin.h3.getResolution(cell) - 1;
      }
      return H3Plugin.h3.cellToParent(cell, res);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_cell_to_children")
  @Description("Find children of an H3 index at given resolution")
  @SqlNullable
  @SqlType("ARRAY(BIGINT)")
  public static Block cellToChildren(
      @SqlType(StandardTypes.BIGINT) long cell, @SqlType(StandardTypes.INTEGER) Long resNullable) {
    try {
      int res;
      if (resNullable != null) {
        res = H3Plugin.longToInt(resNullable);
      } else {
        res = H3Plugin.h3.getResolution(cell) + 1;
      }
      List<Long> children = H3Plugin.h3.cellToChildren(cell, res);
      return H3Plugin.longListToBlock(children);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_cell_to_center_child")
  @Description("Find the center child of an H3 index at a given resolution")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long cellToCenterChild(
      @SqlType(StandardTypes.BIGINT) long cell, @SqlType(StandardTypes.INTEGER) Long resNullable) {
    try {
      int res;
      if (resNullable != null) {
        res = H3Plugin.longToInt(resNullable);
      } else {
        res = H3Plugin.h3.getResolution(cell) + 1;
      }
      return H3Plugin.h3.cellToCenterChild(cell, res);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_compact_cells")
  @Description("Compact indexes to coarser resolutions")
  @SqlType("ARRAY(BIGINT)")
  public static Block compactCells(@SqlType("ARRAY(BIGINT)") Block cellsBlock) {
    try {
      List<Long> cells = H3Plugin.longBlockToList(cellsBlock);
      List<Long> compacted = H3Plugin.h3.compactCells(cells);
      return H3Plugin.longListToBlock(compacted);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_uncompact_cells")
  @Description("Uncompact indexes to finer resolutions")
  @SqlType("ARRAY(BIGINT)")
  public static Block uncompactCells(
      @SqlType("ARRAY(BIGINT)") Block cellsBlock, @SqlType(StandardTypes.INTEGER) long res) {
    try {
      List<Long> cells = H3Plugin.longBlockToList(cellsBlock);
      List<Long> uncompacted = H3Plugin.h3.uncompactCells(cells, H3Plugin.longToInt(res));
      return H3Plugin.longListToBlock(uncompacted);
    } catch (Exception e) {
      return null;
    }
  }
}
