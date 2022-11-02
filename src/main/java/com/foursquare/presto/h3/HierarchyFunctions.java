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
      @SqlType(StandardTypes.BIGINT) long h3, @SqlType(StandardTypes.INTEGER) long res) {
    try {
      return H3Plugin.h3.cellToParent(h3, H3Plugin.longToInt(res));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_cell_to_children")
  @Description("Find children of an H3 index at given resolution")
  @SqlNullable
  @SqlType("ARRAY(BIGINT)")
  public static Block cellToChildren(
      @SqlType(StandardTypes.BIGINT) long h3, @SqlType(StandardTypes.INTEGER) long res) {
    try {
      List<Long> children = H3Plugin.h3.cellToChildren(h3, H3Plugin.longToInt(res));
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
      @SqlType(StandardTypes.BIGINT) long h3, @SqlType(StandardTypes.INTEGER) long res) {
    try {
      return H3Plugin.h3.cellToCenterChild(h3, H3Plugin.longToInt(res));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_compact_cells")
  @Description("Compact indexes to coarser resolutions")
  @SqlNullable
  @SqlType("ARRAY(BIGINT)")
  public static Block compactCells(@SqlType("ARRAY(BIGINT)") Block h3Block) {
    try {
      List<Long> h3 = H3Plugin.longBlockToList(h3Block);
      List<Long> compacted = H3Plugin.h3.compactCells(h3);
      return H3Plugin.longListToBlock(compacted);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_uncompact_cells")
  @Description("Uncompact indexes to finer resolutions")
  @SqlNullable
  @SqlType("ARRAY(BIGINT)")
  public static Block uncompactCells(
      @SqlType("ARRAY(BIGINT)") Block h3Block, @SqlType(StandardTypes.INTEGER) long res) {
    try {
      List<Long> h3 = H3Plugin.longBlockToList(h3Block);
      List<Long> uncompacted = H3Plugin.h3.uncompactCells(h3, H3Plugin.longToInt(res));
      return H3Plugin.longListToBlock(uncompacted);
    } catch (Exception e) {
      return null;
    }
  }
}
