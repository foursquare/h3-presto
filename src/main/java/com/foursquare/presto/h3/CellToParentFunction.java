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

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;

/** Function wrapping {@link com.uber.h3core.H3Core#cellToParent(long, int)} */
public final class CellToParentFunction {
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
}
