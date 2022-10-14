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
