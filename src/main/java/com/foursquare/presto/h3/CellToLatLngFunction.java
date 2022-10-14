package com.foursquare.presto.h3;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.uber.h3core.util.LatLng;

/**
 * Wraps {@link com.uber.h3core.H3Core#cellToLatLng(long)}. Produces a row of latitude, longitude
 * degrees.
 */
public final class CellToLatLngFunction {
  @ScalarFunction(value = "h3_cell_to_latlng")
  @Description("Convert H3 index to degrees lat/lng")
  @SqlNullable
  @SqlType("ARRAY(DOUBLE)")
  public static Block latLngToCell(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      LatLng latLng = H3Plugin.h3.cellToLatLng(h3);
      // TODO: It would be nice to return this as a ROW(lat DOUBLE, lng DOUBLE)
      // but that is blocked on https://github.com/prestodb/presto/issues/18494
      // (determining how to build the Block to return)
      BlockBuilder blockBuilder = DOUBLE.createFixedSizeBlockBuilder(2);
      DOUBLE.writeDouble(blockBuilder, latLng.lat);
      DOUBLE.writeDouble(blockBuilder, latLng.lng);
      return blockBuilder.build();
    } catch (Exception e) {
      return null;
    }
  }
}
