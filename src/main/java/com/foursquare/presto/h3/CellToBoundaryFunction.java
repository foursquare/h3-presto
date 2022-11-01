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
import java.util.List;

/**
 * Wraps {@link com.uber.h3core.H3Core#cellToBoundary(long)}. Produces a row of latitude, longitude
 * degrees interleaved as (lat0, lng0, lat1, lng1, ..., latN, lngN).
 */
public final class CellToBoundaryFunction {
  @ScalarFunction(value = "h3_cell_to_boundary")
  @Description("Convert H3 index to boundary degrees lat/lng, interleaved")
  @SqlNullable
  @SqlType("ARRAY(DOUBLE)")
  public static Block cellToBoundary(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      List<LatLng> latLng = H3Plugin.h3.cellToBoundary(h3);
      // TODO: It would be nice to return this as a ARRAY(ROW(lat DOUBLE, lng DOUBLE))
      // but that is blocked on https://github.com/prestodb/presto/issues/18494
      // (determining how to build the Block to return)

      BlockBuilder blockBuilder = DOUBLE.createFixedSizeBlockBuilder(latLng.size() * 2);
      for (int i = 0; i < latLng.size(); i++) {
        LatLng ll = latLng.get(i);
        DOUBLE.writeDouble(blockBuilder, ll.lat);
        DOUBLE.writeDouble(blockBuilder, ll.lng);
      }

      return blockBuilder.build();
    } catch (Exception e) {
      return null;
    }
  }
}
