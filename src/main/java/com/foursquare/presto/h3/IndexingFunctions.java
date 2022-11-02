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

/** Wraps https://h3geo.org/docs/api/indexing */
public final class IndexingFunctions {
  /** Function wrapping {@link com.uber.h3core.H3Core#latLngToCell(double, double, int)} */
  @ScalarFunction(value = "h3_latlng_to_cell")
  @Description("Convert degrees lat/lng to H3 index")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long latLngToCell(
      @SqlType(StandardTypes.DOUBLE) double lat,
      @SqlType(StandardTypes.DOUBLE) double lng,
      @SqlType(StandardTypes.INTEGER) long res) {
    try {
      return H3Plugin.h3.latLngToCell(lat, lng, H3Plugin.longToInt(res));
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Wraps {@link com.uber.h3core.H3Core#cellToLatLng(long)}. Produces a row of latitude, longitude
   * degrees.
   */
  @ScalarFunction(value = "h3_cell_to_latlng")
  @Description("Convert H3 index to degrees lat/lng")
  @SqlNullable
  @SqlType("ARRAY(DOUBLE)")
  public static Block cellToLatLng(@SqlType(StandardTypes.BIGINT) long h3) {
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

  /**
   * Wraps {@link com.uber.h3core.H3Core#cellToBoundary(long)}. Produces a row of latitude,
   * longitude degrees interleaved as (lat0, lng0, lat1, lng1, ..., latN, lngN).
   */
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
