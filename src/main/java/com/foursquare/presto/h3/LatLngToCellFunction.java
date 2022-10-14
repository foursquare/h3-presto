package com.foursquare.presto.h3;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;

/** Function wrapping {@link com.uber.h3core.H3Core#latLngToCell(double, double, int)} */
public final class LatLngToCellFunction {
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
}
