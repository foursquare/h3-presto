package com.foursquare.presto.h3;

import static com.facebook.presto.geospatial.serde.JtsGeometrySerde.deserialize;
import static com.facebook.presto.geospatial.type.GeometryType.GEOMETRY_TYPE_NAME;
import static org.locationtech.jts.geom.Geometry.TYPENAME_POINT;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.geospatial.GeometryType;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.uber.h3core.util.LatLng;
import io.airlift.slice.Slice;
import java.util.List;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

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

  /** Function wrapping {@link com.uber.h3core.H3Core#latLngToCell(double, double, int)} */
  @ScalarFunction(value = "h3_latlng_to_cell")
  @Description("Convert degrees lat/lng to H3 index")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long latLngToCell(
      @SqlType(GEOMETRY_TYPE_NAME) Slice pointSlice, @SqlType(StandardTypes.INTEGER) long res) {
    try {
      Geometry pointGeomUntyped = deserialize(pointSlice);
      if (!TYPENAME_POINT.equals(pointGeomUntyped.getGeometryType())) {
        throw new IllegalArgumentException("Invalid polygon geometry");
      }
      Point pointGeom = (Point) pointGeomUntyped;

      return H3Plugin.h3.latLngToCell(pointGeom.getY(), pointGeom.getX(), H3Plugin.longToInt(res));
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
  @SqlType(GEOMETRY_TYPE_NAME)
  public static Slice cellToLatLng(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      LatLng latLng = H3Plugin.h3.cellToLatLng(h3);
      return H3Plugin.latLngToGeometry(latLng);
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
  @SqlType(GEOMETRY_TYPE_NAME)
  public static Slice cellToBoundary(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      List<LatLng> boundary = H3Plugin.h3.cellToBoundary(h3);
      // Duplicate the first point at the end to form a closed ring
      boundary.add(boundary.get(0));
      return H3Plugin.latLngListToGeometry(boundary, GeometryType.POLYGON);
    } catch (Exception e) {
      return null;
    }
  }
}
