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
import com.uber.h3core.util.LatLng;
import java.util.List;

/**
 * Functions wrapping https://h3geo.org/docs/api/regions
 *
 * <p>TODO: This API is not very friendly
 */
public final class RegionFunctions {
  @ScalarFunction(value = "h3_polygon_to_cells")
  @Description("Convert a polygon to H3 cells")
  @SqlNullable
  @SqlType("ARRAY(BIGINT)")
  public static Block polygonToCells(
      @SqlType("ARRAY(DOUBLE)") Block pointsBlock,
      @SqlType("ARRAY(ARRAY(DOUBLE))") Block holesBlock,
      @SqlType(StandardTypes.INTEGER) long res) {
    try {
      List<LatLng> points = H3Plugin.latLngBlockToList(pointsBlock);
      List<List<LatLng>> holes = H3Plugin.latLngArrayBlockToList(holesBlock);
      List<Long> cells = H3Plugin.h3.polygonToCells(points, holes, H3Plugin.longToInt(res));
      return H3Plugin.longListToBlock(cells);
    } catch (Exception e) {
      return null;
    }
  }

  // TODO: Unclear why this does not work
  // @ScalarFunction(value = "h3_cells_to_multi_polygon")
  // @Description("Find the multipolygon of the given cells")
  // @SqlNullable
  // @SqlType("ARRAY(ARRAY(ARRAY(DOUBLE)))")
  // public static Block cellsToMultiPolygon(
  //     @SqlType("ARRAY(BIGINT)") Block h3Block, @SqlType("BOOLEAN") boolean geojson) {
  //   try {
  //     List<Long> cells = H3Plugin.longBlockToList(h3Block);
  //     List<List<List<LatLng>>> multiPolygon = H3Plugin.h3.cellsToMultiPolygon(cells, geojson);

  //     Type doubleArray = new ArrayType(DOUBLE);
  //     Type doubleArrayArray = new ArrayType(doubleArray);
  //     Type doubleArrayArrayArray = new ArrayType(doubleArrayArray);
  //     BlockBuilder blockBuilder =
  //         doubleArrayArrayArray.createBlockBuilder(null, multiPolygon.size());
  //     for (List<List<LatLng>> polygon : multiPolygon) {
  //       BlockBuilder blockBuilder2 = doubleArrayArray.createBlockBuilder(null, polygon.size());
  //       for (List<LatLng> loop : polygon) {
  //         doubleArray.writeObject(blockBuilder2, H3Plugin.latLngListToBlock(loop));
  //       }
  //       doubleArrayArray.writeObject(blockBuilder, blockBuilder2.build());
  //     }
  //     return blockBuilder.build();
  //   } catch (Exception e) {
  //     e.printStackTrace();
  //     return null;
  //   }
  // }
}
