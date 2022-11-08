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

import static com.facebook.presto.geospatial.type.GeometryType.GEOMETRY_TYPE_NAME;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.geospatial.GeometryType;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

/** Functions wrapping https://h3geo.org/docs/api/uniedge */
public final class DirectedEdgeFunctions {
  @ScalarFunction(value = "h3_are_neighbor_cells")
  @Description("Returns true if the H3 cells are adjacent")
  @SqlNullable
  @SqlType(StandardTypes.BOOLEAN)
  public static Boolean areNeighborCells(
      @SqlType(StandardTypes.BIGINT) long a, @SqlType(StandardTypes.BIGINT) long b) {
    try {
      return H3Plugin.h3.areNeighborCells(a, b);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_cells_to_directed_edge")
  @Description("Find directed edge index from origin to destination")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long cellsToDirectedEdge(
      @SqlType(StandardTypes.BIGINT) long origin, @SqlType(StandardTypes.BIGINT) long destination) {
    try {
      return H3Plugin.h3.cellsToDirectedEdge(origin, destination);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_get_directed_edge_origin")
  @Description("Find the origin cell index of a directed edge")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long getDirectedEdgeOrigin(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      return H3Plugin.h3.getDirectedEdgeOrigin(h3);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_get_directed_edge_destination")
  @Description("Find the destination cell index of a directed edge")
  @SqlNullable
  @SqlType(StandardTypes.BIGINT)
  public static Long getDirectedEdgeDestination(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      return H3Plugin.h3.getDirectedEdgeDestination(h3);
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_directed_edge_to_cells")
  @Description("Find the origin and destination cell indexes of a directed edge")
  @SqlNullable
  @SqlType(H3Plugin.TYPE_ARRAY_BIGINT)
  public static Block directedEdgeToCells(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      return H3Plugin.longListToBlock(H3Plugin.h3.directedEdgeToCells(h3));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_origin_to_directed_edges")
  @Description("Find the directed edges from an origin")
  @SqlNullable
  @SqlType(H3Plugin.TYPE_ARRAY_BIGINT)
  public static Block originToDirectedEdges(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      return H3Plugin.longListToBlock(H3Plugin.h3.originToDirectedEdges(h3));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_directed_edge_to_boundary")
  @Description("Find the lat/lng boundary of a directed edge")
  @SqlNullable
  @SqlType(GEOMETRY_TYPE_NAME)
  public static Slice directedEdgeToBoundary(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      return H3Plugin.latLngListToGeometry(
          H3Plugin.h3.directedEdgeToBoundary(h3), GeometryType.LINE_STRING);
    } catch (Exception e) {
      return null;
    }
  }
}
