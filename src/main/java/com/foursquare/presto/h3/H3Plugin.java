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

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.geospatial.serde.JtsGeometrySerde.serialize;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.geospatial.GeometryType;
import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import io.airlift.slice.Slice;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

public class H3Plugin implements Plugin {
  static final String TYPE_ARRAY_BIGINT = "ARRAY(BIGINT)";
  static final String TYPE_ARRAY_INTEGER = "ARRAY(INTEGER)";

  static final H3Core h3;

  static {
    try {
      h3 = H3Core.newInstance();
    } catch (IOException e) {
      throw new RuntimeException("H3 setup failed", e);
    }
  }

  /**
   * Presto passes integer parameters in as `long`s in Java. These need to be cast down to `int` for
   * H3 functions. Throws if out of range.
   */
  static int longToInt(long l) {
    if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
      throw new RuntimeException("integer out of range");
    } else {
      return (int) l;
    }
  }

  static List<Long> longBlockToList(Block block) {
    List<Long> list = new ArrayList<>(block.getPositionCount());
    for (int i = 0; i < block.getPositionCount(); i++) {
      list.add(block.getLong(i));
    }
    return list;
  }

  static Block longListToBlock(List<Long> list) {
    BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(list.size());
    for (Long cell : list) {
      BIGINT.writeLong(blockBuilder, cell);
    }
    return blockBuilder.build();
  }

  static Slice latLngListToGeometry(List<LatLng> list, GeometryType type) {
    Coordinate[] coordinates =
        list.stream().map(ll -> new Coordinate(ll.lng, ll.lat)).toArray(Coordinate[]::new);
    GeometryFactory geomFactory = new GeometryFactory();
    if (GeometryType.LINE_STRING.equals(type)) {
      return serialize(geomFactory.createLineString(coordinates));
    } else if (GeometryType.POLYGON.equals(type)) {
      return serialize(geomFactory.createPolygon(coordinates));
    } else {
      throw new IllegalArgumentException("Cannot serialize with GeometryType " + type);
    }
  }

  static Block intListToBlock(List<Integer> list) {
    BlockBuilder blockBuilder = INTEGER.createFixedSizeBlockBuilder(list.size());
    for (Integer val : list) {
      INTEGER.writeLong(blockBuilder, val);
    }
    return blockBuilder.build();
  }

  static Slice latLngToGeometry(LatLng latLng) {
    GeometryFactory geomFactory = new GeometryFactory();
    Point point = geomFactory.createPoint(new Coordinate(latLng.lng, latLng.lat));
    return serialize(point);
  }

  @Override
  public Set<Class<?>> getFunctions() {
    return ImmutableSet.<Class<?>>builder()
        .add(
            IndexingFunctions.class,
            InspectionFunctions.class,
            HierarchyFunctions.class,
            TraversalFunctions.class,
            RegionFunctions.class,
            DirectedEdgeFunctions.class,
            VertexFunctions.class,
            MiscellaneousFunctions.class)
        .build();
  }
}
