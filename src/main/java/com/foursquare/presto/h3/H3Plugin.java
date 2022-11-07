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
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class H3Plugin implements Plugin {
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

  static Block latLngListToBlock(List<LatLng> list) {
    BlockBuilder blockBuilder = DOUBLE.createFixedSizeBlockBuilder(list.size() * 2);
    for (LatLng latLng : list) {
      DOUBLE.writeDouble(blockBuilder, latLng.lat);
      DOUBLE.writeDouble(blockBuilder, latLng.lng);
    }
    return blockBuilder.build();
  }

  static Block intListToBlock(List<Integer> list) {
    BlockBuilder blockBuilder = INTEGER.createFixedSizeBlockBuilder(list.size());
    for (Integer val : list) {
      INTEGER.writeLong(blockBuilder, val);
    }
    return blockBuilder.build();
  }

  static Block latLngToBlock(LatLng latLng) {
    // TODO: It would be nice to return this as a ROW(lat DOUBLE, lng DOUBLE)
    // but that is blocked on https://github.com/prestodb/presto/issues/18494
    // (determining how to build the Block to return)
    // TODO: Or to return this directly as Geometry
    BlockBuilder blockBuilder = DOUBLE.createFixedSizeBlockBuilder(2);
    DOUBLE.writeDouble(blockBuilder, latLng.lat);
    DOUBLE.writeDouble(blockBuilder, latLng.lng);
    return blockBuilder.build();
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
            VertexFunctions.class,
            MiscellaneousFunctions.class)
        .build();
  }
}
