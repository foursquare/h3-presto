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

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;
import com.uber.h3core.H3Core;
import java.io.IOException;
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

  @Override
  public Set<Class<?>> getFunctions() {
    return ImmutableSet.<Class<?>>builder()
        .add(IndexingFunctions.class, CellToParentFunction.class, InspectionFunctions.class)
        .build();
  }
}
