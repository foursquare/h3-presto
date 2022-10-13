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
      // TODO: Check if there are any plugin setup hooks we can use instead
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
        .add(LatLngToCellFunction.class, CellToParentFunction.class)
        .build();
  }
}
