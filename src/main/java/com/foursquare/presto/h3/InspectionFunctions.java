package com.foursquare.presto.h3;

import static com.facebook.presto.common.type.IntegerType.INTEGER;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import java.util.Collection;

/** Wraps https://h3geo.org/docs/api/inspection/ */
public final class InspectionFunctions {
  @ScalarFunction(value = "h3_get_resolution")
  @Description("Convert H3 index to resolution (0-15)")
  @SqlType("INTEGER")
  public static Long getResolution(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      return Long.valueOf(H3Plugin.h3.getResolution(h3));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_get_base_cell_number")
  @Description("Convert H3 index to base cell number (0-122)")
  @SqlType("INTEGER")
  public static Long getBaseCellNumber(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      return Long.valueOf(H3Plugin.h3.getBaseCellNumber(h3));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_string_to_h3")
  @Description("Convert H3 index string to integer form")
  @SqlType("BIGINT")
  public static Long stringToH3(@SqlType(StandardTypes.VARCHAR) Slice h3) {
    try {
      return H3Plugin.h3.stringToH3(h3.toStringUtf8());
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_h3_to_string")
  @Description("Convert H3 index integer to string form")
  @SqlType("VARCHAR")
  public static Slice h3ToString(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      return Slices.utf8Slice(H3Plugin.h3.h3ToString(h3));
    } catch (Exception e) {
      return null;
    }
  }

  @ScalarFunction(value = "h3_is_valid_cell")
  @Description("Returns true if given a valid H3 cell identifier")
  @SqlType("BOOLEAN")
  public static boolean isValidCell(@SqlType(StandardTypes.BIGINT) long h3) {
    return H3Plugin.h3.isValidCell(h3);
  }

  @ScalarFunction(value = "h3_is_res_class_iii")
  @Description("Returns true if the index is in resolution class III")
  @SqlType("BOOLEAN")
  public static boolean isResClassIII(@SqlType(StandardTypes.BIGINT) long h3) {
    return H3Plugin.h3.isResClassIII(h3);
  }

  @ScalarFunction(value = "h3_is_pentagon")
  @Description("Returns true if the cell index is a pentagon")
  @SqlType("BOOLEAN")
  public static boolean isPentagon(@SqlType(StandardTypes.BIGINT) long h3) {
    return H3Plugin.h3.isPentagon(h3);
  }

  @ScalarFunction(value = "h3_get_icosahedron_faces")
  @Description("Convert H3 index to icosahedron face IDs")
  @SqlType("ARRAY(INTEGER)")
  public static Block getIcosahedronFaces(@SqlType(StandardTypes.BIGINT) long h3) {
    try {
      Collection<Integer> faces = H3Plugin.h3.getIcosahedronFaces(h3);
      BlockBuilder blockBuilder = INTEGER.createFixedSizeBlockBuilder(faces.size());
      for (Integer face : faces) {
        INTEGER.writeLong(blockBuilder, face);
      }
      return blockBuilder.build();
    } catch (Exception e) {
      return null;
    }
  }
}
