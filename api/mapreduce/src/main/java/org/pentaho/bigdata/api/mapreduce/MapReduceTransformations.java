package org.pentaho.bigdata.api.mapreduce;

import com.google.common.collect.ImmutableList;
import org.pentaho.di.trans.TransMeta;

import java.util.EnumMap;
import java.util.List;

/**
 * Created by ccaspanello on 8/29/2016.
 */
public class MapReduceTransformations extends EnumMap<MapReduceTransformation.Type, MapReduceTransformation> {

  public MapReduceTransformations() {
    super( MapReduceTransformation.Type.class );
  }

  public List<TransMeta> getTransMetas() {
    final ImmutableList.Builder<TransMeta> builder = ImmutableList.builder();

    values().stream()
      .map( MapReduceTransformation::getTransMeta )
      .forEach( builder::add );

    return builder.build();
  }

}
