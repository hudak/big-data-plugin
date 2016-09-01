package org.pentaho.bigdata.api.mapreduce;

import java.util.Map;

/**
 * Created by ccaspanello on 8/29/2016.
 */
public interface TransformationVisitorService {

  void visit( MapReduceTransformations transformations );
}
