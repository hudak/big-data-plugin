package org.pentaho.big.data.impl.shim.mapreduce;

import org.pentaho.bigdata.api.mapreduce.MapReduceTransformations;
import org.pentaho.bigdata.api.mapreduce.TransformationVisitorService;

/**
 * Created by ccaspanello on 8/29/2016.
 */
public class MockVisitorService implements TransformationVisitorService {
    @Override
    public void visit(MapReduceTransformations transformations) {
        // Do Nothing
    }
}
