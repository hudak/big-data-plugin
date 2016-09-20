package org.pentaho.big.data.impl.spark;

import org.pentaho.bigdata.api.spark.SparkJob;
import org.pentaho.di.core.exception.KettleException;

import java.util.List;
import java.util.Map;

/**
 * Created by hudak on 9/19/16.
 */
public interface SubmitService {
  SparkJob submit( List<String> args, Map<String, String> env ) throws KettleException;
}
