package org.pentaho.big.data.impl.spark;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.pentaho.bigdata.api.spark.SparkJob;
import org.pentaho.bigdata.api.spark.SparkJobBuilder;
import org.pentaho.di.core.exception.KettleException;

/**
 * Created by hudak on 9/15/16.
 */
public class SparkJobBuilderImpl implements SparkJobBuilder {
  private final SparkLauncher sparkLauncher;
  private final Function<SparkAppHandle, SparkJob> jobFactory;

  public SparkJobBuilderImpl( SparkLauncher sparkLauncher, Function<SparkAppHandle, SparkJob> jobFactory ) {
    this.sparkLauncher = sparkLauncher;
    this.jobFactory = jobFactory;
  }

  @Override public SparkJobBuilder setSparkConf( String key, String value ) {
    sparkLauncher.setConf( key, value );
    return this;
  }

  @Override public SparkJobBuilder setSparkArg( String name, String value ) {
    sparkLauncher.addSparkArg( name, value );
    return this;
  }

  @Override public SparkJobBuilder setSparkArg( String name ) {
    sparkLauncher.addSparkArg( name );
    return this;
  }

  @Override public SparkJobBuilder setAppResource( String value ) {
    sparkLauncher.setAppResource( value );
    return this;
  }

  @Override public SparkJobBuilder addAppArgs( List<String> args ) {
    sparkLauncher.addAppArgs( args.toArray( new String[ args.size() ] ) );
    return this;
  }

  @Override public SparkJob submit() throws KettleException {
    try {
      return jobFactory.apply( sparkLauncher.startApplication() );
    } catch ( IOException e ) {
      throw new KettleException( e );
    }
  }
}
