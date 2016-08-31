package org.pentaho.big.data.impl.spark;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.pentaho.bigdata.api.spark.SparkJob;
import org.pentaho.bigdata.api.spark.SparkJobBuilder;

/**
 * Created by hudak on 9/15/16.
 */
public class SparkJobBuilderImpl implements SparkJobBuilder {
  private final SparkLauncher sparkLauncher;
  private final MonitorFunction monitorFunction;

  SparkJobBuilderImpl( SparkLauncher sparkLauncher, MonitorFunction monitorFunction ) {
    this.sparkLauncher = sparkLauncher;
    this.monitorFunction = monitorFunction;
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
    sparkLauncher.addAppArgs( args.stream().toArray( String[]::new ) );
    return this;
  }

  @Override public CompletableFuture<SparkJob> submit() {
    return monitorFunction.monitor( this::startApplication );
  }

  private SparkAppHandle startApplication( SparkAppHandle.Listener listener ) throws CompletionException {
    try {
      return sparkLauncher.startApplication( listener );
    } catch ( IOException e ) {
      throw new CompletionException( e );
    }
  }

  @FunctionalInterface
  interface MonitorFunction {
    CompletableFuture<SparkJob> monitor( Function<SparkAppHandle.Listener, SparkAppHandle> start );
  }
}
