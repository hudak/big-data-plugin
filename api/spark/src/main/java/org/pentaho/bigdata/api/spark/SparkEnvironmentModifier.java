package org.pentaho.bigdata.api.spark;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Created by hudak on 11/30/16.
 */
@FunctionalInterface
public interface SparkEnvironmentModifier
  extends Function<Map<String, String>, CompletableFuture<Map<String, String>>> {

  @Override CompletableFuture<Map<String, String>> apply( Map<String, String> env );
}
