package org.pentaho.bigdata.api.spark;

import java.util.concurrent.CompletableFuture;

/**
 * Created by hudak on 9/9/16.
 */
public interface SparkJob {
  String getJobId();

  CompletableFuture<Integer> getExitCode();

  void cancel();
}
