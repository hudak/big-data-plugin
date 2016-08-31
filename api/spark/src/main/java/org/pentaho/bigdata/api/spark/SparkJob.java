package org.pentaho.bigdata.api.spark;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Created by hudak on 9/9/16.
 */
public interface SparkJob {
  ListenableFuture<String> getJobId();

  ListenableFuture<Integer> getExitCode();

  void cancel();
}
