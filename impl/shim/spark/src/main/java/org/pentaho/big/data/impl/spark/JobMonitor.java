package org.pentaho.big.data.impl.spark;

import com.google.common.base.Strings;
import org.apache.spark.launcher.SparkAppHandle;
import org.pentaho.bigdata.api.spark.SparkJob;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Monitors state of a submitted Spark job.
 * Created by hudak on 11/29/16.
 */
class JobMonitor implements SparkAppHandle.Listener {
  private final CompletableFuture<String> appId = new CompletableFuture<>();
  private final CompletableFuture<Integer> exitCode = new CompletableFuture<>();
  private final SparkAppHandle appHandle;

  static CompletableFuture<SparkJob> start( Function<SparkAppHandle.Listener, SparkAppHandle> startApplication ) {
    return CompletableFuture.supplyAsync( () -> new JobMonitor( startApplication ) )
      .thenCompose( JobMonitor::getSparkJob );
  }

  private JobMonitor( Function<SparkAppHandle.Listener, SparkAppHandle> startFn ) {
    appHandle = startFn.apply( this );
  }

  @Override public void stateChanged( SparkAppHandle handle ) {
    if ( !appId.isDone() ) {
      infoChanged( handle );
    }

    SparkAppHandle.State state = handle.getState();
    if ( state.isFinal() ) {
      exitCode.complete( SparkAppHandle.State.FINISHED.equals( state ) ? 0 : state.ordinal() );
    }
  }

  @Override public void infoChanged( SparkAppHandle handle ) {
    Optional.ofNullable( handle.getAppId() )
      .map( Strings::emptyToNull )
      .filter( Objects::nonNull )
      .ifPresent( appId::complete );
  }

  CompletableFuture<SparkJob> getSparkJob() {
    return appId.thenApply( jobId -> new SparkJob() {
      @Override public String getJobId() {
        return jobId;
      }

      @Override public CompletableFuture<Integer> getExitCode() {
        return exitCode;
      }

      @Override public void cancel() {
        JobMonitor.this.cancel();
      }
    } );
  }

  private void cancel() {
    if ( !appHandle.getState().isFinal() ) {
      try {
        appHandle.stop();
      } catch ( Exception e ) {
        appHandle.kill();
      }
    }
  }

}
