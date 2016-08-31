package org.pentaho.big.data.impl.spark;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.spark.launcher.SparkAppHandle;
import org.pentaho.bigdata.api.spark.SparkJob;

import java.util.Optional;

/**
 * Created by hudak on 9/21/16.
 */
public class SparkJobImpl implements SparkJob {
  private final SparkAppHandle sparkAppHandle;
  private final SettableFuture<Integer> exitCode = SettableFuture.create();
  private final SettableFuture<String> jobId = SettableFuture.create();

  public SparkJobImpl( SparkAppHandle sparkAppHandle ) {
    this.sparkAppHandle = sparkAppHandle;
  }

  public static SparkJob create( final SparkAppHandle sparkAppHandle ) {
    // Create a new instance
    SparkJobImpl job = new SparkJobImpl( sparkAppHandle );
    // Register listener
    sparkAppHandle.addListener( job.getHandler() );
    // Initialize state
    return job.update();
  }

  private SparkAppHandle.Listener getHandler() {
    return new SparkAppHandle.Listener() {
      @Override public void stateChanged( SparkAppHandle handle ) {
        update();
      }

      @Override public void infoChanged( SparkAppHandle handle ) {
        update();
      }
    };
  }

  private SparkJobImpl update() {
    SparkAppHandle.State state = sparkAppHandle.getState();
    if ( state.isFinal() ) {
      exitCode.set( SparkAppHandle.State.FINISHED.equals( state ) ? 0 : state.ordinal() );
    }
    Optional.ofNullable( sparkAppHandle.getAppId() ).ifPresent( jobId::set );
    return this;
  }

  @Override public ListenableFuture<String> getJobId() {
    return jobId;
  }

  @Override public ListenableFuture<Integer> getExitCode() {
    return exitCode;
  }

  @Override public void cancel() {
    // Throw an InvalidStateException if appHandle can not be retrieved in 10 seconds.
    // This should be set if any method is called on the handler
    if ( !exitCode.isDone() ) {
      try {
        sparkAppHandle.stop();
      } catch ( Exception e ) {
        sparkAppHandle.kill();
      }
    }
  }
}
