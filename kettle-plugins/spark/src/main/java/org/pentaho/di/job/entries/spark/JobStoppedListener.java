package org.pentaho.di.job.entries.spark;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.pentaho.di.job.Job;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * Created by hudak on 9/13/16.
 */
public class JobStoppedListener implements Function<Job, ListenableFuture<Boolean>> {
  private final ListeningExecutorService executorService;
  private int sleep = 3000;

  public JobStoppedListener( ListeningExecutorService executorService ) {
    this.executorService = executorService;
  }

  public JobStoppedListener( ExecutorService executorService ) {
    this( MoreExecutors.listeningDecorator( executorService ) );
  }

  public int getSleep() {
    return sleep;
  }

  public void setSleep( int sleep ) {
    this.sleep = sleep;
  }

  @Override public ListenableFuture<Boolean> apply( Job job ) {
    return executorService.submit( () -> {
      while ( !job.isStopped() && job.isActive() ) {
        Thread.sleep( sleep );
      }
      return job.isStopped();
    } );
  }
}
