package org.pentaho.di.job.entries.spark;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.job.Job;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by hudak on 9/14/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class JobStoppedListenerTest {

  @Mock Job job;
  private JobStoppedListener listener;

  @Before
  public void setUp() throws Exception {
    listener = new JobStoppedListener( Executors.newSingleThreadExecutor() );
    listener.setSleep( 100 );
    when( job.isStopped() ).thenReturn( false );
    when( job.isActive() ).thenReturn( true );
  }

  @Test
  public void jobFinishes() throws Exception {
    ListenableFuture<Boolean> future = start();

    when( job.isActive() ).thenReturn( false );

    verify( future, false );
  }

  @Test
  public void jobIsStopped() throws Exception {
    ListenableFuture<Boolean> future = start();

    when( job.isStopped() ).thenReturn( true );

    verify( future, true );
  }

  @Test
  public void canceled() throws Exception {
    ListenableFuture<Boolean> future = start();
    future.cancel( false );

    assert future.isDone();
    assert future.isCancelled();
  }

  @Test
  public void interrupt() throws Exception {
    ListenableFuture<Boolean> future = start();
    future.cancel( true );

    assert future.isDone();
    assert future.isCancelled();
  }

  private ListenableFuture<Boolean> start() throws Exception {
    // Wait a few cycles before returning
    CountDownLatch countDown = new CountDownLatch( 4 );
    when( job.isActive() )
      .then( ( invocation ) -> {
        countDown.countDown();
        return true;
      } );
    ListenableFuture<Boolean> future = listener.apply( job );

    countDown.await( 5, TimeUnit.SECONDS );
    assertThat( future, hasProperty( "done", is( false ) ) );
    return future;
  }

  private void verify( ListenableFuture<Boolean> future, boolean result ) throws Exception {
    assertThat( future.get( 5, TimeUnit.SECONDS ), is( result ) );
  }
}