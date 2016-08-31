package org.pentaho.big.data.impl.spark;

import org.apache.spark.launcher.SparkAppHandle;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.bigdata.api.spark.SparkJob;
import org.pentaho.hadoop.shim.HadoopConfiguration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by hudak on 9/22/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class JobMonitorTest {
  private static final String APP_ID = "mock-app-id";

  @Mock SparkAppHandle handle;
  private SparkAppHandle.Listener listener;
  private SparkJob sparkJob;

  @Before
  public void setUp() throws Exception {
    sparkJob = null;
    when( handle.getState() ).thenReturn( SparkAppHandle.State.UNKNOWN );
    JobMonitor.start( this::start ).thenAccept( this::setSparkJob ).join();
  }

  private SparkAppHandle start( SparkAppHandle.Listener listener ) {
    when( handle.getAppId() ).thenReturn( APP_ID );
    when( handle.getState() ).thenReturn( SparkAppHandle.State.SUBMITTED );
    listener.infoChanged( handle );

    this.listener = listener;
    return handle;
  }

  private void setSparkJob( SparkJob sparkJob ) {
    this.sparkJob = sparkJob;
  }

  @Test
  public void complete() throws Exception {
    assertThat( sparkJob.getJobId(), equalTo( APP_ID ) );
    assertThat( sparkJob.getExitCode().isDone(), is( false ) );

    when( handle.getState() ).thenReturn( SparkAppHandle.State.FINISHED );
    listener.stateChanged( handle );

    assertThat( sparkJob.getExitCode().getNow( -1 ), is( 0 ) );

    sparkJob.cancel();
    verify( handle, never() ).stop();
    verify( handle, never() ).kill();
  }

  @Test
  public void cancel() throws Exception {
    sparkJob.cancel();
    verify( handle ).stop();
    verify( handle, never() ).kill();
  }

  @Test
  public void kill() throws Exception {
    doThrow( new RuntimeException( "problem stopping" ) ).when( handle ).stop();

    sparkJob.cancel();
    verify( handle ).kill();
  }

  @Test
  public void failure() throws Exception {
    when( handle.getState() ).thenReturn( SparkAppHandle.State.FAILED );
    listener.stateChanged( handle );

    assertThat( sparkJob.getExitCode().getNow( -1 ), greaterThan( 0 ) );
  }
}