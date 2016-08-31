/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.job.entries.spark;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.bigdata.api.spark.SparkJob;
import org.pentaho.bigdata.api.spark.SparkJobBuilder;
import org.pentaho.bigdata.api.spark.SparkService;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.job.Job;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.pentaho.di.job.entries.spark.JobEntrySparkSubmit.JOB_TYPE_JAVA_SCALA;
import static org.pentaho.di.job.entries.spark.JobEntrySparkSubmit.JOB_TYPE_PYTHON;

@RunWith( MockitoJUnitRunner.class )
public class JobEntrySparkSubmitTest {
  @Mock NamedClusterServiceLocator serviceLocator;
  @Mock JobStoppedListener jobStoppedListener;
  @Mock LogChannelInterface logChannel;
  @Mock SparkService service;
  @Mock SparkJob sparkJob;
  @Mock Job parent;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  private JobEntrySparkSubmit sparkSubmit;
  private File sparkDir;
  private File sparkClient;
  private SettableFuture<Boolean> jobStoppedFuture;

  @Before
  public void setUp() throws Exception {
    sparkSubmit = new JobEntrySparkSubmit( serviceLocator, jobStoppedListener );
    sparkSubmit.setParentJob( parent );
    sparkSubmit.setLogChannel( logChannel );

    sparkDir = temporaryFolder.newFolder( "spark" );
    // Copy the spark jar to the temp directory
    Files.write( "MOCK_DATA", createFile( "jars", "DATA", "TEST" ), Charsets.UTF_8 );
    sparkClient = createFile( "bin", "spark-submit" );

    // Create a mock builder
    SparkJobBuilder builder = mock( SparkJobBuilder.class, (Answer) InvocationOnMock::getMock );
    doReturn( sparkJob ).when( builder ).submit();

    // Setup mock service
    when( serviceLocator.getService( any(), eq( SparkService.class ) ) ).thenReturn( service );
    when( service.createJobBuilder( any() ) ).thenReturn( builder );

    when( jobStoppedListener.apply( parent ) ).thenReturn( jobStoppedFuture = SettableFuture.create() );
  }

  private File createFile( String... path ) throws Exception {
    File file = sparkDir;
    for ( String element : path ) {
      file = new File( file, element );
    }

    Files.createParentDirs( file );
    assertThat( file.createNewFile(), is( true ) );
    return file;
  }

  @Test
  public void testJavaConfiguration() throws Exception {
    sparkSubmit.setMaster( "master_url" );
    sparkSubmit.setJobType( JOB_TYPE_JAVA_SCALA );
    sparkSubmit.setJar( "jar_path" );
    sparkSubmit.setArgs( "${VAR1} \"double quoted string\" 'single quoted string'" );
    sparkSubmit.setVariable( "VAR1", "VAR_VALUE" );
    sparkSubmit.setClassName( "class_name" );
    sparkSubmit.setDriverMemory( "driverMemory" );
    sparkSubmit.setExecutorMemory( "executorMemory" );

    sparkSubmit.setConfigParams( ImmutableList.of( "name1=value1", "name2=value 2" ) );
    sparkSubmit.setLibs( ImmutableMap.of( "file:///path/to/lib1", "Local", "/path/to/lib2", "<Static>" ) );

    SparkJobBuilder builder = sparkSubmit.configure( mock( SparkJobBuilder.class ) );

    ImmutableMap<String, String> expectedSparkArgs = ImmutableMap.<String, String>builder()
      .put( "--master", "master_url" )
      .put( "--driver-memory", "driverMemory" )
      .put( "--executor-memory", "executorMemory" )
      .put( "--class", "class_name" )
      .put( "--jars", "file:///path/to/lib1,/path/to/lib2" )
      .build();
    ImmutableMap<String, String> expectedConf = ImmutableMap.<String, String>builder()
      .put( "name1", "value1" )
      .put( "name2", "value 2" )
      .build();
    List<String> expectedAppArgs = ImmutableList.of( "VAR_VALUE", "double quoted string", "single quoted string" );

    expectedSparkArgs.entrySet().forEach( e -> verify( builder ).setSparkArg( e.getKey(), e.getValue() ) );
    expectedConf.entrySet().forEach( e -> verify( builder ).setSparkConf( e.getKey(), e.getValue() ) );
    verify( builder ).setAppResource( "jar_path" );
    verify( builder ).addAppArgs( eq( expectedAppArgs ) );
  }

  @Test
  public void testPythonConfiguration() throws Exception {
    sparkSubmit.setMaster( "master_url" );
    sparkSubmit.setJobType( JOB_TYPE_PYTHON );
    sparkSubmit.setPyFile( "pyFile-path" );
    sparkSubmit.setLibs( ImmutableMap.of( "file:///path/to/lib1", "Local", "/path/to/lib2", "<Static>" ) );

    SparkJobBuilder builder = sparkSubmit.configure( mock( SparkJobBuilder.class ) );

    Map<String, String> expectedSparkArgs = ImmutableMap.<String, String>builder()
      .put( "--master", "master_url" )
      .put( "--py-files", "file:///path/to/lib1,/path/to/lib2" )
      .build();

    expectedSparkArgs.entrySet().forEach( e -> verify( builder ).setSparkArg( e.getKey(), e.getValue() ) );
    verify( builder ).setAppResource( "pyFile-path" );
  }

  @Test
  public void testValidate() throws Exception {
    Assert.assertFalse( sparkSubmit.validate() );
    // Use working dir which exists
    sparkSubmit.setScriptPath( temporaryFolder.newFile().getPath() );
    sparkSubmit.setMaster( "" );
    Assert.assertFalse( sparkSubmit.validate() );
    sparkSubmit.setMaster( "master-url" );
    Assert.assertFalse( "Jar path", sparkSubmit.validate() );
    sparkSubmit.setJobType( JOB_TYPE_JAVA_SCALA );
    Assert.assertFalse( "Jar path should not be empty", sparkSubmit.validate() );
    sparkSubmit.setJar( "jar-path" );
    sparkSubmit.setScriptPath( sparkClient.getPath() );
    Assert.assertTrue( "Validation should pass", sparkSubmit.validate() );
    sparkSubmit.setJobType( JobEntrySparkSubmit.JOB_TYPE_PYTHON );
    Assert.assertFalse( "Pyfile path should not be empty", sparkSubmit.validate() );
    sparkSubmit.setPyFile( "pyfile-path" );
    Assert.assertTrue( "Validation should pass", sparkSubmit.validate() );

    sparkSubmit.setScriptPath( sparkDir.getPath() );
    Assert.assertTrue( "Validation should pass", sparkSubmit.validate() );
  }

  @Test
  public void testArgsParsing() throws Exception {
    assertThat( sparkSubmit.getCmds(), hasItems() );
  }

  @Test
  public void testCheck() {
    sparkSubmit.setJobType( JOB_TYPE_JAVA_SCALA );
    List<CheckResultInterface> remarks = new ArrayList<>();
    sparkSubmit.setMaster( "" );
    sparkSubmit.check( remarks, null, null, null, null );
    Assert.assertEquals( "Number of remarks should be 4", 4, remarks.size() );

    int errors = 0;
    for ( CheckResultInterface remark : remarks ) {
      if ( remark.getType() == CheckResultInterface.TYPE_RESULT_ERROR ) {
        errors++;
      }
    }
    Assert.assertEquals( "Number of errors should be 4", 4, errors );

    remarks.clear();
    sparkSubmit.setJobType( JobEntrySparkSubmit.JOB_TYPE_PYTHON );
    sparkSubmit.check( remarks, null, null, null, null );
    Assert.assertEquals( "Number of remarks should be 4", 4, remarks.size() );

    errors = 0;
    for ( CheckResultInterface remark : remarks ) {
      if ( remark.getType() == CheckResultInterface.TYPE_RESULT_ERROR ) {
        errors++;
      }
    }
    Assert.assertEquals( "Number of errors should be 4", 4, errors );
  }

  @Test
  public void submitNonBlocking() throws Exception {
    sparkSubmit.setJobType( JOB_TYPE_JAVA_SCALA );
    sparkSubmit.setScriptPath( sparkDir.getPath() );
    sparkSubmit.setJar( "jar-path" );
    sparkSubmit.setBlockExecution( false );

    Result prev = new Result();
    when( sparkJob.getJobId() ).thenReturn( Futures.immediateFuture( "spark-job_1337" ) );

    Result result = sparkSubmit.execute( prev, 1 );

    assertThat( result.getResult(), is( true ) );
  }

  @Test
  @SuppressWarnings( "unchecked" )
  public void submitBlocking() throws Exception {
    sparkSubmit.setJobType( JOB_TYPE_JAVA_SCALA );
    sparkSubmit.setScriptPath( sparkDir.getPath() );
    sparkSubmit.setJar( "jar-path" );
    sparkSubmit.setBlockExecution( true );
    sparkSubmit.setVariable( "VAR1", "VAR_VALUE" );

    Result prev = new Result();
    when( sparkJob.getJobId() ).thenReturn( Futures.immediateFuture( "spark-job_1337" ) );
    when( sparkJob.getExitCode() ).thenReturn( Futures.immediateFuture( 0 ) );

    Result result = sparkSubmit.execute( prev, 1 );

    assertThat( result.getResult(), is( true ) );
    assertThat( jobStoppedFuture.isCancelled(), is( true ) );
    verify( service ).createJobBuilder( (Map) argThat( allOf(
      hasEntry( "VAR1", "VAR_VALUE" ),
      hasEntry( "SPARK_HOME", sparkDir.getAbsolutePath() )
    ) ) );
  }

  @Test
  public void submitFailure() throws Exception {
    sparkSubmit.setJobType( JOB_TYPE_JAVA_SCALA );
    sparkSubmit.setScriptPath( sparkDir.getPath() );
    sparkSubmit.setJar( "jar-path" );
    sparkSubmit.setBlockExecution( true );

    Result prev = new Result();
    KettleException expected = new KettleException();
    when( sparkJob.getJobId() ).thenReturn( Futures.immediateFailedFuture( expected ) );

    expectedException.expectCause( is( expected ) );
    sparkSubmit.execute( prev, 1 );
  }

  @Test
  public void blockedJobFailure() throws Exception {
    sparkSubmit.setJobType( JOB_TYPE_JAVA_SCALA );
    sparkSubmit.setScriptPath( sparkDir.getPath() );
    sparkSubmit.setJar( "jar-path" );
    sparkSubmit.setBlockExecution( true );

    Result prev = new Result();
    IOException expected = new IOException( "all your base are belong to us" );
    when( sparkJob.getJobId() ).thenReturn( Futures.immediateFuture( "spark-job_9999" ) );
    when( sparkJob.getExitCode() ).thenReturn( Futures.immediateFailedFuture( expected ) );

    expectedException.expectCause( is( expected ) );
    try {
      sparkSubmit.execute( prev, 1 );
    } finally {
      assertThat( jobStoppedFuture.isCancelled(), is( true ) );
    }
  }

  @Test
  public void jobStopped() throws Exception {
    sparkSubmit.setJobType( JOB_TYPE_JAVA_SCALA );
    sparkSubmit.setScriptPath( sparkDir.getPath() );
    sparkSubmit.setJar( "jar-path" );
    sparkSubmit.setBlockExecution( true );

    Result prev = new Result();
    when( sparkJob.getJobId() ).thenReturn( Futures.immediateFuture( "spark-job_9999" ) );
    jobStoppedFuture.set( true );
    when( sparkJob.getExitCode() ).then( invocation -> {
      verify( sparkJob ).cancel();
      return Futures.immediateCancelledFuture();
    } );

    expectedException.expectCause( instanceOf( CancellationException.class ) );
    sparkSubmit.execute( prev, 1 );
  }

}
