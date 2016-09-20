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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.spark.SparkException;
import org.apache.spark.deploy.SparkSubmit;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
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
import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.*;
import static org.pentaho.di.job.entries.spark.JobEntrySparkSubmit.JOB_TYPE_JAVA_SCALA;
import static org.pentaho.di.job.entries.spark.JobEntrySparkSubmit.JOB_TYPE_PYTHON;

@RunWith( MockitoJUnitRunner.class )
public class JobEntrySparkSubmitTest {
  private static final String SPARK_CORE_JAR = "spark-core_SOME_VERSION_DOESNT_MATTER.jar";

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
  private List<String> builderArgs;
  private HashMap<Object, Object> builderEnv;

  @Before
  public void setUp() throws Exception {
    sparkSubmit = new JobEntrySparkSubmit( serviceLocator, jobStoppedListener );
    sparkSubmit.setParentJob( parent );
    sparkSubmit.setLogChannel( logChannel );

    sparkDir = temporaryFolder.newFolder( "spark" );
    // Copy the spark jar to the temp directory
    Files.copy( getSparkJar(), createFile( "jars", SPARK_CORE_JAR ) );
    Files.write( "MOCK_DATA", createFile( "jars", "DATA", "TEST" ), Charsets.UTF_8 );
    sparkClient = createFile( "bin", "spark-client" );

    builderArgs = Lists.newArrayList();
    builderEnv = Maps.newHashMap();
    SparkJobBuilder builder = new SparkJobBuilder() {
      @Override public SparkJobBuilder addArguments( List<String> cmds ) {
        builderArgs.addAll( cmds );
        return this;
      }

      @Override public SparkJobBuilder addEnvironmentVariables( Map<String, String> env ) {
        builderEnv.putAll( env );
        return this;
      }

      @Override public SparkJob submit() {
        return sparkJob;
      }
    };
    when( serviceLocator.getService( any(), eq( SparkService.class ) ) ).thenReturn( service );
    when( service.createJobBuilder( same( logChannel ), argThat( new ClassLoaderWithSpark() ) ) ).thenReturn( builder );

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
  public void testGetCmds() throws Exception {
    sparkSubmit.setMaster( "master_url" );
    sparkSubmit.setJobType( JOB_TYPE_JAVA_SCALA );
    sparkSubmit.setJar( "jar_path" );
    sparkSubmit.setArgs( "arg1 arg2" );
    sparkSubmit.setClassName( "class_name" );
    sparkSubmit.setDriverMemory( "driverMemory" );
    sparkSubmit.setExecutorMemory( "executorMemory" );

    sparkSubmit.setConfigParams( ImmutableList.of( "name1=value1", "name2=value 2" ) );

    Map<String, String> libs = new LinkedHashMap<>();
    libs.put( "file:///path/to/lib1", "Local" );
    libs.put( "/path/to/lib2", "<Static>" );
    sparkSubmit.setLibs( libs );

    String[] expected = new String[] { "--master", "master_url", "--conf", "name1=value1", "--conf",
      "name2=value 2", "--driver-memory", "driverMemory", "--executor-memory",
      "executorMemory", "--class", "class_name", "--jars", "file:///path/to/lib1,/path/to/lib2", "jar_path", "arg1",
      "arg2" };
    Assert.assertArrayEquals( expected, sparkSubmit.getCmds().toArray() );

    sparkSubmit.setJobType( JOB_TYPE_PYTHON );
    sparkSubmit.setPyFile( "pyFile-path" );
    expected = new String[] { "--master", "master_url", "--conf", "name1=value1", "--conf",
      "name2=value 2", "--driver-memory", "driverMemory", "--executor-memory",
      "executorMemory", "--py-files", "file:///path/to/lib1,/path/to/lib2", "pyFile-path", "arg1", "arg2" };
    Assert.assertArrayEquals( expected, sparkSubmit.getCmds().toArray() );
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
    sparkSubmit.setArgs( "${VAR1} \"double quoted string\" 'single quoted string'" );
    sparkSubmit.setVariable( "VAR1", "VAR_VALUE" );
    assertThat( sparkSubmit.getCmds(), hasItems( "VAR_VALUE", "double quoted string", "single quoted string" ) );
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

    assertThat( builderArgs, hasItems( "jar-path" ) );
    assertThat( result.getResult(), is( true ) );
  }

  @Test
  public void submitBlocking() throws Exception {
    sparkSubmit.setJobType( JOB_TYPE_JAVA_SCALA );
    sparkSubmit.setScriptPath( sparkDir.getPath() );
    sparkSubmit.setJar( "jar-path" );
    sparkSubmit.setBlockExecution( true );

    Result prev = new Result();
    when( sparkJob.getJobId() ).thenReturn( Futures.immediateFuture( "spark-job_1337" ) );
    when( sparkJob.getExitCode() ).thenReturn( Futures.immediateFuture( 0 ) );

    Result result = sparkSubmit.execute( prev, 1 );

    assertThat( result.getResult(), is( true ) );
    assertThat( builderArgs, hasItems( "jar-path" ) );
    assertThat( jobStoppedFuture.isCancelled(), is( true ) );
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
    SparkException expected = new SparkException( "all your base are belong to us" );
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

  private static File getSparkJar() throws IOException {
    String sparkClassName = SparkSubmit.class.getName();
    String resourceName = sparkClassName.replace( ".", "/" ) + ".class";

    URL resource = Resources.getResource( resourceName );

    return new File( ( (JarURLConnection) resource.openConnection() ).getJarFile().getName() );
  }

  private class ClassLoaderWithSpark extends TypeSafeMatcher<UnaryOperator<ClassLoader>> {
    @Override protected boolean matchesSafely( UnaryOperator<ClassLoader> classLoaderFactory ) {
      Thread currentThread = Thread.currentThread();
      ClassLoader tccl = currentThread.getContextClassLoader();
      ClassLoader parent = mock( ClassLoader.class );
      ClassLoader sparkClassloader = classLoaderFactory.apply( parent );

      assertThat( sparkClassloader.getParent(), is( parent ) );

      try {
        currentThread.setContextClassLoader( sparkClassloader );
        URL resource = Resources.getResource( "TEST" );
        return Resources.toString( resource, Charsets.UTF_8 ).equals( "MOCK_DATA" );
      } catch ( IOException e ) {
        throw new AssertionError( e );
      } finally {
        currentThread.setContextClassLoader( tccl );
      }
    }

    @Override public void describeTo( Description description ) {
      description.appendText( "ClassLoader with resource TEST == MOCK_DATA" );
    }
  }
}
