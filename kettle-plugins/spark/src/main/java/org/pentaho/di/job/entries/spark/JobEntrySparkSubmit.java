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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.bigdata.api.spark.SparkJob;
import org.pentaho.bigdata.api.spark.SparkService;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobEntryListener;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.io.File;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.pentaho.di.job.entry.validator.AndValidator.putValidators;
import static org.pentaho.di.job.entry.validator.JobEntryValidatorUtils.*;

/**
 * This job entry submits a JAR to Spark and executes a class. It uses the spark-submit script to submit a command like
 * this: spark-submit --class org.pentaho.spark.SparkExecTest --master yarn-cluster my-spark-job.jar arg1 arg2
 * <p>
 * More information on the options is here: http://spark.apache.org/docs/1.2.0/submitting-applications.html
 *
 * @author jdixon
 * @since Dec 3 2014
 */

@JobEntry( image = "org/pentaho/di/ui/job/entries/spark/img/spark.svg", id = "SparkSubmit",
  name = "JobEntrySparkSubmit.Title", description = "JobEntrySparkSubmit.Description",
  categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData",
  i18nPackageName = "org.pentaho.di.job.entries.spark",
  documentationUrl = "http://wiki.pentaho.com/display/EAI/Spark+Submit" )
public class JobEntrySparkSubmit extends JobEntryBase implements Cloneable, JobEntryInterface, JobEntryListener {
  public static final String JOB_TYPE_JAVA_SCALA = "Java or Scala";
  public static final String JOB_TYPE_PYTHON = "Python";

  private static Class<?> PKG = JobEntrySparkSubmit.class; // for i18n purposes, needed by Translator2!!

  private String jobType = JOB_TYPE_JAVA_SCALA;
  private String scriptPath; // the path for the spark-submit utility
  private String master = "yarn-cluster"; // the URL for the Spark master
  private Map<String, String> libs = new LinkedHashMap<>();
  // supporting documents options, "path->environment"
  private List<String> configParams = new ArrayList<String>(); // configuration options, "key=value"
  private String jar; // the path for the jar containing the Spark code to run
  private String pyFile; // path to python file for python jobs
  private String className; // the name of the class to run
  private String args; // arguments for the Spark code
  private boolean blockExecution = true; // wait for job to complete
  private String executorMemory; // memory allocation config param for the executor
  private String driverMemory; // memory allocation config param for the driver

  private final List<Runnable> afterExecution = Collections.synchronizedList( Lists.newLinkedList() );
  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private final Function<Job, ListenableFuture<Boolean>> jobStoppedListener;

  public JobEntrySparkSubmit( NamedClusterServiceLocator namedClusterServiceLocator,
                              Function<Job, ListenableFuture<Boolean>> jobStoppedListener ) {
    this.namedClusterServiceLocator = namedClusterServiceLocator;
    this.jobStoppedListener = jobStoppedListener;
  }

  /**
   * Converts the state into XML and returns it
   *
   * @return The XML for the current state
   */
  public String getXML() {
    StringBuffer retval = new StringBuffer( 200 );

    retval.append( super.getXML() );
    retval.append( "      " ).append( XMLHandler.addTagValue( "scriptPath", scriptPath ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "jobType", jobType ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "master", master ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "jar", jar ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "pyFile", pyFile ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "className", className ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "args", args ) );
    retval.append( "      " ).append( XMLHandler.openTag( "configParams" ) ).append( Const.CR );
    for ( String param : configParams ) {
      retval.append( "            " ).append( XMLHandler.addTagValue( "param", param ) );
    }
    retval.append( "      " ).append( XMLHandler.closeTag( "configParams" ) ).append( Const.CR );
    retval.append( "      " ).append( XMLHandler.openTag( "libs" ) ).append( Const.CR );

    for ( String key : libs.keySet() ) {
      retval.append( "            " ).append( XMLHandler.addTagValue( "env", libs.get( key ) ) );
      retval.append( "            " ).append( XMLHandler.addTagValue( "path", key ) );
    }
    retval.append( "      " ).append( XMLHandler.closeTag( "libs" ) ).append( Const.CR );
    retval.append( "      " ).append( XMLHandler.addTagValue( "driverMemory", driverMemory ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "executorMemory", executorMemory ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "blockExecution", blockExecution ) );
    return retval.toString();
  }

  /**
   * Parses XML and recreates the state
   */
  public void loadXML( Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers, Repository rep,
                       IMetaStore metaStore ) throws KettleXMLException {
    try {
      super.loadXML( entrynode, databases, slaveServers );

      scriptPath = XMLHandler.getTagValue( entrynode, "scriptPath" );
      master = XMLHandler.getTagValue( entrynode, "master" );
      jobType = XMLHandler.getTagValue( entrynode, "jobType" );
      if ( jobType == null ) {
        this.jobType = JOB_TYPE_JAVA_SCALA;
      }
      jar = XMLHandler.getTagValue( entrynode, "jar" );
      pyFile = XMLHandler.getTagValue( entrynode, "pyFile" );
      className = XMLHandler.getTagValue( entrynode, "className" );
      args = XMLHandler.getTagValue( entrynode, "args" );
      Node configParamsNode = XMLHandler.getSubNode( entrynode, "configParams" );
      List<Node> paramNodes = XMLHandler.getNodes( configParamsNode, "param" );
      for ( Node paramNode : paramNodes ) {
        configParams.add( paramNode.getTextContent() );
      }
      Node libsNode = XMLHandler.getSubNode( entrynode, "libs" );
      if ( libsNode != null ) {
        List<Node> envNodes = XMLHandler.getNodes( libsNode, "env" );
        List<Node> pathNodes = XMLHandler.getNodes( libsNode, "path" );
        for ( int i = 0; i < envNodes.size(); i++ ) {
          libs.put( pathNodes.get( i ).getTextContent(), envNodes.get( i ).getTextContent() );
        }
      }
      driverMemory = XMLHandler.getTagValue( entrynode, "driverMemory" );
      executorMemory = XMLHandler.getTagValue( entrynode, "executorMemory" );
      blockExecution = "Y".equalsIgnoreCase( XMLHandler.getTagValue( entrynode, "blockExecution" ) );
    } catch ( KettleXMLException xe ) {
      throw new KettleXMLException( "Unable to load job entry of type 'SparkSubmit' from XML node", xe );
    }
  }

  /**
   * Reads the state from the repository
   */
  public void loadRep( Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
                       List<SlaveServer> slaveServers ) throws KettleException {
    try {
      scriptPath = rep.getJobEntryAttributeString( id_jobentry, "scriptPath" );
      master = rep.getJobEntryAttributeString( id_jobentry, "master" );
      jobType = rep.getJobEntryAttributeString( id_jobentry, "jobType" );
      if ( jobType == null ) {
        this.jobType = JOB_TYPE_JAVA_SCALA;
      }
      jar = rep.getJobEntryAttributeString( id_jobentry, "jar" );
      pyFile = rep.getJobEntryAttributeString( id_jobentry, "pyFile" );
      className = rep.getJobEntryAttributeString( id_jobentry, "className" );
      args = rep.getJobEntryAttributeString( id_jobentry, "args" );
      for ( int i = 0; i < rep.countNrJobEntryAttributes( id_jobentry, "param" ); i++ ) {
        configParams.add( rep.getJobEntryAttributeString( id_jobentry, i, "param" ) );
      }
      for ( int i = 0; i < rep.countNrJobEntryAttributes( id_jobentry, "libsEnv" ); i++ ) {
        libs.put( rep.getJobEntryAttributeString( id_jobentry, i, "libsPath" ),
          rep.getJobEntryAttributeString( id_jobentry, i, "libsEnv" ) );
      }
      driverMemory = rep.getJobEntryAttributeString( id_jobentry, "driverMemory" );
      executorMemory = rep.getJobEntryAttributeString( id_jobentry, "executorMemory" );
      blockExecution = rep.getJobEntryAttributeBoolean( id_jobentry, "blockExecution" );
    } catch ( KettleException dbe ) {
      throw new KettleException( "Unable to load job entry of type 'SparkSubmit' from the repository for id_jobentry="
        + id_jobentry, dbe );
    }
  }

  /**
   * Saves the current state into the repository
   */
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_job ) throws KettleException {
    try {
      rep.saveJobEntryAttribute( id_job, getObjectId(), "scriptPath", scriptPath );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "master", master );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "jobType", jobType );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "jar", jar );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "pyFile", pyFile );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "className", className );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "args", args );
      for ( int i = 0; i < configParams.size(); i++ ) {
        rep.saveJobEntryAttribute( id_job, getObjectId(), i, "param", configParams.get( i ) );
      }

      int i = 0;
      for ( String key : libs.keySet() ) {
        rep.saveJobEntryAttribute( id_job, getObjectId(), i, "libsEnv", libs.get( key ) );
        rep.saveJobEntryAttribute( id_job, getObjectId(), i++, "libsPath", key );
      }
      rep.saveJobEntryAttribute( id_job, getObjectId(), "driverMemory", driverMemory );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "executorMemory", executorMemory );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "blockExecution", blockExecution );
    } catch ( KettleDatabaseException dbe ) {
      throw new KettleException( "Unable to save job entry of type 'SparkSubmit' to the repository for id_job="
        + id_job, dbe );
    }
  }

  /**
   * Returns the path for the spark-submit utility
   *
   * @return The script path
   */
  public String getScriptPath() {
    return scriptPath;
  }

  /**
   * Sets the path for the spark-submit utility
   *
   * @param scriptPath path to spark-submit utility
   */
  public void setScriptPath( String scriptPath ) {
    this.scriptPath = scriptPath;
  }

  /**
   * Returns the URL for the Spark master node
   *
   * @return The URL for the Spark master node
   */
  public String getMaster() {
    return master;
  }

  /**
   * Sets the URL for the Spark master node
   *
   * @param master URL for the Spark master node
   */
  public void setMaster( String master ) {
    this.master = master;
  }

  /**
   * Returns map of configuration params
   *
   * @return map of configuration params
   */
  public List<String> getConfigParams() {
    return configParams;
  }

  /**
   * Sets configuration params
   */
  public void setConfigParams( List<String> configParams ) {
    this.configParams = configParams;
  }

  /**
   * Returns list of library-env pairs.
   *
   * @return list of libs.
   */
  public Map<String, String> getLibs() {
    return libs;
  }

  /**
   * Sets path-env pairs for libraries
   */
  public void setLibs( Map<String, String> docs ) {
    this.libs = docs;
  }

  /**
   * Returns the path for the jar containing the Spark code to execute
   *
   * @return The path for the jar
   */
  public String getJar() {
    return jar;
  }

  /**
   * Sets the path for the jar containing the Spark code to execute
   *
   * @param jar path for the jar
   */
  public void setJar( String jar ) {
    this.jar = jar;
  }

  /**
   * Returns the name of the class containing the Spark code to execute
   *
   * @return The name of the class
   */
  public String getClassName() {
    return className;
  }

  /**
   * Sets the name of the class containing the Spark code to execute
   *
   * @param className name of the class
   */
  public void setClassName( String className ) {
    this.className = className;
  }

  /**
   * Returns the arguments for the Spark class. This is a space-separated list of strings, e.g. "http.log 1000"
   *
   * @return The arguments
   */
  public String getArgs() {
    return args;
  }

  /**
   * Sets the arguments for the Spark class. This is a space-separated list of strings, e.g. "http.log 1000"
   *
   * @param args arguments
   */
  public void setArgs( String args ) {
    this.args = args;
  }

  /**
   * Returns executor memory config param's value
   *
   * @return executor memory config param
   */
  public String getExecutorMemory() {
    return executorMemory;
  }

  /**
   * Sets executor memory config param's value
   *
   * @param executorMemory amount of memory executor process is allowed to consume
   */
  public void setExecutorMemory( String executorMemory ) {
    this.executorMemory = executorMemory;
  }

  /**
   * Returns driver memory config param's value
   *
   * @return driver memory config param
   */
  public String getDriverMemory() {
    return driverMemory;
  }

  /**
   * Sets driver memory config param's value
   *
   * @param driverMemory amount of memory driver process is allowed to consume
   */
  public void setDriverMemory( String driverMemory ) {
    this.driverMemory = driverMemory;
  }

  /**
   * Returns if the job entry will wait till job execution completes
   *
   * @return blocking mode
   */
  public boolean isBlockExecution() {
    return blockExecution;
  }

  /**
   * Sets if the job entry will wait for job execution to complete
   *
   * @param blockExecution blocking mode
   */
  public void setBlockExecution( boolean blockExecution ) {
    this.blockExecution = blockExecution;
  }

  /**
   * Returns type of job, valid types are {@link #JOB_TYPE_JAVA_SCALA} and {@link #JOB_TYPE_PYTHON}.
   *
   * @return spark job's type
   */
  public String getJobType() {
    return jobType;
  }

  /**
   * Sets spark job type to be executed, valid types are {@link #JOB_TYPE_JAVA_SCALA} and {@link #JOB_TYPE_PYTHON}..
   *
   * @param jobType to be set
   */
  public void setJobType( String jobType ) {
    this.jobType = jobType;
  }

  /**
   * Returns path to job's python file. Valid for jobs of {@link #JOB_TYPE_PYTHON} type.
   *
   * @return path to python script
   */
  public String getPyFile() {
    return pyFile;
  }

  /**
   * Sets path to python script to be executed. Valid for jobs of {@link #JOB_TYPE_PYTHON} type.
   *
   * @param pyFile path to set
   */
  public void setPyFile( String pyFile ) {
    this.pyFile = pyFile;
  }

  /**
   * Returns the spark-submit command as a list of strings. e.g. <path to spark-submit> --class <main-class> --master
   * <master-url> --deploy-mode <deploy-mode> --conf <key>=<value> <application-jar> \ [application-arguments]
   *
   * @return The spark-submit command
   */
  public List<String> getCmds() {
    List<String> cmds = new ArrayList<>();

    cmds.add( "--master" );
    cmds.add( environmentSubstitute( master ) );

    for ( String confParam : configParams ) {
      cmds.add( "--conf" );
      cmds.add( environmentSubstitute( confParam ) );
    }

    if ( !Strings.isNullOrEmpty( driverMemory ) ) {
      cmds.add( "--driver-memory" );
      cmds.add( environmentSubstitute( driverMemory ) );
    }

    if ( !Strings.isNullOrEmpty( executorMemory ) ) {
      cmds.add( "--executor-memory" );
      cmds.add( environmentSubstitute( executorMemory ) );
    }

    switch( jobType ) {
      case JOB_TYPE_JAVA_SCALA: {
        if ( !Strings.isNullOrEmpty( className ) ) {
          cmds.add( "--class" );
          cmds.add( environmentSubstitute( className ) );
        }

        if ( !libs.isEmpty() ) {
          cmds.add( "--jars" );
          cmds.add( environmentSubstitute( Joiner.on( ',' ).join( libs.keySet() ) ) );
        }

        cmds.add( environmentSubstitute( jar ) );

        break;
      }
      case JOB_TYPE_PYTHON: {
        if ( !libs.isEmpty() ) {
          cmds.add( "--py-files" );
          cmds.add( environmentSubstitute( Joiner.on( ',' ).join( libs.keySet() ) ) );
        }

        cmds.add( environmentSubstitute( pyFile ) );

        break;
      }
    }

    if ( !Strings.isNullOrEmpty( args ) ) {
      List<String> argArray = parseCommandLine( args );
      for ( String anArg : argArray ) {
        if ( !Strings.isNullOrEmpty( anArg ) ) {
          cmds.add( anArg );
        }
      }
    }

    return cmds;
  }

  @VisibleForTesting
  protected boolean validate() {
    boolean valid = true;
    if ( !getSparkRoot().isPresent() ) {
      logError( getString( "JobEntrySparkSubmit.Error.SparkSubmitPathInvalid" ) );
      valid = false;
    }

    if ( Strings.isNullOrEmpty( master ) ) {
      logError( getString( "JobEntrySparkSubmit.Error.MasterURLEmpty" ) );
      valid = false;
    }

    if ( JOB_TYPE_JAVA_SCALA.equals( getJobType() ) ) {
      if ( Strings.isNullOrEmpty( jar ) ) {
        logError( getString( "JobEntrySparkSubmit.Error.JarPathEmpty" ) );
        valid = false;
      }
    } else {
      if ( Strings.isNullOrEmpty( pyFile ) ) {
        logError( getString( "JobEntrySparkSubmit.Error.PyFilePathEmpty" ) );
        valid = false;
      }
    }

    return valid;
  }

  /**
   * Executes the spark-submit command and returns a Result
   *
   * @return The Result of the operation
   */
  public Result execute( Result result, int nr ) throws KettleException {
    if ( !validate() ) {
      logError( getString( getString( "JobEntrySparkSubmit.Error.SubmittingScript", "validation failed" ) ) );
      result.setResult( false );
      return result;
    }

    final SparkService service;
    try {
      // TODO a named cluster should be associated with the service
      service = namedClusterServiceLocator.getService( null, SparkService.class );
    } catch ( Exception e ) {
      throw new KettleException( getString( "JobEntrySparkSubmit.Error.SubmittingScript", e.getMessage() ), e );
    }

    URL[] sparkJars = getSparkRoot()
      .flatMap( root -> Optional.ofNullable( new File( root, "jars" ).listFiles() ) )
      .map( Arrays::stream )
      .orElseThrow( () ->
        new KettleException( getString( "JobEntrySparkSubmit.Error.SubmittingScript", "spark libraries not found" ) )
      )
      .map( jar -> {
        try {
          return jar.toURI().toURL();
        } catch ( MalformedURLException e ) {
          throw new IllegalStateException( e );
        }
      } )
      .toArray( URL[]::new );

    SparkJob sparkJob = service.createJobBuilder( log, ( parent ) -> new URLClassLoader( sparkJars, parent ) )
      .addArguments( getCmds() )
      .addEnvironmentVariables( getEnv() )
      .submit();

    // Ensure spark job started successfully
    String jobId = Futures.get( sparkJob.getJobId(), KettleException.class );
    logBasic( getString( "JobEntrySparkSubmit.Start", jobId ) );

    final int exitCode;
    if ( isBlockExecution() ) {
      // Cancel spark if parent job stops running for any reason
      afterExecution.add( sparkJob::cancel );
      // Also interrupt spark if job is trying to halt
      ListenableFuture<Boolean> jobStopped = jobStoppedListener.apply( getParentJob() );
      jobStopped.addListener( sparkJob::cancel, MoreExecutors.directExecutor() );

      // Wait for completion
      logBasic( getString( "JobEntrySparkSubmit.Block" ) );
      try {
        exitCode = Futures.get( sparkJob.getExitCode(), KettleException.class );
        logDetailed( getString( "JobEntrySparkSubmit.ExitStatus" ), exitCode );
      } finally {
        // Stop polling jobStopped
        jobStopped.cancel( true );
      }
    } else {
      exitCode = 0;
    }

    // Record results
    result.setExitStatus( exitCode );
    result.setNrErrors( exitCode == 0 ? 0 : 1 );
    result.setResult( exitCode == 0 );

    return result;
  }

  /**
   * Starts a recursive search up the file system for the root directory of the spark installation.
   * {@link #scriptPath} must be set to any file or directory within the installation.
   * The location of the spark-core jar is used to test the directory
   *
   * @return Root directory of spark installation, or {@link Optional#empty()} if {@link #scriptPath} is invalid
   */
  private Optional<File> getSparkRoot() {
    return Optional.ofNullable( Strings.emptyToNull( scriptPath ) ) // Ensure supplied script path is not empty or null
      .map( this::environmentSubstitute )
      .map( ( path ) -> new File( path ).getAbsoluteFile() ) // convert to absolute path file
      .flatMap( this::resolveSparkRoot ); // Start recursive search
  }

  private static final Pattern sparkCoreJar = Pattern.compile( "spark-core.*[.]jar" );

  /**
   * Recursive search for the root directory of the spark folder.
   * <pre>jars/spark-core*.jar</pre> is used to check the given directory.
   *
   * @param file starting location (file or folder)
   * @return root directory if found
   */
  private Optional<File> resolveSparkRoot( File file ) {
    boolean valid = Optional.ofNullable( new File( file, "jars" ).listFiles() )
      .map( Stream::of ).orElseGet( Stream::empty ) // iterate over jars/ directory
      .filter( File::isFile ) // Check if file is real
      .anyMatch( f -> sparkCoreJar.matcher( f.getName() ).matches() );

    // Return if valid, otherwise try parent
    return valid ? Optional.of( file ) : Optional.ofNullable( file.getParentFile() ).flatMap( this::resolveSparkRoot );
  }

  private Map<String, String> getEnv() {
    return Stream.of( listVariables() ).collect( Collectors.toMap( Function.identity(), this::getVariable ) );
  }

  public boolean evaluates() {
    return true;
  }

  /**
   * Checks that the minimum options have been provided.
   */
  @Override
  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space, Repository repository,
                     IMetaStore metaStore ) {
    andValidator().validate( this, "scriptPath", remarks, putValidators( notBlankValidator() ) );
    andValidator().validate( this, "scriptPath", remarks, putValidators( fileExistsValidator() ) );
    andValidator().validate( this, "master", remarks, putValidators( notBlankValidator() ) );
    if ( JOB_TYPE_JAVA_SCALA.equals( getJobType() ) ) {
      andValidator().validate( this, "jar", remarks, putValidators( notBlankValidator() ) );
    } else {
      andValidator().validate( this, "pyFile", remarks, putValidators( notBlankValidator() ) );
    }
  }

  /**
   * Parse a string into arguments as if it were provided on the command line.
   *
   * @param commandLineString A command line string.
   * @return List of parsed arguments
   * @throws IOException when the command line could not be parsed
   */
  public List<String> parseCommandLine( String commandLineString ) {
    List<String> args = new ArrayList<>();
    StringReader reader = new StringReader( commandLineString );
    try {
      StreamTokenizer tokenizer = new StreamTokenizer( reader );
      // Treat a dash as an ordinary character so it gets included in the token
      tokenizer.ordinaryChar( '-' );
      tokenizer.ordinaryChar( '.' );
      tokenizer.ordinaryChars( '0', '9' );
      // Treat all characters as word characters so nothing is parsed out
      tokenizer.wordChars( '\u0000', '\uFFFF' );

      // Re-add whitespace characters
      tokenizer.whitespaceChars( 0, ' ' );

      // Use " and ' as quote characters
      tokenizer.quoteChar( '"' );
      tokenizer.quoteChar( '\'' );

      // Add all non-null string values tokenized from the string to the argument list
      while ( tokenizer.nextToken() != StreamTokenizer.TT_EOF ) {
        if ( tokenizer.sval != null ) {
          String s = tokenizer.sval;
          s = environmentSubstitute( s );
          args.add( s );
        }
      }
    } catch ( IOException e ) {
      throw new IllegalArgumentException( e );
    } finally {
      reader.close();
    }

    return args;
  }

  @Override
  public void afterExecution( Job arg0, JobEntryCopy arg1, JobEntryInterface arg2, Result arg3 ) {
    synchronized( afterExecution ) {
      afterExecution.forEach( Runnable::run );
      afterExecution.clear();
    }
  }

  @Override
  public void beforeExecution( Job arg0, JobEntryCopy arg1, JobEntryInterface arg2 ) {
  }

  @VisibleForTesting
  protected void setLogChannel( LogChannelInterface log ) {
    this.log = log;
  }

  private static String getString( String id, Object... arguments ) {
    return BaseMessages.getString( PKG, id, arguments );
  }

}
