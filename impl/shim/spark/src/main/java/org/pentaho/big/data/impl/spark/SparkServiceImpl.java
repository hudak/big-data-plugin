package org.pentaho.big.data.impl.spark;

import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.bigdata.api.spark.SparkJobBuilder;
import org.pentaho.bigdata.api.spark.SparkService;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.hadoop.shim.HadoopConfiguration;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;
import java.util.function.UnaryOperator;

/**
 * Created by hudak on 9/14/16.
 */
public class SparkServiceImpl implements SparkService {
  private final HadoopConfiguration hadoopConfiguration;
  private final ExecutorService executorService;
  private static final String NATIVE_SPARK_SUBMIT = "org.pentaho.big.data.impl.spark.shim.NativeSubmitService";

  public SparkServiceImpl( HadoopConfiguration hadoopConfiguration, ExecutorService executorService ) {
    this.hadoopConfiguration = hadoopConfiguration;
    this.executorService = executorService;
  }

  @Override
  public SparkJobBuilder createJobBuilder( LogChannelInterface log, UnaryOperator<ClassLoader> sparkClassloaderFactory )
    throws KettleException {
    return new SparkJobBuilderImpl( ( args, env ) -> {
      ClassLoader sparkClassLoader = createSparkClassLoader( sparkClassloaderFactory );
      SubmitService submitUtil = createSubmitService( log, sparkClassLoader );
      env.put( "HADOOP_CONF_DIR", getHadoopConfDir() );
      return submitUtil.submit( args, env );
    } );
  }

  private String getHadoopConfDir() throws KettleException {
    try {
      return hadoopConfiguration.getLocation().getURL().getPath();
    } catch ( FileSystemException e ) {
      throw new KettleException( "Unable to locate shim", e );
    }
  }

  private ClassLoader createSparkClassLoader( UnaryOperator<ClassLoader> sparkClassloaderFactory ) {
    ClassLoader shimBridgingClassloader = getClass().getClassLoader();
    return sparkClassloaderFactory.apply( shimBridgingClassloader );
  }

  protected SubmitService createSubmitService( LogChannelInterface log, ClassLoader sparkClassLoader )
    throws KettleException {
    try {
      return ( (SubmitService) sparkClassLoader.loadClass( NATIVE_SPARK_SUBMIT )
        .getConstructor( LogChannelInterface.class )
        .newInstance( log ) );
    } catch ( InvocationTargetException e ) {
      throw new KettleException( e.getCause() );
    } catch ( ReflectiveOperationException e ) {
      throw new KettleException( e );
    }
  }
}
