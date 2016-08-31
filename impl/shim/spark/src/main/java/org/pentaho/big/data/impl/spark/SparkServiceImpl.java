package org.pentaho.big.data.impl.spark;

import com.google.common.collect.Maps;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.spark.launcher.SparkLauncher;
import org.pentaho.bigdata.api.spark.SparkJobBuilder;
import org.pentaho.bigdata.api.spark.SparkService;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationClassLoader;

import java.util.Map;

/**
 * Created by hudak on 9/14/16.
 */
public class SparkServiceImpl implements SparkService {
  private final HadoopConfiguration hadoopConfiguration;

  public SparkServiceImpl( HadoopConfiguration hadoopConfiguration ) {
    this.hadoopConfiguration = hadoopConfiguration;
  }

  @Override public SparkJobBuilder createJobBuilder( Map<String, String> env ) throws KettleException {
    // Setup the hadoop environment
    env = configureEnvironment( env );

    // Create a new builder
    return new SparkJobBuilderImpl( new SparkLauncher( env ), SparkJobImpl::create );
  }

  private Map<String, String> configureEnvironment( Map<String, String> env ) {
    env = Maps.newHashMap( env );
    env.computeIfAbsent( "HADOOP_CONF_DIR", this::getHadoopConfDir );
    env.computeIfAbsent( "SPARK_DIST_CLASSPATH", this::generateClassPath );
    return env;
  }

  protected String getHadoopConfDir( String variable ) {
    try {
      return hadoopConfiguration.getLocation().getURL().getPath();
    } catch ( FileSystemException e ) {
      throw new IllegalStateException( "Unable to locate shim", e );
    }
  }

  protected String generateClassPath( String variable ) {
    HadoopConfigurationClassLoader classLoader =
      (HadoopConfigurationClassLoader) hadoopConfiguration.getHadoopShim().getClass().getClassLoader();
    return classLoader.generateClassPathString();
  }

}
