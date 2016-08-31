package org.pentaho.big.data.impl.spark;

import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.bigdata.api.spark.SparkEnvironmentModifier;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationClassLoader;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Created by hudak on 11/30/16.
 */
class ShimConfigModifier implements SparkEnvironmentModifier {
  private final HadoopConfiguration hadoopConfiguration;

  public ShimConfigModifier( HadoopConfiguration hadoopConfiguration ) {
    this.hadoopConfiguration = hadoopConfiguration;
  }

  @Override public CompletableFuture<Map<String, String>> apply( Map<String, String> env ) {
    HashMap<String, String> hashEnv = new HashMap<>( env );

    hashEnv.computeIfAbsent( "HADOOP_CONF_DIR", variable -> getHadoopConfDir() );
    hashEnv.computeIfAbsent( "SPARK_DIST_CLASSPATH", variable -> generateClassPath() );

    return CompletableFuture.completedFuture( Collections.unmodifiableMap( hashEnv ) );
  }

  private String generateClassPath() {
    HadoopConfigurationClassLoader classLoader = (HadoopConfigurationClassLoader) HadoopShim.class.getClassLoader();
    return classLoader.generateClassPathString();
  }

  private String getHadoopConfDir() {
    try {
      return hadoopConfiguration.getLocation().getURL().getPath();
    } catch ( FileSystemException e ) {
      throw new IllegalStateException( "Unable to locate shim", e );
    }
  }
}
