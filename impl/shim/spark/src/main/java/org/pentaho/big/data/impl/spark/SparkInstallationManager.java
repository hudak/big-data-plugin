package org.pentaho.big.data.impl.spark;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.pentaho.bigdata.api.spark.SparkEnvironmentModifier;
import org.pentaho.di.core.Const;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by hudak on 12/2/16.
 */
public class SparkInstallationManager implements SparkEnvironmentModifier, ManagedService {
  public static final String ENV_HOME = "SPARK_HOME";
  public static final String PID = "org.pentaho.big.data.spark";
  private final AtomicReference<CompletableFuture<String>> systemSparkHome = new AtomicReference<>( null );
  private final ConfigurationAdmin configurationAdmin;
  private final ArchiveInstaller archiveInstaller;
  private final ExecutorService executorService;

  public SparkInstallationManager( ConfigurationAdmin configurationAdmin,
                                   ArchiveInstaller archiveInstaller,
                                   ExecutorService executorService ) {
    this.configurationAdmin = configurationAdmin;
    this.archiveInstaller = archiveInstaller;
    this.executorService = executorService;
  }

  @Override public CompletableFuture<Map<String, String>> apply( Map<String, String> env ) {
    if ( env.containsKey( ENV_HOME ) ) {
      return CompletableFuture.completedFuture( env );
    }
    return getSparkHome().thenApply( sparkHome ->
      ImmutableMap.<String, String>builder()
        .putAll( env )
        .put( ENV_HOME, sparkHome )
        .build()
    );
  }

  private CompletableFuture<String> getSparkHome() {
    // Check if spark home has not yet been initialized
    CompletableFuture<String> init = new CompletableFuture<>();
    if ( init == systemSparkHome
      .updateAndGet( future -> future == null || future.isCompletedExceptionally() ? init : future ) ) {
      executorService.execute( this::init );
    }

    return systemSparkHome.get();
  }

  private void init() {
    try ( InputStream inputStream = new FileInputStream( getSparkArchiveLocation() ) ) {
      // TODO need better determination of where to install
      File sparkHome = archiveInstaller.install( inputStream, new File( Const.getUserHomeDirectory() ) );
      Configuration config = configurationAdmin.getConfiguration( PID );
      Dictionary<String, Object> properties = Optional.of( config.getProperties() ).orElseGet( Hashtable::new );

      properties.put( ENV_HOME, sparkHome.getAbsolutePath() );
      config.update( properties );
    } catch ( Exception e ) {
      updateException( e );
    }
  }

  private String getSparkArchiveLocation() {
    // TODO need better management of spark archive
    return Optional.ofNullable( System.getProperty( "SPARK_ARCHIVE" ) )
      .orElseThrow( () -> new RuntimeException( "SPARK_ARCHIVE is not set" ) );
  }

  @Override public synchronized void updated( Dictionary<String, ?> dictionary ) throws ConfigurationException {
    Optional.ofNullable( dictionary )
      .map( dict -> (String) dict.get( ENV_HOME ) )
      .map( Strings::emptyToNull )
      .flatMap( Optional::ofNullable )
      .ifPresent( this::updateSparkHome );
  }

  private void updateSparkHome( String sparkHome ) {
    systemSparkHome.updateAndGet( future -> {
      if ( sparkHome == null ) {
        return null;
      } else if ( future != null && future.complete( sparkHome ) ) {
        return future;
      } else {
        return CompletableFuture.completedFuture( sparkHome );
      }
    } );
  }

  private void updateException( Exception e ) {
    systemSparkHome.updateAndGet( future -> {
      if ( future == null || !future.completeExceptionally( e ) ) {
        future = new CompletableFuture<>();
        future.completeExceptionally( e );
      }
      return future;
    } );
  }

}
