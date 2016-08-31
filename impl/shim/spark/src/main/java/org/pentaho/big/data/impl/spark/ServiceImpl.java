package org.pentaho.big.data.impl.spark;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.spark.launcher.SparkLauncher;
import org.pentaho.bigdata.api.spark.SparkEnvironmentModifier;
import org.pentaho.bigdata.api.spark.SparkJobBuilder;
import org.pentaho.bigdata.api.spark.SparkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Created by hudak on 9/14/16.
 */
class ServiceImpl implements SparkService {
  private static final Logger LOGGER = LoggerFactory.getLogger( ServiceImpl.class );
  private final List<SparkEnvironmentModifier> envModifiers;
  @VisibleForTesting Function<Map<String, String>, SparkLauncher> launcherFactory = SparkLauncher::new;

  ServiceImpl( List<SparkEnvironmentModifier> envModifiers ) {
    this.envModifiers = envModifiers;
  }

  @Override public SparkJobBuilder createJobBuilder( Map<String, String> env ) {
    // Configure spark environment
    Map<String, String> effectiveEnv = envModifiers.parallelStream().reduce(
      // Starting with the user's environment...
      CompletableFuture.completedFuture( Optional.ofNullable( env ).orElse( ImmutableMap.of() ) ),
      // Apply modifiers (may be asynchronous)
      ( envFuture, modifier ) -> envFuture.thenCompose( safeApply( modifier ) ),
      // Merge environments if necessary
      ( left, right ) -> left.thenCombine( right, ServiceImpl::combine )
    ).join();

    // Create a new builder
    return new SparkJobBuilderImpl( launcherFactory.apply( effectiveEnv ), JobMonitor::start );
  }

  private SparkEnvironmentModifier safeApply( SparkEnvironmentModifier modifier ) {
    return ( env ) -> {
      try {
        return modifier.apply( ImmutableMap.copyOf( env ) );
      } catch ( Throwable t ) {
        LOGGER.error( "Problem configuring SPARK environment, rolling back changes: " + modifier, t );
        return CompletableFuture.completedFuture( env );
      }
    };
  }

  private static Map<String, String> combine( Map<String, String> left, Map<String, String> right ) {
    MapDifference<String, String> diff = Maps.difference( left, right );
    diff.entriesDiffering()
      .forEach( ( k, v ) -> LOGGER.warn( "Conflicting environment values for {}: {}. Using {}", k, v, v.leftValue() ) );

    return ImmutableMap.<String, String>builder()
      .putAll( left )
      .putAll( diff.entriesOnlyOnRight() )
      .build();
  }
}
