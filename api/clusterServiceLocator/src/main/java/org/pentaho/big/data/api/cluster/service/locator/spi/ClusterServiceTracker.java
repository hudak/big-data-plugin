package org.pentaho.big.data.api.cluster.service.locator.spi;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Created by hudak on 11/18/16.
 */
public class ClusterServiceTracker<S> extends ServiceTracker<S, S>
  implements NamedClusterServiceLocator {

  public ClusterServiceTracker( BundleContext context, String className ) {
    super( context, className, null );
  }

  @Override public <T> T getService( NamedCluster namedCluster, Class<T> serviceClass )
    throws ClusterInitializationException {
    return Arrays.stream( getServiceReferences() )
      .filter( byNamedCluster( namedCluster ) )
      .map( this::getService )
      .map( serviceClass::cast )
      .findFirst().orElse( null );
  }

  private Predicate<ServiceReference> byNamedCluster( NamedCluster namedCluster ) {
    return ref -> Optional.ofNullable( ref.getProperty( "namedClusterName" ) )
      .map( Object::toString )
      .map( namedCluster.getName()::equalsIgnoreCase )
      .orElse( false );
  }
}
