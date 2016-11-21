package org.pentaho.big.data.impl.shim.remote;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.api.initializer.ClusterInitializerProvider;

import java.util.function.Supplier;

/**
 * Created by hudak on 11/18/16.
 */
public class ClusterTracker extends ServiceTracker<ClusterInitializerProvider, ClusterInitializerProvider> {
  private final Supplier<NamedCluster> namedClusterSupplier;

  public ClusterTracker( BundleContext bundleContext, Supplier<NamedCluster> namedClusterSupplier ) {
    super( bundleContext, ClusterInitializerProvider.class, null );
    this.namedClusterSupplier = namedClusterSupplier;
  }

  @Override
  public ClusterInitializerProvider addingService( ServiceReference<ClusterInitializerProvider> reference ) {
    ClusterInitializerProvider provider = super.addingService( reference );
    NamedCluster namedCluster = namedClusterSupplier.get();
    try {
      if ( provider.canHandle( namedCluster ) ) {
        provider.initialize( namedCluster );
      }
    } catch ( ClusterInitializationException e ) {
      e.printStackTrace();
    }
    return provider;
  }
}
