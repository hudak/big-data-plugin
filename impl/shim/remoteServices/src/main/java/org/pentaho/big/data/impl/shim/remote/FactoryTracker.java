package org.pentaho.big.data.impl.shim.remote;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;

import java.util.Hashtable;
import java.util.function.Supplier;

/**
 * Created by hudak on 11/18/16.
 */
@SuppressWarnings( "deprecation" ) public class FactoryTracker
  extends ServiceTracker<NamedClusterServiceFactory, ServiceRegistration> {
  private final Supplier<NamedCluster> namedClusterSupplier;

  public FactoryTracker( BundleContext bundleContext, Supplier<NamedCluster> namedClusterSupplier ) {
    super( bundleContext, NamedClusterServiceFactory.class, null );
    this.namedClusterSupplier = namedClusterSupplier;
  }

  @Override public ServiceRegistration addingService( ServiceReference<NamedClusterServiceFactory> reference ) {
    NamedClusterServiceFactory serviceFactory = context.getService( reference );
    Class<?> serviceClass = serviceFactory.getServiceClass();

    Hashtable<String, Object> properties = new Hashtable<>();
    properties.put( "service.exported.interfaces", "*" );
    NamedCluster namedCluster = namedClusterSupplier.get();
    properties.put( "pentaho.namedClusterName", namedCluster.getName() );

    BundleContext implCtxt = reference.getBundle().getBundleContext();

    return implCtxt.registerService( serviceClass.getName(), serviceFactory.create( namedCluster ), properties );
  }

  @Override
  public void modifiedService( ServiceReference<NamedClusterServiceFactory> reference, ServiceRegistration service ) {
    removedService( reference, service );
    addingService( reference );
  }

  @Override
  public void removedService( ServiceReference<NamedClusterServiceFactory> reference, ServiceRegistration service ) {
    context.ungetService( reference );
    service.unregister();
  }
}
