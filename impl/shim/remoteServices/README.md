# Remote Shim prototype

This branch explores the possibility of using the [Aries Remote Service Admin](https://github.com/apache/aries-rsa/) to defer shim loading to a separate JVM.

This prototype focuses on the connection between kettle plugins and service APIs by publishing NamedCluster services as remote endpoints. Services are classified by named cluster, allowing the possibility of concurrent calls to different configured clusters.

## Install
1) Start with a clean pdi-ce installation

2) Configure karaf

  - **Add repositories** to org.ops4j.pax.url.mvn.cfg

```cfg
https://repo1.maven.org/maven2/
http://central.maven.org/maven3/
```
  - **Add feature repo** to org.apache.karaf.features.cfg

```cfg
mvn:org.apache.aries.rsa/rsa-features/1.9.0/xml/features
```
  - **Create** org.apache.aries.rsa.provider.fastbin.cfg

```cfg
uri = tcp://0.0.0.0:0
```
3) Build and deploy to karaf/system

  - api/clusterServiceLocator
  - impl/shim/remoteServices
  - assemblies/features
  - Any updated kettle-plugins

4) Configure a shim

## Usage
1) Start a carte server. From karaf-ssh, run:

```
feature:install pentaho-big-data-agent-server
```

2) Run Spoon(s)

3) Create a named cluster named "default"

4) Calls into big data plugin shim-impls should route through carte!

## How it works
The big-data-plugin feature is redefined to load only APIs and kettle plugins. Instead of loading shim implementations, it installs the remote-service-admin with ZooKeeper discovery (connecting to localhost:2181) and the Fastbin provider.

```xml
  <feature name="pentaho-big-data-plugin-testless-osgi" version="1.0">
    <feature>pentaho-big-data-kettle-plugins</feature>
    <feature>pentaho-big-data-plugin-api</feature>
    <feature>scr</feature>
    <feature>aries-rsa-provider-fastbin</feature>
    <feature>aries-rsa-discovery-zookeeper</feature>
  </feature>
```

If the agent-server feature is installed to a carte instance, it configures it's ZooKeeper node to run as a server and loads the shim impl bundles.

```xml
  <feature name="pentaho-big-data-agent-server" version="1.0">
    <feature>aries-rsa-discovery-zookeeper-server</feature>
    <feature>pentaho-big-data-plugin-shim-impl</feature>
    <bundle>mvn:pentaho/pentaho-big-data-impl-shim-remote/7.1-SNAPSHOT</bundle>
  </feature>
```

The impl-shim-remote bundle creates the bridge between local NamedClusterServiceFactory services and remote service endpoints.

### ClusterTracker
- Calls any available  ClusterInitializerProvider services
- Usually this service is called lazily, we want to initialize on startup, since we don't know beforehand what services we are 

### FactoryTracker
- Listens for NamedClusterServiceFactory services
- Instantiates service with pre-configured NamedCluster
- Marks service as exportable

```groovy
properties.put( "service.exported.interfaces", "*" );
```

Services with this property are picked up the the RSA TopologyManager. It defers to the RemoteServiceAdmin to locate an provider (Fastbin) which creates an EndpointDescription and announces the endpoint with discovery services (Zookeeper).

Client discovery services detect this endpoint and create listeners for service interest. If interest is found, the service is imported via the same provider. 

### ClusterServiceTracker
- Creates an active filter for cluster services by interface name
- Applies a filter to locate the appropriate service by NamedCluster

## Limitations
Most of the service APIs may not be exportable if they rely on non-serializable classes. The Fastbin provider also excels when calls are asynchronous. Ideally APIs can be reworked for simplicity, since all configuration is assumed to be on the agent.
