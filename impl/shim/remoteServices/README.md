# Install
1) Rebuild and update big-data-features.
2) Add repositories to org.ops4j.pax.url.mvn.cfg
```
https://repo1.maven.org/maven2/
http://central.maven.org/maven3/
```
3) Add feature repo to org.apache.karaf.features.cfg
```
mvn:org.apache.aries.rsa/rsa-features/1.9.0/xml/features
```
