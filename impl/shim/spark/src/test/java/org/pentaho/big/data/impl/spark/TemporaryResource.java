package org.pentaho.big.data.impl.spark;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class TemporaryResource extends TemporaryFolder {

  File createResourceDir( String name ) throws IOException {
    File classpath = newFolder();
    Files.write( "DATA", new File( classpath, name ), Charsets.UTF_8 );
    return classpath;
  }

  URL[] createClasspath( String resourceName ) throws IOException {
    return new URL[] { createResourceDir( resourceName ).toURI().toURL() };
  }
}