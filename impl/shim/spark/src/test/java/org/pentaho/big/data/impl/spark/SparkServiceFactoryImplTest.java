package org.pentaho.big.data.impl.spark;

import org.apache.commons.vfs2.VFS;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.wiring.BundleWiring;
import org.pentaho.hadoop.shim.HadoopConfiguration;

import java.net.URLClassLoader;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/**
 * Created by hudak on 9/15/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class SparkServiceFactoryImplTest {
  @Mock HadoopConfiguration hadoopConfiguration;
  private SparkServiceFactoryImpl sparkServiceFactory;

  @Before
  public void setUp() throws Exception {
    sparkServiceFactory = new SparkServiceFactoryImpl( true, hadoopConfiguration );
  }

  @Test
  public void properties() throws Exception {
    assertThat( sparkServiceFactory.getHadoopConfiguration(), is( hadoopConfiguration ) );
  }

  //      assertThat( "Unable to find " + resource, url, notNullValue() );
  //      assertThat( Resources.toString( url, Charsets.UTF_8 ), is( "DATA" ) );
}