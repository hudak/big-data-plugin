package org.pentaho.big.data.impl.spark;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.hadoop.shim.HadoopConfiguration;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * Created by hudak on 9/15/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class SparkServiceFactoryImplTest {
  @Mock HadoopConfiguration hadoopConfiguration;
  private SparkServiceFactoryImpl sparkServiceFactory;
  @Mock

  @Before
  public void setUp() throws Exception {
    sparkServiceFactory = new SparkServiceFactoryImpl( true, hadoopConfiguration, configAdmin, executorService );
  }

  @Test
  public void properties() throws Exception {
    assertThat( sparkServiceFactory.getHadoopConfiguration(), is( hadoopConfiguration ) );
  }
}