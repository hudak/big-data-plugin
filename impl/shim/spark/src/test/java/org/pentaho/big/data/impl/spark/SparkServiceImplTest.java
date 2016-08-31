package org.pentaho.big.data.impl.spark;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.bigdata.api.spark.SparkJobBuilder;
import org.pentaho.hadoop.shim.HadoopConfiguration;

import java.io.File;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * Created by hudak on 9/15/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class SparkServiceImplTest {
  private SparkServiceImpl sparkService;
  @Mock HadoopConfiguration hadoopConfiguration;
  @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File sparkHome;

  @Before
  public void setUp() throws Exception {
    sparkService = new SparkServiceImpl( hadoopConfiguration );
    sparkHome = temporaryFolder.newFolder( "spark" );
  }

  @Test
  public void createBuilder() throws Exception {
    SparkJobBuilder builder = sparkService.createJobBuilder( ImmutableMap.of( "VAR1", "the value" ) );
    assertThat( builder, instanceOf( SparkJobBuilderImpl.class ) );
  }
}