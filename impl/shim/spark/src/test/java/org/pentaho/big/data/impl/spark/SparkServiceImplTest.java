package org.pentaho.big.data.impl.spark;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.bigdata.api.spark.SparkJobBuilder;

import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * Created by hudak on 9/15/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class SparkServiceImplTest {
  private SparkServiceImpl sparkService;

  @Before
  public void setUp() throws Exception {
    sparkService = new SparkServiceImpl( hadoopConfiguration, executorService );
  }

  @Test
  public void createBuilder() throws Exception {
    SparkJobBuilder builder = sparkService.createJobBuilder( UnaryOperator.identity() );
    assertThat( builder, instanceOf( SparkJobBuilderImpl.class ) );
  }
}