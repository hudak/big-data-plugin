package org.pentaho.big.data.impl.spark;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.bigdata.api.spark.SparkEnvironmentModifier;
import org.pentaho.bigdata.api.spark.SparkJobBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by hudak on 9/15/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class ServiceImplTest {
  private ServiceImpl sparkService;
  @Mock private SparkLauncher launcher;
  @Mock private Function<Map<String, String>, SparkLauncher> launcherFactory;

  private List<SparkEnvironmentModifier> modifiers;

  @Before
  public void setUp() throws Exception {
    when( launcherFactory.apply( any() ) ).thenReturn( launcher );
    modifiers = new LinkedList<>();
    sparkService = new ServiceImpl( modifiers );
    sparkService.launcherFactory = launcherFactory;
  }

  private SparkEnvironmentModifier modifier( UnaryOperator<HashMap<String, String>> op ) {
    return env -> CompletableFuture.completedFuture( op.apply( new HashMap<>( env ) ) );
  }

  private SparkEnvironmentModifier modifier( Supplier<Map<String, String>> op ) {
    return modifier( env -> {
      env.putAll( op.get() );
      return env;
    } );
  }

  @Test
  public void createBuilder() throws Exception {
    Map<String, String> expected = new HashMap<>();
    IntStream.range( 0, 10 ).forEach( i -> {
      String key = "MODIFIED " + i;
      String value = String.valueOf( (char) ( 'A' + i ) );

      modifiers.add( modifier( () -> ImmutableMap.of( key, value ) ) );
      expected.put( key, value );
    } );

    SparkJobBuilder jobBuilder = sparkService.createJobBuilder( Collections.emptyMap() );
    assertThat( jobBuilder, instanceOf( SparkJobBuilderImpl.class ) );

    verify( launcherFactory ).apply( expected );
  }

  @Test
  public void rollback() throws Exception {
    modifiers.add( modifier( () -> ImmutableMap.of( "MODIFIED 0", "A" ) ) );
    modifiers.add( ( env ) -> {
      throw new RuntimeException( "bad changes" );
    } );

    SparkJobBuilder builder = sparkService.createJobBuilder( ImmutableMap.of( "VAR1", "the value" ) );
    assertThat( builder, instanceOf( SparkJobBuilderImpl.class ) );

    verify( launcherFactory ).apply( ImmutableMap.of(
      "VAR1", "the value",
      "MODIFIED 0", "A"
    ) );
  }
}