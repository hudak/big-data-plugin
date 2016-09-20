package org.pentaho.big.data.impl.spark.shim;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.spark.deploy.SparkSubmit$;
import org.apache.spark.deploy.SparkSubmitArguments;
import org.pentaho.big.data.impl.spark.SubmitService;
import org.pentaho.bigdata.api.spark.SparkJob;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import scala.Tuple2;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;
import scala.collection.mutable.Builder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;

/**
 * Created by hudak on 9/19/16.
 */
public class NativeSubmitService implements SubmitService {
  private final ExecutorService executorService;
  private final LogChannelInterface log;

  public NativeSubmitService( ExecutorService executorService, LogChannelInterface log ) {
    this.executorService = executorService;
    this.log = log;
  }

  @Override public SparkJob submit( java.util.List<String> args, java.util.Map<String, String> env )
    throws KettleException {

    // Overlay env on sysEnv and set HADOOP_CONF_DIR to shim dir
    ImmutableMap<String, String> sysEnv = ImmutableMap.<String, String>builder()
      .putAll( System.getenv() )
      .putAll( env ).build();

    SparkSubmitArguments sparkSubmitArguments = parseArguments( args, sysEnv );

    // TODO inject logger as print stream
    SparkSubmit$.MODULE$.printStream_$eq( System.err );
    Method submit;
    try {
      submit = SparkSubmit$.class.getDeclaredMethod( "submit", SparkSubmitArguments.class );
      submit.setAccessible( true );
      submit.invoke( SparkSubmit$.MODULE$, sparkSubmitArguments );
    } catch ( InvocationTargetException e ) {
      throw new KettleException( e.getCause() );
    } catch ( NoSuchMethodException | IllegalAccessException e ) {
      throw new KettleException( e );
    }
    throw new RuntimeException( "Didn't finish implementation of this method" );
  }

  private SparkSubmitArguments parseArguments( java.util.List<String> args, java.util.Map<String, String> env ) {
    List<String> scalaArgs = args.stream()
      .collect( List$.MODULE$::<String>newBuilder, Builder::$plus$eq,
        ( left, right ) -> left.$plus$plus$eq( right.result() ) )
      .result();

    Map<String, String> scalaEnv = env.entrySet().stream()
      .map( entry -> new Tuple2<>( entry.getKey(), entry.getValue() ) )
      .reduce( Map$.MODULE$.empty(), Map::$plus, Map::$plus$plus );

    return new SparkSubmitArguments( scalaArgs, scalaEnv );
  }
}
