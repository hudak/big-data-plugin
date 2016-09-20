package org.pentaho.big.data.impl.spark;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.pentaho.bigdata.api.spark.SparkJob;
import org.pentaho.bigdata.api.spark.SparkJobBuilder;
import org.pentaho.di.core.exception.KettleException;

/**
 * Created by hudak on 9/15/16.
 */
public class SparkJobBuilderImpl implements SparkJobBuilder {
  private final SubmitService submitService;
  private List<String> arguments = Lists.newLinkedList();
  private Map<String, String> environment = Maps.newHashMap();

  public SparkJobBuilderImpl( SubmitService submitService ) {
    this.submitService = submitService;
  }

  @Override public SparkJobBuilder addArguments( List<String> cmds ) {
    arguments.addAll( cmds );
    return this;
  }

  @Override public SparkJobBuilder addEnvironmentVariables( Map<String, String> env ) {
    environment.putAll( env );
    return this;
  }

  @Override public SparkJob submit() throws KettleException {
    return submitService.submit( ImmutableList.copyOf( arguments ), ImmutableMap.copyOf( environment ) );
  }
}
