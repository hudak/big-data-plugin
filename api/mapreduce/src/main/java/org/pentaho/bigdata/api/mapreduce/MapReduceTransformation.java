package org.pentaho.bigdata.api.mapreduce;

import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransMeta;

/**
 * Created by hudak on 8/31/16.
 */
public class MapReduceTransformation {
  private final TransConfiguration transConfiguration;
  private final String inputStep;
  private final String outputStep;

  public enum Type {
    MAPPER, COMBINER, REDUCER
  }

  public MapReduceTransformation( TransConfiguration transConfiguration, String inputStep, String outputStep ) {
    this.transConfiguration = transConfiguration;
    this.inputStep = inputStep;
    this.outputStep = outputStep;
  }

  public TransConfiguration getTransConfiguration() {
    return transConfiguration;
  }

  public TransMeta getTransMeta() {
    return transConfiguration.getTransMeta();
  }

  public String getInputStep() {
    return inputStep;
  }

  public String getOutputStep() {
    return outputStep;
  }
}
