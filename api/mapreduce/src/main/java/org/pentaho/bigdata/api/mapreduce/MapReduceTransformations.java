package org.pentaho.bigdata.api.mapreduce;

import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransMeta;

import java.util.Optional;

/**
 * Created by ccaspanello on 8/29/2016.
 */
public class MapReduceTransformations {

  private MapReduceTransformation combiner;
  private Optional<TransMeta> mapper;
  private Optional<TransMeta> reducer;

  public MapReduceTransformations() {
    this.combiner = Optional.empty();
    this.mapper = Optional.empty();
    this.reducer = Optional.empty();
  }

  public Optional<TransMeta> getCombiner() {
    return combiner;
  }

  public void setCombiner( Optional<TransMeta> combiner ) {
    this.combiner = combiner;
  }

  public Optional<TransMeta> getMapper() {
    return mapper;
  }

  public void setMapper( Optional<TransMeta> mapper ) {
    this.mapper = mapper;
  }

  public Optional<TransMeta> getReducer() {
    return reducer;
  }

  public void setReducer( Optional<TransMeta> reducer ) {
    this.reducer = reducer;
  }

  public void setCombiner( TransConfiguration transMeta, String inputStep, String outputStep ) {
    combiner = new MapReduceTransformation( transMeta, inputStep, outputStep );
  }

}
