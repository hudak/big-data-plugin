/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.big.data.impl.shim.mapreduce;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.thoughtworks.xstream.XStream;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.mapreduce.MapReduceJobAdvanced;
import org.pentaho.bigdata.api.mapreduce.MapReduceTransformation;
import org.pentaho.bigdata.api.mapreduce.MapReduceTransformations;
import org.pentaho.bigdata.api.mapreduce.PentahoMapReduceJobBuilder;
import org.pentaho.bigdata.api.mapreduce.TransformationVisitorService;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.version.BuildVersion;
import org.pentaho.hadoop.PluginPropertiesUtil;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.fs.FileSystem;
import org.pentaho.hadoop.shim.api.fs.Path;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Created by bryan on 1/8/16.
 */
public class PentahoMapReduceJobBuilderImpl extends MapReduceJobBuilderImpl implements PentahoMapReduceJobBuilder {
  public static final Class<?> PKG = PentahoMapReduceJobBuilderImpl.class;
  public static final String MAPREDUCE_APPLICATION_CLASSPATH = "mapreduce.application.classpath";
  public static final String DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH =
    "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*,$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*";
  public static final String PENTAHO_MAPREDUCE_PROPERTY_PMR_LIBRARIES_ARCHIVE_FILE = "pmr.libraries.archive.file";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_STEP_NOT_SPECIFIED =
    "PentahoMapReduceJobBuilderImpl.InputStepNotSpecified";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_STEP_NOT_FOUND =
    "PentahoMapReduceJobBuilderImpl.InputStepNotFound";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_KEY_ORDINAL =
    "PentahoMapReduceJobBuilderImpl.NoKeyOrdinal";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_VALUE_ORDINAL =
    "PentahoMapReduceJobBuilderImpl.NoValueOrdinal";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_HOP_DISABLED =
    "PentahoMapReduceJobBuilderImpl.InputHopDisabled";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_OUTPUT_STEP_NOT_SPECIFIED =
    "PentahoMapReduceJobBuilderImpl.OutputStepNotSpecified";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_OUTPUT_STEP_NOT_FOUND =
    "PentahoMapReduceJobBuilderImpl.OutputStepNotFound";
  public static final String ORG_PENTAHO_BIG_DATA_KETTLE_PLUGINS_MAPREDUCE_STEP_HADOOP_EXIT_META =
    "org.pentaho.big.data.kettle.plugins.mapreduce.step.HadoopExitMeta";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_VALIDATION_ERROR =
    "PentahoMapReduceJobBuilderImpl.ValidationError";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_OUTPUT_KEY_ORDINAL =
    "PentahoMapReduceJobBuilderImpl.NoOutputKeyOrdinal";
  public static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_OUTPUT_VALUE_ORDINAL =
    "PentahoMapReduceJobBuilderImpl.NoOutputValueOrdinal";
  public static final String TRANSFORMATION_MAP_XML = "transformation-map-xml";
  public static final String TRANSFORMATION_MAP_INPUT_STEPNAME = "transformation-map-input-stepname";
  public static final String TRANSFORMATION_MAP_OUTPUT_STEPNAME = "transformation-map-output-stepname";
  public static final String LOG_LEVEL = "logLevel";
  public static final String TRANSFORMATION_COMBINER_XML = "transformation-combiner-xml";
  public static final String TRANSFORMATION_COMBINER_INPUT_STEPNAME = "transformation-combiner-input-stepname";
  public static final String TRANSFORMATION_COMBINER_OUTPUT_STEPNAME = "transformation-combiner-output-stepname";
  public static final String TRANSFORMATION_REDUCE_XML = "transformation-reduce-xml";
  public static final String TRANSFORMATION_REDUCE_INPUT_STEPNAME = "transformation-reduce-input-stepname";
  public static final String TRANSFORMATION_REDUCE_OUTPUT_STEPNAME = "transformation-reduce-output-stepname";
  public static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_KETTLE_HDFS_INSTALL_DIR_MISSING =
    "JobEntryHadoopTransJobExecutor.KettleHdfsInstallDirMissing";
  public static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_INSTALLATION_OF_KETTLE_FAILED =
    "JobEntryHadoopTransJobExecutor.InstallationOfKettleFailed";
  public static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_CONFIGURING_JOB_WITH_KETTLE_AT =
    "JobEntryHadoopTransJobExecutor.ConfiguringJobWithKettleAt";
  public static final String CLASSES = "classes/,";
  public static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_UNABLE_TO_LOCATE_ARCHIVE =
    "JobEntryHadoopTransJobExecutor.UnableToLocateArchive";
  public static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_KETTLE_INSTALLATION_MISSING_FROM =
    "JobEntryHadoopTransJobExecutor.KettleInstallationMissingFrom";
  public static final String VARIABLE_SPACE = "variableSpace";
  private final LogChannelInterface log;
  private final List<TransformationVisitorService> visitorServices;
  private final PMRService pmrService;
  private final String installId;
  private boolean cleanOutputPath;
  private LogLevel logLevel;
  private final MapReduceTransformations transformations = new MapReduceTransformations();

  @Deprecated
  public PentahoMapReduceJobBuilderImpl( NamedCluster namedCluster,
                                         HadoopConfiguration hadoopConfiguration,
                                         LogChannelInterface log,
                                         VariableSpace variableSpace, PluginInterface pluginInterface,
                                         Properties pmrProperties,
                                         List<TransformationVisitorService> visitorServices )
    throws KettleFileException {
    this( namedCluster, hadoopConfiguration, log, variableSpace, visitorServices,
      new PMRService( log, pmrProperties, hadoopConfiguration.getHadoopShim(), pluginInterface,
        KettleVFS::getFileObject ) );
  }


  PentahoMapReduceJobBuilderImpl( NamedCluster namedCluster, HadoopConfiguration hadoopConfiguration,
                                  LogChannelInterface log,
                                  VariableSpace variableSpace,
                                  List<TransformationVisitorService> visitorServices, PMRService pmrService ) {
    super( namedCluster, hadoopConfiguration.getHadoopShim(), log, variableSpace );
    this.log = log;
    this.installId = buildInstallIdBase( hadoopConfiguration );
    this.visitorServices = visitorServices;
    this.pmrService = pmrService;
  }

  private static String buildInstallIdBase( HadoopConfiguration hadoopConfiguration ) {
    String pluginVersion = new PluginPropertiesUtil().getVersion();

    String installId = BuildVersion.getInstance().getVersion();
    if ( pluginVersion != null ) {
      installId = installId + "-" + pluginVersion;
    }

    return installId + "-" + hadoopConfiguration.getIdentifier();
  }

  @Override
  public String getHadoopWritableCompatibleClassName( ValueMetaInterface valueMetaInterface ) {
    Class<?> hadoopWritableCompatibleClass = getHadoopShim().getHadoopWritableCompatibleClass( valueMetaInterface );
    if ( hadoopWritableCompatibleClass == null ) {
      return null;
    }
    return hadoopWritableCompatibleClass.getCanonicalName();
  }

  @Override
  public void setLogLevel( LogLevel logLevel ) {
    this.logLevel = logLevel;
  }

  @Override
  public void setCleanOutputPath( boolean cleanOutputPath ) {
    this.cleanOutputPath = cleanOutputPath;
  }

  @Override
  public void verifyTransMeta( TransMeta transMeta, String inputStepName, String outputStepName )
    throws KettleException {
    pmrService.verifyTransMeta( transMeta, inputStepName, outputStepName, Trans::new );
  }

  @Override
  public void setCombinerInfo( String combinerTransformationXml, String combinerInputStep, String combinerOutputStep ) {
    setTransInfo( MapReduceTransformation.Type.COMBINER,
      combinerTransformationXml, combinerInputStep, combinerOutputStep );
  }

  @Override
  public void setReducerInfo( String reducerTransformationXml, String reducerInputStep, String reducerOutputStep ) {
    setTransInfo( MapReduceTransformation.Type.REDUCER, reducerTransformationXml, reducerInputStep, reducerOutputStep );
  }

  @Override
  public void setMapperInfo( String mapperTransformationXml, String mapperInputStep, String mapperOutputStep ) {
    setTransInfo( MapReduceTransformation.Type.MAPPER, mapperTransformationXml, mapperInputStep, mapperOutputStep );
  }

  private MapReduceTransformation setTransInfo( MapReduceTransformation.Type type, String transConfigXml,
                                                String inputStep, String outputStep ) {
    MapReduceTransformation result;
    try {
      result = new MapReduceTransformation( type, TransConfiguration.fromXML( transConfigXml ), inputStep, outputStep );
    } catch ( KettleException e ) {
      throw new IllegalArgumentException( "Unable to parse Pentaho Map Reduce transformation config", e );
    }
    return transformations.put( type, result );
  }

  @Override
  protected void configure( Configuration conf ) throws Exception {
    callVisitors();

    setMapRunnerClass( getHadoopShim().getPentahoMapReduceMapRunnerClass().getCanonicalName() );


    Preconditions.checkState( transformations.containsKey( MapReduceTransformation.Type.MAPPER ),
      "mapper transformation is not set" );

    configureTransformation( conf, MapReduceTransformation.Type.MAPPER,
      TRANSFORMATION_MAP_XML,
      TRANSFORMATION_MAP_INPUT_STEPNAME,
      TRANSFORMATION_MAP_OUTPUT_STEPNAME
    );

    configureTransformation( conf, MapReduceTransformation.Type.COMBINER,
      TRANSFORMATION_COMBINER_XML,
      TRANSFORMATION_COMBINER_INPUT_STEPNAME,
      TRANSFORMATION_COMBINER_OUTPUT_STEPNAME
    );

    configureTransformation( conf, MapReduceTransformation.Type.REDUCER,
      TRANSFORMATION_REDUCE_XML,
      TRANSFORMATION_REDUCE_INPUT_STEPNAME,
      TRANSFORMATION_REDUCE_OUTPUT_STEPNAME
    );

    conf.setJarByClass( getHadoopShim().getPentahoMapReduceMapRunnerClass() );
    conf.set( LOG_LEVEL, logLevel.toString() );
    configureVariableSpace( conf );
    super.configure( conf );
  }

  @Override
  protected MapReduceJobAdvanced submit( Configuration conf ) throws IOException {

    if ( cleanOutputPath() ) {
      FileSystem fileSystem = getFileSystem( conf );
      Path path = getOutputPath( conf, fileSystem );
      pmrService.cleanOutputPath( fileSystem, path );
    }

    pmrService.stagePentahoLibraries( conf, getInstallId() );
    return super.submit( conf );
  }

  private void configureTransformation( Configuration conf, MapReduceTransformation.Type type, String xmlProperty,
                                        String inputStepProperty, String outputStepProperty ) throws Exception {
    if ( transformations.containsKey( type ) ) {
      MapReduceTransformation transformation = transformations.get( type );
      conf.set( xmlProperty, transformation.getTransConfiguration().getXML() );
      conf.set( inputStepProperty, transformation.getInputStep() );
      conf.set( outputStepProperty, transformation.getOutputStep() );
    }
  }

  protected void configureVariableSpace( Configuration conf ) {
    // get a reference to the variable space
    XStream xStream = new XStream();

    // this is optional - for human-readable xml file
    xStream.alias( VARIABLE_SPACE, VariableSpace.class );

    // serialize the variable space to XML
    String xmlVariableSpace = xStream.toXML( getVariableSpace() );

    // set a string in the job configuration as the serialized variablespace
    conf.setStrings( VARIABLE_SPACE, xmlVariableSpace );
  }

  private void callVisitors() {
    for ( TransformationVisitorService visitorService : visitorServices ) {
      visitorService.visit( transformations );
    }
  }

  String getInstallId() {
    return installId;
  }

  protected boolean cleanOutputPath() {
    return cleanOutputPath;
  }

}
