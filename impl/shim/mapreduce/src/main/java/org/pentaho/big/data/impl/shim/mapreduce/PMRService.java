package org.pentaho.big.data.impl.shim.mapreduce;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.vfs2.FileObject;
import org.pentaho.bigdata.api.mapreduce.PentahoMapReduceOutputStepMetaInterface;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.hadoop.mapreduce.InKeyValueOrdinals;
import org.pentaho.hadoop.mapreduce.OutKeyValueOrdinals;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.fs.FileSystem;
import org.pentaho.hadoop.shim.api.fs.Path;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

/**
 * Created by hudak on 8/31/16.
 */
public class PMRService {
  private static final Class<?> PKG = PMRService.class;
  static final String PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE = "pmr.use.distributed.cache";
  static final String PENTAHO_MAPREDUCE_PROPERTY_KETTLE_HDFS_INSTALL_DIR = "pmr.kettle.dfs.install.dir";
  static final String PENTAHO_MAPREDUCE_PROPERTY_KETTLE_INSTALLATION_ID = "pmr.kettle.installation.id";
  static final String PENTAHO_MAPREDUCE_PROPERTY_ADDITIONAL_PLUGINS = "pmr.kettle.additional.plugins";
  static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_CLEANING_OUTPUT_PATH =
    "JobEntryHadoopTransJobExecutor.CleaningOutputPath";
  static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_FAILED_TO_CLEAN_OUTPUT_PATH =
    "JobEntryHadoopTransJobExecutor.FailedToCleanOutputPath";
  static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_ERROR_CLEANING_OUTPUT_PATH =
    "JobEntryHadoopTransJobExecutor.ErrorCleaningOutputPath";
  static final String MAPREDUCE_APPLICATION_CLASSPATH = "mapreduce.application.classpath";
  static final String DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH =
    "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*,$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*";
  static final String PENTAHO_MAPREDUCE_PROPERTY_PMR_LIBRARIES_ARCHIVE_FILE = "pmr.libraries.archive.file";
  static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_STEP_NOT_SPECIFIED =
    "PentahoMapReduceJobBuilderImpl.InputStepNotSpecified";
  static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_STEP_NOT_FOUND =
    "PentahoMapReduceJobBuilderImpl.InputStepNotFound";
  static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_KEY_ORDINAL =
    "PentahoMapReduceJobBuilderImpl.NoKeyOrdinal";
  static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_VALUE_ORDINAL =
    "PentahoMapReduceJobBuilderImpl.NoValueOrdinal";
  static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_HOP_DISABLED =
    "PentahoMapReduceJobBuilderImpl.InputHopDisabled";
  static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_OUTPUT_STEP_NOT_SPECIFIED =
    "PentahoMapReduceJobBuilderImpl.OutputStepNotSpecified";
  static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_OUTPUT_STEP_NOT_FOUND =
    "PentahoMapReduceJobBuilderImpl.OutputStepNotFound";
  static final String ORG_PENTAHO_BIG_DATA_KETTLE_PLUGINS_MAPREDUCE_STEP_HADOOP_EXIT_META =
    "org.pentaho.big.data.kettle.plugins.mapreduce.step.HadoopExitMeta";
  static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_VALIDATION_ERROR =
    "PentahoMapReduceJobBuilderImpl.ValidationError";
  static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_OUTPUT_KEY_ORDINAL =
    "PentahoMapReduceJobBuilderImpl.NoOutputKeyOrdinal";
  static final String PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_OUTPUT_VALUE_ORDINAL =
    "PentahoMapReduceJobBuilderImpl.NoOutputValueOrdinal";
  static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_KETTLE_HDFS_INSTALL_DIR_MISSING =
    "JobEntryHadoopTransJobExecutor.KettleHdfsInstallDirMissing";
  static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_INSTALLATION_OF_KETTLE_FAILED =
    "JobEntryHadoopTransJobExecutor.InstallationOfKettleFailed";
  static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_CONFIGURING_JOB_WITH_KETTLE_AT =
    "JobEntryHadoopTransJobExecutor.ConfiguringJobWithKettleAt";
  static final String CLASSES = "classes/,";
  static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_UNABLE_TO_LOCATE_ARCHIVE =
    "JobEntryHadoopTransJobExecutor.UnableToLocateArchive";
  static final String JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_KETTLE_INSTALLATION_MISSING_FROM =
    "JobEntryHadoopTransJobExecutor.KettleInstallationMissingFrom";

  private final LogChannelInterface log;
  private final Properties properties;
  private final HadoopShim hadoopShim;
  private final PluginInterface pluginInterface;
  private final FileResolver fileResolver;
  private final ArchiveSupplier archiveSupplier;

  PMRService( LogChannelInterface log, Properties properties, HadoopShim hadoopShim,
              PluginInterface pluginInterface, FileResolver fileResolver ) {
    this.log = log;
    this.properties = properties;
    this.hadoopShim = hadoopShim;
    this.pluginInterface = pluginInterface;
    this.fileResolver = fileResolver;
    this.archiveSupplier = new ArchiveSupplier( pluginInterface, properties );
  }

  /**
   * Gets a property from the configuration. If it is missing it will load it from the properties provided. If it
   * cannot
   * be found there the default value provided will be used.
   *
   * @param conf         Configuration to check for property first.
   * @param propertyName Name of the property to return
   * @param defaultValue Default value to use if no property by the given name could be found in {@code conf} or {@code
   *                     properties}
   * @return Value of {@code propertyName}
   */
  private String getProperty( Configuration conf, String propertyName, String defaultValue ) {
    return Optional.ofNullable( conf.get( propertyName ) )
      .orElseGet( () -> properties.getProperty( propertyName, defaultValue ) );
  }

  public void cleanOutputPath( FileSystem fs, Path path ) throws IOException {
    log.logBasic(
      getString( JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_CLEANING_OUTPUT_PATH, path ) );
    try {
      // If the path does not exist one could think of it as "already cleaned"
      if ( fs.exists( path ) && !fs.delete( path, true ) ) {
        log.logBasic( BaseMessages
          .getString( PKG,
            JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_FAILED_TO_CLEAN_OUTPUT_PATH, path ) );
      }
    } catch ( IOException ex ) {
      throw new IOException(
        getString(
          JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_ERROR_CLEANING_OUTPUT_PATH, path ),
        ex );
    }
  }

  public void stagePentahoLibraries( Configuration conf, String defaultInstallId )
    throws IOException {
    FileSystem fs = hadoopShim.getFileSystem( conf );
    if ( Boolean.parseBoolean(
      getProperty( conf, PENTAHO_MAPREDUCE_PROPERTY_USE_DISTRIBUTED_CACHE,
        Boolean.toString( true ) ) ) ) {
      String installPath =
        getProperty( conf,
          PENTAHO_MAPREDUCE_PROPERTY_KETTLE_HDFS_INSTALL_DIR, null );
      String installId = getProperty( conf,
        PENTAHO_MAPREDUCE_PROPERTY_KETTLE_INSTALLATION_ID,
        defaultInstallId
      );

      try {
        if ( Const.isEmpty( installPath ) ) {
          throw new IllegalArgumentException(
            getString( JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_KETTLE_HDFS_INSTALL_DIR_MISSING ) );
        }
        if ( !installPath.endsWith( Const.FILE_SEPARATOR ) ) {
          installPath += Const.FILE_SEPARATOR;
        }

        Path kettleEnvInstallDir = fs.asPath( installPath, installId );
        String pluginDirPath = pluginInterface.getPluginDirectory().getPath();
        FileObject pluginDirectory = fileResolver.getFileObject( pluginDirPath );
        String pmrLibPath = pluginDirPath + Const.FILE_SEPARATOR + getProperty( conf,
          PENTAHO_MAPREDUCE_PROPERTY_PMR_LIBRARIES_ARCHIVE_FILE, null );
        FileObject pmrLibArchive = fileResolver.getFileObject( pmrLibPath );

        // Make sure the version we're attempting to use is installed
        if ( hadoopShim.getDistributedCacheUtil().isKettleEnvironmentInstalledAt( fs, kettleEnvInstallDir ) ) {
          log.logDetailed( getString(
            "JobEntryHadoopTransJobExecutor.UsingKettleInstallationFrom",
            kettleEnvInstallDir.toUri().getPath() ) );
        } else {
          // Load additional plugin folders as requested
          String additionalPluginNames =
            getProperty( conf,
              PENTAHO_MAPREDUCE_PROPERTY_ADDITIONAL_PLUGINS, null );
          if ( pmrLibArchive == null ) {
            throw new KettleException(
              getString( JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_UNABLE_TO_LOCATE_ARCHIVE, pmrLibPath ) );
          }

          log.logBasic( BaseMessages
            .getString( PKG, "JobEntryHadoopTransJobExecutor.InstallingKettleAt",
              kettleEnvInstallDir ) );

          hadoopShim.getDistributedCacheUtil()
            .installKettleEnvironment( pmrLibArchive, fs, kettleEnvInstallDir, pluginDirectory, additionalPluginNames );

          log.logBasic( BaseMessages
            .getString( PKG,
              "JobEntryHadoopTransJobExecutor.InstallationOfKettleSuccessful", kettleEnvInstallDir ) );
        }
        if ( !hadoopShim.getDistributedCacheUtil().isKettleEnvironmentInstalledAt( fs, kettleEnvInstallDir ) ) {
          throw new KettleException( getString(
            JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_KETTLE_INSTALLATION_MISSING_FROM,
            kettleEnvInstallDir.toUri().getPath() ) );
        }

        log
          .logBasic( getString(
            JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_CONFIGURING_JOB_WITH_KETTLE_AT,
            kettleEnvInstallDir.toUri().getPath() ) );

        String mapreduceClasspath =
          conf.get( MAPREDUCE_APPLICATION_CLASSPATH, DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH );
        conf
          .set( MAPREDUCE_APPLICATION_CLASSPATH,
            CLASSES + mapreduceClasspath );

        hadoopShim.getDistributedCacheUtil().configureWithKettleEnvironment( conf, fs, kettleEnvInstallDir );
        log.logBasic( MAPREDUCE_APPLICATION_CLASSPATH + ": " + conf
          .get( MAPREDUCE_APPLICATION_CLASSPATH ) );
      } catch ( Exception ex ) {
        throw new IOException(
          getString( JOB_ENTRY_HADOOP_TRANS_JOB_EXECUTOR_INSTALLATION_OF_KETTLE_FAILED ), ex );
      }
    }
  }

  public void verifyTransMeta( TransMeta transMeta, String inputStepName,
                               String outputStepName, Function<TransMeta, Trans> transFactory )
    throws KettleException {
    // Verify the input step: see that the key/value fields are present...
    //
    if ( Const.isEmpty( inputStepName ) ) {
      throw new KettleException( getString( PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_STEP_NOT_SPECIFIED ) );
    }
    StepMeta inputStepMeta = transMeta.findStep( inputStepName );
    if ( inputStepMeta == null ) {
      throw new KettleException(
        getString(
          PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_STEP_NOT_FOUND, inputStepName ) );
    }

    // Get the fields coming out of the input step...
    //
    RowMetaInterface injectorRowMeta = transMeta.getStepFields( inputStepMeta );

    // Verify that the key and value fields are found
    //
    InKeyValueOrdinals inOrdinals = new InKeyValueOrdinals( injectorRowMeta );
    if ( inOrdinals.getKeyOrdinal() < 0 ) {
      throw new KettleException(
        getString(
          PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_KEY_ORDINAL, inputStepName ) );
    }
    if ( inOrdinals.getValueOrdinal() < 0 ) {
      throw new KettleException(
        getString(
          PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_VALUE_ORDINAL, inputStepName ) );
    }

    // make sure that the input step is enabled (i.e. its outgoing hop
    // hasn't been disabled)
    Trans t = transFactory.apply( transMeta );
    t.prepareExecution( null );
    if ( t.getStepInterface( inputStepName, 0 ) == null ) {
      throw new KettleException(
        getString(
          PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_INPUT_HOP_DISABLED, inputStepName ) );
    }

    // Now verify the output step output of the reducer...
    //
    if ( Const.isEmpty( outputStepName ) ) {
      throw new KettleException( getString( PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_OUTPUT_STEP_NOT_SPECIFIED ) );
    }

    StepMeta outputStepMeta = transMeta.findStep( outputStepName );
    if ( outputStepMeta == null ) {
      throw new KettleException(
        getString(
          PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_OUTPUT_STEP_NOT_FOUND, outputStepName ) );
    }

    // It's a special step designed to map the output key/value pair fields...
    //
    if ( outputStepMeta.getStepMetaInterface() instanceof PentahoMapReduceOutputStepMetaInterface ) {
      // Get the row fields entering the output step...
      //
      RowMetaInterface outputRowMeta = transMeta.getPrevStepFields( outputStepMeta );
      StepMetaInterface exitMeta = outputStepMeta.getStepMetaInterface();

      List<CheckResultInterface> remarks = new ArrayList<>();
      ( (PentahoMapReduceOutputStepMetaInterface) exitMeta )
        .checkPmr( remarks, transMeta, outputStepMeta, outputRowMeta );
      StringBuilder message = new StringBuilder();
      for ( CheckResultInterface remark : remarks ) {
        if ( remark.getType() == CheckResultInterface.TYPE_RESULT_ERROR ) {
          message.append( message.toString() ).append( Const.CR );
        }
      }
      if ( message.length() > 0 ) {
        throw new KettleException(
          getString( PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_VALIDATION_ERROR ) + Const.CR + message );
      }
    } else {
      // Any other step: verify that the outKey and outValue fields exist...
      //
      RowMetaInterface outputRowMeta = transMeta.getStepFields( outputStepMeta );
      OutKeyValueOrdinals outOrdinals = new OutKeyValueOrdinals( outputRowMeta );
      if ( outOrdinals.getKeyOrdinal() < 0 ) {
        throw new KettleException( getString(
          PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_OUTPUT_KEY_ORDINAL, outputStepName ) );
      }
      if ( outOrdinals.getValueOrdinal() < 0 ) {
        throw new KettleException( getString(
          PENTAHO_MAP_REDUCE_JOB_BUILDER_IMPL_NO_OUTPUT_VALUE_ORDINAL,
          outputStepName ) );
      }
    }
  }

  @VisibleForTesting class ArchiveSupplier {

    private final PluginInterface pluginInterface = PMRService.this.pluginInterface;

    public ArchiveSupplier( PluginInterface pluginInterface, Properties pmrProperties ) {
    }

    public String getVfsFilename( Configuration conf ) {
      return pluginInterface.getPluginDirectory().getPath() + Const.FILE_SEPARATOR
        +
        getProperty( conf,
          PENTAHO_MAPREDUCE_PROPERTY_PMR_LIBRARIES_ARCHIVE_FILE, null );
    }

  }

  public interface FileResolver {

    FileObject getFileObject( String path ) throws KettleException;
  }

  private static String getString( String key, Object... parameters ) {
    return BaseMessages.getString( PKG, key, parameters );
  }
}
