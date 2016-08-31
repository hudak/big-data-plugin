/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.impl.spark;

import com.google.common.collect.ImmutableList;
import org.osgi.service.cm.ConfigurationAdmin;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;
import org.pentaho.bigdata.api.spark.SparkService;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

public class SparkServiceFactoryImpl implements NamedClusterServiceFactory<SparkService> {
  private static final Logger LOGGER = LoggerFactory.getLogger( SparkServiceFactoryImpl.class );
  private final boolean isActiveConfiguration;
  private final HadoopConfiguration hadoopConfiguration;
  private final ConfigurationAdmin configAdmin;
  private final ExecutorService executorService;

  public SparkServiceFactoryImpl( boolean isActiveConfiguration, HadoopConfiguration hadoopConfiguration,
                                  ConfigurationAdmin configAdmin, ExecutorService executorService ) {
    this.isActiveConfiguration = isActiveConfiguration;
    this.hadoopConfiguration = hadoopConfiguration;
    this.configAdmin = configAdmin;
    this.executorService = executorService;
  }

  @Override public Class<SparkService> getServiceClass() {
    return SparkService.class;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    return isActiveConfiguration;
  }

  @Override public SparkService create( NamedCluster namedCluster ) {
    return new ServiceImpl( ImmutableList.of(
      new ShimConfigModifier( hadoopConfiguration ),
      new SparkInstallationManager( configAdmin, new ArchiveInstaller(), executorService )
    ) );
  }

  HadoopConfiguration getHadoopConfiguration() {
    return hadoopConfiguration;
  }
}
