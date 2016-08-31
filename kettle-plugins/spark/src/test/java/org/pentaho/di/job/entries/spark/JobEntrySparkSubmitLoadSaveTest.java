/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.pentaho.di.job.entries.spark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.entry.loadSave.LoadSaveTester;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.MapLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.StringLoadSaveValidator;

import java.util.concurrent.ExecutorService;

@RunWith( MockitoJUnitRunner.class )
public class JobEntrySparkSubmitLoadSaveTest extends LoadSaveTester<JobEntrySparkSubmit> {

  private static final ImmutableList<String> COMMON_ATTRIBUTES = ImmutableList.of(
    "scriptPath", "master", "jar", "className", "args", "configParams", "driverMemory",
    "executorMemory", "blockExecution", "jobType", "pyFile", "libs" );
  private static final ImmutableMap<String, String> GETTERS = ImmutableMap.of();
  private static final ImmutableMap<String, String> SETTERS = ImmutableMap.of();
  private static final ImmutableMap<String, FieldLoadSaveValidator<?>>
    ATTRIBUTE_VALIDATORS = ImmutableMap.of();
  private static final ImmutableMap<String, FieldLoadSaveValidator<?>>
    TYPE_VALIDATORS = ImmutableMap.of(
    "java.util.Map<java.lang.String,java.lang.String>",
    new MapLoadSaveValidator<>( new StringLoadSaveValidator(), new StringLoadSaveValidator() )
  );

  @Mock NamedClusterServiceLocator serviceLocator;

  public JobEntrySparkSubmitLoadSaveTest() {
    super( JobEntrySparkSubmit.class, JobEntrySparkSubmitLoadSaveTest.COMMON_ATTRIBUTES,
      JobEntrySparkSubmitLoadSaveTest.GETTERS, JobEntrySparkSubmitLoadSaveTest.SETTERS,
      JobEntrySparkSubmitLoadSaveTest.ATTRIBUTE_VALIDATORS, JobEntrySparkSubmitLoadSaveTest.TYPE_VALIDATORS );
  }

  @Test
  public void repoRoundTrip() throws KettleException {
    testRepoRoundTrip();
  }

  @Test
  public void xmlRoundTrip() throws KettleException {
    testXmlRoundTrip();
  }

  @Test
  public void cloneRoundTrip() throws Exception {
    testClone();
  }

  @Override public JobEntrySparkSubmit createMeta() {
    return new JobEntrySparkSubmit( serviceLocator, ( job ) -> Futures.immediateCancelledFuture() );
  }
}
