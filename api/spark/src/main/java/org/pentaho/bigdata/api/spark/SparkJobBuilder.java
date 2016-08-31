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
package org.pentaho.bigdata.api.spark;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by hudak on 9/8/16.
 */
public interface SparkJobBuilder {
  //TODO Need a mechanism to capture logging?

  SparkJobBuilder setSparkConf( String key, String value );

  SparkJobBuilder setSparkArg( String name, String value );

  SparkJobBuilder setSparkArg( String name );

  SparkJobBuilder setAppResource( String value );

  SparkJobBuilder addAppArgs( List<String> args );

  CompletableFuture<SparkJob> submit();
}