/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.expansion;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * A registrar which contains a mapping from URNs to available {@link ExternalTransformBuilder}s.
 * Should be used with {@link com.google.auto.service.AutoService}.
 */
@Experimental(Kind.PORTABILITY)
public interface ExternalTransformRegistrar {

  /** A mapping from URN to an {@link ExternalTransformBuilder} class. */
  Map<String, Class<? extends ExternalTransformBuilder>> knownBuilders();

  /** A mapping from URN to an {@link ExternalTransformBuilder} instance. */
  default Map<String, ExternalTransformBuilder> knownBuilderInstances() {
    ImmutableMap.Builder builder = ImmutableMap.<String, ExternalTransformBuilder>builder();
    Map<String, Class<? extends ExternalTransformBuilder>> knownBuilders = knownBuilders();
    for (String urn : knownBuilders.keySet()) {
      Preconditions.checkState(
              ExternalTransformBuilder.class.isAssignableFrom(knownBuilders.get(urn)),
              "Provided identifier %s is not an ExternalTransformBuilder.",
              knownBuilders.get(urn).getName());
      try {
        Class<? extends ExternalTransformBuilder> builderClass = knownBuilders.get(urn);
        Constructor<? extends ExternalTransformBuilder> constructor = builderClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        ExternalTransformBuilder reg = constructor.newInstance();
        builder.put(urn,reg);
      }
      catch (Exception e) {
        throw new RuntimeException(e.getMessage());
      }
    }
    return builder.build();
  }
}
