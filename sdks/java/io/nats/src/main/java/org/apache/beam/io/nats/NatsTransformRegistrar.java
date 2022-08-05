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
package org.apache.beam.io.nats;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** Exposes {@link DebeziumIO.Read} as an external transform for cross-language usage. */
@Experimental(Experimental.Kind.PORTABILITY)
@AutoService(ExternalTransformRegistrar.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class NatsTransformRegistrar implements ExternalTransformRegistrar {
  public static final String READ_JSON_URN = "beam:transform:io.reflek.gulfstream:nats:v1";

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders() {
    return ImmutableMap.of(
        READ_JSON_URN,
        (Class<? extends ExternalTransformBuilder<?, ?, ?>>) (Class<?>) ReadBuilder.class);
  }

  private abstract static class CrossLanguageConfiguration {
    String serverUri;
    String subject;

    public void setServerUri(String serverUri) {
      this.serverUri = serverUri;
    }

    public void setSubject(String subject) {
      this.subject = subject;
    }
  }

  public static class ReadBuilder
      implements ExternalTransformBuilder<ReadBuilder.Configuration, PBegin, PCollection<byte[]>> {

    public static class Configuration extends CrossLanguageConfiguration {}

    @Override
    public PTransform<PBegin, PCollection<byte[]>> buildExternal(Configuration configuration) {
      NatsIO.ConnectionConfiguration connectorConfiguration =
          NatsIO.ConnectionConfiguration.create(configuration.serverUri, configuration.subject);

      NatsIO.Read readTransform = NatsIO.read().withConnectionConfiguration(connectorConfiguration);

      return readTransform;
    }
  }
}
