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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Subscription;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unbounded source for MQTT broker.
 *
 * <h3>Reading from a MQTT broker</h3>
 *
 * <p>MqttIO source returns an unbounded {@link PCollection} containing MQTT message payloads (as
 * {@code byte[]}).
 *
 * <p>To configure a MQTT source, you have to provide a MQTT connection configuration including
 * {@code ClientId}, a {@code ServerURI}, a {@code Topic} pattern, and optionally {@code username}
 * and {@code password} to connect to the MQTT broker. The following example illustrates various
 * options for configuring the source:
 *
 * <pre>{@code
 * pipeline.apply(
 *   MqttIO.read()
 *    .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create(
 *      "tcp://host:11883",
 *      "my_topic"))
 *
 * }</pre>
 *
 * <h3>Writing to a MQTT broker</h3>
 *
 * <p>MqttIO sink supports writing {@code byte[]} to a topic on a MQTT broker.
 *
 * <p>To configure a MQTT sink, as for the read, you have to specify a MQTT connection configuration
 * with {@code ServerURI}, {@code Topic}, ...
 *
 * <p>The MqttIO only fully supports QoS 1 (at least once). It's the only QoS level guaranteed due
 * to potential retries on bundles.
 *
 * <p>For instance:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...) // provide PCollection<byte[]>
 *   .MqttIO.write()
 *     .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create(
 *       "tcp://host:11883",
 *       "my_topic"))
 *
 * }</pre>
 */
@Experimental(Kind.SOURCE_SINK)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class NatsIO {

  private static final Logger LOG = LoggerFactory.getLogger(NatsIO.class);

  public static Read read() {
    System.out.print("DEBUG");
    return new AutoValue_NatsIO_Read.Builder().build();
  }

  private NatsIO() {}

  /** A POJO describing a MQTT connection. */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    abstract String getServerUri();

    abstract String getSubject();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setServerUri(String serverUri);

      abstract Builder setSubject(String subject);

      abstract ConnectionConfiguration build();
    }

    /**
     * Describe a connection configuration to the MQTT broker. This method creates a unique random
     * MQTT client ID.
     *
     * @param serverUri The MQTT broker URI.
     * @param subject The MQTT getSubject pattern.
     * @return A connection configuration to the MQTT broker.
     */
    public static ConnectionConfiguration create(String serverUri, String subject) {
      checkArgument(serverUri != null, "serverUri can not be null");
      checkArgument(subject != null, "subject can not be null");
      return new AutoValue_NatsIO_ConnectionConfiguration.Builder()
          .setServerUri(serverUri)
          .setSubject(subject)
          .build();
    }

    /** Set up the MQTT broker URI. */
    public ConnectionConfiguration withServerUri(String serverUri) {
      checkArgument(serverUri != null, "serverUri can not be null");
      return builder().setServerUri(serverUri).build();
    }

    /** Set up the MQTT getSubject pattern. */
    public ConnectionConfiguration withSubject(String subject) {
      checkArgument(subject != null, "subject can not be null");
      return builder().setSubject(subject).build();
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("serverUri", getServerUri()));
      builder.add(DisplayData.item("subject", getSubject()));
    }

    private Connection createConnection() throws Exception {
      LOG.debug("Creating MQTT client to {}", getServerUri());
      Connection connection = Nats.connect(getServerUri());
      return connection;
    }
  }

  /** A {@link PTransform} to read from a MQTT broker. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<byte[]>> {

    abstract @Nullable ConnectionConfiguration connectionConfiguration();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration config);

      abstract Read build();
    }

    /** Define the MQTT connection configuration used to connect to the MQTT broker. */
    public Read withConnectionConfiguration(ConnectionConfiguration configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return builder().setConnectionConfiguration(configuration).build();
    }

    @Override
    public PCollection<byte[]> expand(PBegin input) {
      org.apache.beam.sdk.io.Read.Unbounded<byte[]> unbounded =
          org.apache.beam.sdk.io.Read.from(new UnboundedMqttSource(this));

      PTransform<PBegin, PCollection<byte[]>> transform = unbounded;

      return input.getPipeline().apply(transform);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      connectionConfiguration().populateDisplayData(builder);
    }
  }

  /**
   * Checkpoint for an unbounded MQTT source. Consists of the MQTT messages waiting to be
   * acknowledged and oldest pending message timestamp.
   */
  @VisibleForTesting
  static class NatsCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

    @VisibleForTesting String clientId;
    @VisibleForTesting Instant oldestMessageTimestamp = Instant.now();
    @VisibleForTesting transient List<Message> messages = new ArrayList<>();

    public NatsCheckpointMark() {}

    public NatsCheckpointMark(String id) {
      clientId = id;
    }

    public void add(Message message, Instant timestamp) {
      if (timestamp.isBefore(oldestMessageTimestamp)) {
        oldestMessageTimestamp = timestamp;
      }
      messages.add(message);
    }

    @Override
    public void finalizeCheckpoint() {
      LOG.debug("Finalizing checkpoint acknowledging pending messages for client ID {}", clientId);
      for (Message message : messages) {
        try {
          message.ack();
        } catch (Exception e) {
          LOG.warn("Can't ack message for client ID {}", clientId, e);
        }
      }
      oldestMessageTimestamp = Instant.now();
      messages.clear();
    }

    // set an empty list to messages when deserialize
    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
      stream.defaultReadObject();
      messages = new ArrayList<>();
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other instanceof NatsCheckpointMark) {
        NatsCheckpointMark that = (NatsCheckpointMark) other;
        return Objects.equals(this.clientId, that.clientId)
            && Objects.equals(this.oldestMessageTimestamp, that.oldestMessageTimestamp)
            && Objects.deepEquals(this.messages, that.messages);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(clientId, oldestMessageTimestamp, messages);
    }
  }

  @VisibleForTesting
  static class UnboundedMqttSource extends UnboundedSource<byte[], NatsCheckpointMark> {

    private final Read spec;

    public UnboundedMqttSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public UnboundedReader<byte[]> createReader(
        PipelineOptions options, NatsCheckpointMark checkpointMark) {
      return new UnboundedMqttReader(this, checkpointMark);
    }

    @Override
    public List<UnboundedMqttSource> split(int desiredNumSplits, PipelineOptions options) {
      // Mqtt is based on a pub/sub pattern
      // so, if we create several subscribers on the same topic, they all will receive the same
      // message, resulting to duplicate messages in the PCollection.
      // So, for MQTT, we limit to number of split ot 1 (unique source).
      return Collections.singletonList(new UnboundedMqttSource(spec));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
    }

    @Override
    public Coder<NatsCheckpointMark> getCheckpointMarkCoder() {
      return SerializableCoder.of(NatsCheckpointMark.class);
    }

    @Override
    public Coder<byte[]> getOutputCoder() {
      return ByteArrayCoder.of();
    }
  }

  @VisibleForTesting
  static class UnboundedMqttReader extends UnboundedSource.UnboundedReader<byte[]> {

    private final UnboundedMqttSource source;

    private Connection nc;
    // private JetStreamSubscription sub;
    private Subscription sub;
    private byte[] current;
    private Instant currentTimestamp;
    private NatsCheckpointMark checkpointMark;

    public UnboundedMqttReader(UnboundedMqttSource source, NatsCheckpointMark checkpointMark) {
      this.source = source;
      this.current = null;
      if (checkpointMark != null) {
        this.checkpointMark = checkpointMark;
      } else {
        this.checkpointMark = new NatsCheckpointMark();
      }
    }

    @Override
    public boolean start() throws IOException {
      LOG.debug("Starting MQTT reader ...");
      Read spec = source.spec;
      try {
        nc = spec.connectionConfiguration().createConnection();
        // JetStream js = nc.jetStream();
        // PushSubscribeOptions so =
        //     PushSubscribeOptions.builder()
        //         // .durable(spec.connectionConfiguration().getDurableName())
        //         .build();
        // sub = nc.subscribe(spec.connectionConfiguration().getSubject(), so);
        sub = nc.subscribe(spec.connectionConfiguration().getSubject());
        nc.flush(Duration.ofSeconds(5));
        return advance();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public boolean advance() throws IOException {
      try {
        // LOG.trace("MQTT reader (client ID {}) waiting message ...", client.getClientId());
        Message msg = sub.nextMessage(Duration.ofSeconds(1));
        if (msg == null) {
          return false;
        }
        current = msg.getData();
        currentTimestamp = Instant.now();
        checkpointMark.add(msg, currentTimestamp);
      } catch (Exception e) {
        throw new IOException(e);
      }
      return true;
    }

    @Override
    public void close() throws IOException {
      LOG.debug("Closing MQTT reader (client ID {})");
      try {
        if (nc != null) {
          // todo drain and close?
          sub.unsubscribe();
          nc.close();
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public Instant getWatermark() {
      return checkpointMark.oldestMessageTimestamp;
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
      return checkpointMark;
    }

    @Override
    public byte[] getCurrent() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public Instant getCurrentTimestamp() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return currentTimestamp;
    }

    @Override
    public UnboundedMqttSource getCurrentSource() {
      return source;
    }
  }
}
