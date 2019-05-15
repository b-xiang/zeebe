/*
 * Zeebe Workflow Engine
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.engine.processor;

import io.zeebe.engine.Loggers;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.protocol.impl.record.value.error.ErrorRecord;
import io.zeebe.util.sched.ActorControl;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import org.slf4j.Logger;

@SuppressWarnings({"unchecked"})
public class TypedStreamProcessor implements StreamProcessor {

  private static final Logger LOG = Loggers.WORKFLOW_PROCESSOR_LOGGER;

  protected final RecordProcessorMap recordProcessors;
  protected final List<StreamProcessorLifecycleAware> lifecycleListeners = new ArrayList<>();
  protected final ZeebeState zeebeState;

  private final ErrorRecord errorRecord = new ErrorRecord();
  protected final RecordMetadata metadata = new RecordMetadata();
  protected final EnumMap<ValueType, Class<? extends UnpackedObject>> eventRegistry;
  protected final EnumMap<ValueType, UnpackedObject> eventCache;

  protected final TypedEventImpl typedEvent = new TypedEventImpl();
  private final TypedStreamEnvironment environment;
  private final CommandResponseWriter commandResponseWriter;

  private DelegatingEventProcessor eventProcessorWrapper;
  private ActorControl actor;
  private StreamProcessorContext streamProcessorContext;
  private TypedStreamWriterImpl streamWriter;

  @Override
  public void onOpen(final StreamProcessorContext context) {
    final LogStream logStream = context.getLogStream();
    this.streamWriter = new TypedStreamWriterImpl(logStream);

    this.eventProcessorWrapper =
        new DelegatingEventProcessor(
            context.getId(), commandResponseWriter, logStream, streamWriter, zeebeState);

    this.actor = context.getActorControl();
    this.streamProcessorContext = context;
    lifecycleListeners.forEach(e -> e.onOpen(this));
  }

  @Override
  public void onRecovered() {
    lifecycleListeners.forEach(e -> e.onRecovered(this));
  }

  @Override
  public void onClose() {
    lifecycleListeners.forEach(e -> e.onClose());
  }

  @Override
  public long getPositionToRecoverFrom() {
    return zeebeState.getLastSuccessfuProcessedRecordPosition();
  }

  @Override
  public long getFailedPosition(LoggedEvent currentEvent) {
    metadata.reset();
    currentEvent.readMetadata(metadata);

    if (metadata.getValueType() == ValueType.ERROR) {
      currentEvent.readValue(errorRecord);
      return errorRecord.getErrorEventPosition();
    }
    return -1;
  }

  public ActorControl getActor() {
    return actor;
  }

  public StreamProcessorContext getStreamProcessorContext() {
    return streamProcessorContext;
  }

  public TypedStreamEnvironment getEnvironment() {
    return environment;
  }

  public KeyGenerator getKeyGenerator() {
    return zeebeState.getKeyGenerator();
  }

  public TypedStreamWriterImpl getStreamWriter() {
    return streamWriter;
  }
}
