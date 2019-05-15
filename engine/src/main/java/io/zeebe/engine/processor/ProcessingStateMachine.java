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

import static io.zeebe.engine.processor.TypedEventRegistry.EVENT_REGISTRY;

import io.zeebe.db.DbContext;
import io.zeebe.db.ZeebeDbTransaction;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.WorkflowInstanceRelated;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.protocol.impl.record.value.error.ErrorRecord;
import io.zeebe.protocol.intent.ErrorIntent;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.protocol.intent.WorkflowInstanceRelatedIntent;
import io.zeebe.util.ReflectUtil;
import io.zeebe.util.exception.RecoverableException;
import io.zeebe.util.retry.AbortableRetryStrategy;
import io.zeebe.util.retry.RecoverableRetryStrategy;
import io.zeebe.util.retry.RetryStrategy;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;

/**
 * Represents the processing state machine, which is executed on normal processing.
 *
 * <pre>
 *
 * +-----------------+             +--------------------+
 * |                 |             |                    |      exception
 * | readNextEvent() |------------>|   processEvent()   |------------------+
 * |                 |             |                    |                  v
 * +-----------------+             +--------------------+            +---------------+
 *           ^                             |                         |               |------+
 *           |                             |         +-------------->|   onError()   |      | exception
 *           |                             |         |  exception    |               |<-----+
 *           |                     +-------v-------------+           +---------------+
 *           |                     |                     |                 |
 *           |                     |    writeEvent()     |                 |
 *           |                     |                     |<----------------+
 * +----------------------+        +---------------------+
 * |                      |                 |
 * | executeSideEffects() |                 v
 * |                      |       +----------------------+
 * +----------------------+       |                      |
 *           ^                    |     updateState()    |
 *           +--------------------|                      |
 *                                +----------------------+
 *                                       ^      |
 *                                       |      | exception
 *                                       |      |
 *                                    +---------v----+
 *                                    |              |
 *                                    |   onError()  |
 *                                    |              |
 *                                    +--------------+
 *                                       ^     |
 *                                       |     |  exception
 *                                       +-----+
 *
 * </pre>
 */
public final class ProcessingStateMachine {

  private static final Logger LOG = Loggers.PROCESSOR_LOGGER;

  public static final String ERROR_MESSAGE_WRITE_EVENT_ABORTED =
      "Expected to write one or more follow up events for event '{}' without errors, but exception was thrown.";
  private static final String ERROR_MESSAGE_ROLLBACK_ABORTED =
      "Expected to roll back the current transaction for event '{}' successfully, but exception was thrown.";
  private static final String ERROR_MESSAGE_EXECUTE_SIDE_EFFECT_ABORTED =
      "Expected to execute side effects for event '{}' successfully, but exception was thrown.";
  private static final String ERROR_MESSAGE_UPDATE_STATE_FAILED =
      "Expected to successfully update state for event '{}' with processor '{}', but caught an exception. Retry.";
  private static final String ERROR_MESSAGE_ON_EVENT_FAILED_SKIP_EVENT =
      "Expected to find event processor for event '{}' with processor '{}', but caught an exception. Skip this event.";
  private static final String ERROR_MESSAGE_PROCESSING_FAILED_SKIP_EVENT =
      "Expected to successfully process event '{}' with processor '{}', but caught an exception. Skip this event.";
  private static final String ERROR_MESSAGE_PROCESSING_FAILED_RETRY_PROCESSING =
      "Expected to process event '{}' successfully on stream processor '{}', but caught recoverable exception. Retry processing.";

  private static final String LOG_ERROR_EVENT_COMMITTED =
      "Error event was committed, we continue with processing.";
  private static final String LOG_ERROR_EVENT_WRITTEN =
      "Error record was written at {}, we will continue with processing if event was committed. Current commit position is {}.";

  private static final Duration PROCESSING_RETRY_DELAY = Duration.ofMillis(250);

  public static ProcessingStateMachineBuilder builder() {
    return new ProcessingStateMachineBuilder();
  }

  private final ActorControl actor;
  private final int producerId;
  private final String streamProcessorName;
  private final StreamProcessorMetrics metrics;
  private final RecordProcessorMap recordProcessorMap;
  private final EventFilter eventFilter;
  private final LogStream logStream;
  private final LogStreamReader logStreamReader;
  private final TypedStreamWriter logStreamWriter;

  private final DbContext dbContext;
  private final RetryStrategy writeRetryStrategy;
  private final RetryStrategy sideEffectsRetryStrategy;
  private final RetryStrategy updateStateRetryStrategy;

  private final BooleanSupplier shouldProcessNext;
  private final BooleanSupplier abortCondition;

  protected final ZeebeState zeebeState;

  private final ErrorRecord errorRecord = new ErrorRecord();
  protected final RecordMetadata metadata = new RecordMetadata();
  protected final EnumMap<ValueType, UnpackedObject> eventCache;

  protected final TypedEventImpl typedEvent = new TypedEventImpl();

  private ProcessingStateMachine(
      StreamProcessorContext context,
      StreamProcessorMetrics metrics,
      RecordProcessorMap recordProcessorMap,
      ZeebeState zeebeState,
      DbContext dbContext,
      BooleanSupplier shouldProcessNext,
      BooleanSupplier abortCondition) {
    this.actor = context.getActorControl();
    this.producerId = context.getId();
    this.streamProcessorName = context.getName();
    this.eventFilter = context.getEventFilter();
    this.logStreamReader = context.getLogStreamReader();
    this.logStreamWriter = context.getTypedStreamWriter();
    this.logStream = context.getLogStream();

    this.metrics = metrics;
    this.recordProcessorMap = recordProcessorMap;
    this.zeebeState = zeebeState;
    this.dbContext = dbContext;
    this.writeRetryStrategy = new AbortableRetryStrategy(actor);
    this.sideEffectsRetryStrategy = new AbortableRetryStrategy(actor);
    this.updateStateRetryStrategy = new RecoverableRetryStrategy(actor);
    this.shouldProcessNext = shouldProcessNext;
    this.abortCondition = abortCondition;

    this.eventCache = new EnumMap<>(ValueType.class);
    EVENT_REGISTRY.forEach((t, c) -> eventCache.put(t, ReflectUtil.newInstance(c)));

    this.responseWriter =
        new TypedResponseWriterImpl(context.getCommandResponseWriter(), logStream.getPartitionId());
  }

  // current iteration
  private LoggedEvent currentEvent;
  private TypedRecordProcessor<?> currentProcessor;
  private ZeebeDbTransaction zeebeDbTransaction;

  private long eventPosition = -1L;
  private long lastSuccessfulProcessedEventPosition = -1L;
  private long lastWrittenEventPosition = -1L;

  private boolean onErrorHandling;
  private long errorRecordPosition = -1;

  private void skipRecord() {
    actor.submit(this::readNextEvent);
    metrics.incrementEventsSkippedCount();
  }

  void readNextEvent() {
    if (shouldProcessNext.getAsBoolean()
        && logStreamReader.hasNext()
        && currentProcessor == null
        && logStream.getCommitPosition() >= errorRecordPosition) {

      if (onErrorHandling) {
        LOG.info(LOG_ERROR_EVENT_COMMITTED);
        onErrorHandling = false;
      }

      currentEvent = logStreamReader.next();

      if (eventFilter == null || eventFilter.applies(currentEvent)) {
        processEvent(currentEvent);
      } else {
        skipRecord();
      }
    }
  }

  static final String PROCESSING_ERROR_MESSAGE =
      "Expected to process event '%s' without errors, but exception occurred with message '%s' .";

  final int streamProcessorId;
  protected final TypedStreamWriterImpl writer;
  protected final TypedResponseWriterImpl responseWriter;

  TypedRecordProcessor<?> eventProcessor;
  protected TypedEventImpl event;
  private SideEffectProducer sideEffectProducer;
  private long position;

  private void resetOutput() {
    responseWriter.reset();
    writer.reset();
    this.writer.configureSourceContext(streamProcessorId, position);
  }

  public void setSideEffectProducer(final SideEffectProducer sideEffectProducer) {
    this.sideEffectProducer = sideEffectProducer;
  }

  private void processEvent(final LoggedEvent event) {

    // choose next processor
    try {
      metadata.reset();
      event.readMetadata(metadata);

      currentProcessor =
          recordProcessorMap.get(
              metadata.getRecordType(), metadata.getValueType(), metadata.getIntent().value());
    } catch (final Exception e) {
      LOG.error(ERROR_MESSAGE_ON_EVENT_FAILED_SKIP_EVENT, event, streamProcessorName, e);
      skipRecord();
      return;
    }

    // if no ones want to process it skip this event
    if (currentProcessor == null) {
      skipRecord();
      return;
    }

    // processing
    try {
      final UnpackedObject value = eventCache.get(metadata.getValueType());
      value.reset();
      event.readValue(value);
      typedEvent.wrap(event, metadata, value);

      zeebeDbTransaction = dbContext.getCurrentTransaction();
      zeebeDbTransaction.run(
          () -> {
            // processing
            resetOutput();

            // default side effect is responses; can be changed by processor
            sideEffectProducer = responseWriter;

            final boolean isNotOnBlacklist = !zeebeState.isOnBlacklist(typedEvent);
            if (isNotOnBlacklist) {
              eventProcessor.processRecord(
                  position, typedEvent, responseWriter, writer, this::setSideEffectProducer);
            }

            zeebeState.markAsProcessed(position);
          });
      metrics.incrementEventsProcessedCount();
      writeEvent();
    } catch (final RecoverableException recoverableException) {
      // recoverable
      LOG.error(
          ERROR_MESSAGE_PROCESSING_FAILED_RETRY_PROCESSING,
          event,
          streamProcessorName,
          recoverableException);
      actor.runDelayed(PROCESSING_RETRY_DELAY, () -> processEvent(currentEvent));
    } catch (final Exception e) {
      LOG.error(ERROR_MESSAGE_PROCESSING_FAILED_SKIP_EVENT, event, streamProcessorName, e);
      onError(e, this::writeEvent);
    }
  }

  private void writeRejectionOnCommand(Throwable exception) {
    final String errorMessage =
        String.format(PROCESSING_ERROR_MESSAGE, event, exception.getMessage());
    LOG.error(errorMessage, exception);

    if (event.metadata.getRecordType() == RecordType.COMMAND) {
      sendCommandRejectionOnException(errorMessage);
      writeCommandRejectionOnException(errorMessage);
    }
  }

  private boolean shouldBeBlacklisted(Intent intent) {

    if (isWorkflowInstanceRelated(intent)) {
      final WorkflowInstanceRelatedIntent workflowInstanceRelatedIntent =
          (WorkflowInstanceRelatedIntent) intent;

      return workflowInstanceRelatedIntent.shouldBlacklistInstanceOnError();
    }

    return false;
  }

  private boolean isWorkflowInstanceRelated(Intent intent) {
    return intent instanceof WorkflowInstanceRelatedIntent;
  }

  private void writeCommandRejectionOnException(String errorMessage) {
    writer.appendRejection(event, RejectionType.PROCESSING_ERROR, errorMessage);
  }

  private void sendCommandRejectionOnException(String errorMessage) {
    responseWriter.writeRejectionOnCommand(event, RejectionType.PROCESSING_ERROR, errorMessage);
  }

  private void onError(Throwable processingException, Runnable nextStep) {
    final ActorFuture<Boolean> retryFuture =
        updateStateRetryStrategy.runWithRetry(
            () -> {
              zeebeDbTransaction.rollback();
              return true;
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, throwable) -> {
          if (throwable != null) {
            LOG.error(ERROR_MESSAGE_ROLLBACK_ABORTED, currentEvent, throwable);
          }
          try {
            zeebeDbTransaction = dbContext.getCurrentTransaction();
            zeebeDbTransaction.run(
                () -> {

                  // old on error
                  resetOutput();

                  writeRejectionOnCommand(processingException);
                  errorRecord.initErrorRecord(processingException, event.getPosition());

                  final Intent intent = event.getMetadata().getIntent();
                  if (shouldBeBlacklisted(intent)) {
                    final UnpackedObject value = event.getValue();
                    if (value instanceof WorkflowInstanceRelated) {
                      final long workflowInstanceKey =
                          ((WorkflowInstanceRelated) value).getWorkflowInstanceKey();
                      zeebeState.blacklist(workflowInstanceKey);
                      errorRecord.setWorkflowInstanceKey(workflowInstanceKey);
                    }
                  }

                  writer.appendFollowUpEvent(event.getKey(), ErrorIntent.CREATED, errorRecord);
                });
            onErrorHandling = true;
            nextStep.run();
          } catch (Exception ex) {
            onError(ex, nextStep);
          }
        });
  }

  private void writeEvent() {
    logStreamWriter.producerId(producerId).sourceRecordPosition(currentEvent.getPosition());

    final ActorFuture<Boolean> retryFuture =
        writeRetryStrategy.runWithRetry(
            () -> {
              eventPosition = logStreamWriter.flush();
              return eventPosition >= 0;
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, t) -> {
          if (t != null) {
            LOG.error(ERROR_MESSAGE_WRITE_EVENT_ABORTED, currentEvent, t);
            onError(t, this::writeEvent);
          } else {
            metrics.incrementEventsWrittenCount();
            updateState();
          }
        });
  }

  private void updateState() {
    final ActorFuture<Boolean> retryFuture =
        updateStateRetryStrategy.runWithRetry(
            () -> {
              zeebeDbTransaction.commit();

              // needs to be directly after commit
              // so no other ActorJob can interfere between commit and update the positions
              if (onErrorHandling) {
                errorRecordPosition = eventPosition;
                LOG.info(
                    LOG_ERROR_EVENT_WRITTEN, errorRecordPosition, logStream.getCommitPosition());
              }
              lastSuccessfulProcessedEventPosition = currentEvent.getPosition();
              lastWrittenEventPosition = eventPosition;
              return true;
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, throwable) -> {
          if (throwable != null) {
            LOG.error(
                ERROR_MESSAGE_UPDATE_STATE_FAILED, currentEvent, streamProcessorName, throwable);
            onError(throwable, this::updateState);
          } else {
            executeSideEffects();
          }
        });
  }

  private void executeSideEffects() {
    final ActorFuture<Boolean> retryFuture =
        sideEffectsRetryStrategy.runWithRetry(sideEffectProducer::flush, abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, throwable) -> {
          if (throwable != null) {
            LOG.error(ERROR_MESSAGE_EXECUTE_SIDE_EFFECT_ABORTED, currentEvent, throwable);
          }

          // continue with next event
          currentProcessor = null;
          actor.submit(this::readNextEvent);
        });
  }

  public long getLastSuccessfulProcessedEventPosition() {
    return lastSuccessfulProcessedEventPosition;
  }

  public long getLastWrittenEventPosition() {
    return lastWrittenEventPosition;
  }

  public ActorFuture<Long> getLastWrittenPositionAsync() {
    return actor.call(this::getLastWrittenEventPosition);
  }

  public ActorFuture<Long> getLastProcessedPositionAsync() {
    return actor.call(this::getLastSuccessfulProcessedEventPosition);
  }

  public static class ProcessingStateMachineBuilder {

    private StreamProcessorMetrics metrics;
    private RecordProcessorMap recordProcessorMap;

    private StreamProcessorContext streamProcessorContext;
    private DbContext dbContext;
    private BooleanSupplier shouldProcessNext;
    private BooleanSupplier abortCondition;
    private ZeebeState zeebeState;

    public ProcessingStateMachineBuilder setMetrics(StreamProcessorMetrics metrics) {
      this.metrics = metrics;
      return this;
    }

    public ProcessingStateMachineBuilder setRecordProcessorMap(
        RecordProcessorMap recordProcessorMap) {
      this.recordProcessorMap = recordProcessorMap;
      return this;
    }

    public ProcessingStateMachineBuilder setStreamProcessorContext(StreamProcessorContext context) {
      this.streamProcessorContext = context;
      return this;
    }

    public ProcessingStateMachineBuilder setDbContext(DbContext dbContext) {
      this.dbContext = dbContext;
      return this;
    }

    public ProcessingStateMachineBuilder setShouldProcessNext(BooleanSupplier shouldProcessNext) {
      this.shouldProcessNext = shouldProcessNext;
      return this;
    }

    public ProcessingStateMachineBuilder setAbortCondition(BooleanSupplier abortCondition) {
      this.abortCondition = abortCondition;
      return this;
    }

    public ProcessingStateMachineBuilder setZeebeState(ZeebeState zeebeState) {
      this.zeebeState = zeebeState;
      return this;
    }

    public ProcessingStateMachine build() {
      Objects.requireNonNull(streamProcessorContext);
      Objects.requireNonNull(metrics);
      Objects.requireNonNull(recordProcessorMap);
      Objects.requireNonNull(dbContext);
      Objects.requireNonNull(shouldProcessNext);
      Objects.requireNonNull(abortCondition);
      Objects.requireNonNull(zeebeState);

      return new ProcessingStateMachine(
          streamProcessorContext,
          metrics,
          recordProcessorMap,
          zeebeState,
          dbContext,
          shouldProcessNext,
          abortCondition);
    }
  }
}
