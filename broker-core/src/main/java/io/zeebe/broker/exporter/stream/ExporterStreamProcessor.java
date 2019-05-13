/*
 * Zeebe Broker Core
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
package io.zeebe.broker.exporter.stream;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.exporter.ExporterObjectMapper;
import io.zeebe.broker.exporter.context.ExporterContext;
import io.zeebe.broker.exporter.record.RecordMetadataImpl;
import io.zeebe.broker.exporter.repo.ExporterDescriptor;
import io.zeebe.broker.exporter.stream.ExporterRecord.ExporterPosition;
import io.zeebe.db.DbContext;
import io.zeebe.db.ZeebeDb;
import io.zeebe.exporter.api.context.Context;
import io.zeebe.exporter.api.context.Controller;
import io.zeebe.exporter.api.record.Record;
import io.zeebe.exporter.api.spi.Exporter;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.processor.EventProcessor;
import io.zeebe.logstreams.processor.StreamProcessor;
import io.zeebe.logstreams.processor.StreamProcessorContext;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.ActorControl;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExporterStreamProcessor implements StreamProcessor {

  private static final Logger LOG = Loggers.EXPORTER_LOGGER;

  private final RecordMetadata rawMetadata = new RecordMetadata();

  private final List<ExporterContainer> containers;
  private final int partitionId;

  private final ExporterStreamProcessorState state;
  private final RecordExporter recordExporter = new RecordExporter();
  private final ExporterRecordProcessor exporterRecordProcessor = new ExporterRecordProcessor();

  private ActorControl actorControl;

  public ExporterStreamProcessor(
      ZeebeDb<ExporterColumnFamilies> zeebeDb,
      DbContext dbContext,
      final int partitionId,
      final Collection<ExporterDescriptor> descriptors) {
    state = new ExporterStreamProcessorState(zeebeDb, dbContext);

    this.partitionId = partitionId;

    this.containers = new ArrayList<>(descriptors.size());
    for (final ExporterDescriptor descriptor : descriptors) {
      this.containers.add(new ExporterContainer(descriptor));
    }
  }

  public ExporterStreamProcessorState getState() {
    return state;
  }

  @Override
  public EventProcessor onEvent(LoggedEvent event) {
    final EventProcessor processor;
    event.readMetadata(rawMetadata);

    if (rawMetadata.getValueType() == ValueType.EXPORTER) {
      exporterRecordProcessor.wrap(event);
      processor = exporterRecordProcessor;
    } else {
      recordExporter.wrap(event);
      processor = recordExporter;
    }

    return processor;
  }

  @Override
  public void onOpen(StreamProcessorContext context) {
    actorControl = context.getActorControl();

    for (final ExporterContainer container : containers) {
      container.exporter.configure(container.context);
    }

    final List<Context.RecordFilter> recordFilters =
        containers.stream().map(c -> c.context.getFilter()).collect(Collectors.toList());

    final Map<RecordType, Boolean> acceptRecordType =
        Arrays.stream(RecordType.values())
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    type -> recordFilters.stream().anyMatch(f -> f.acceptType(type))));

    final Map<ValueType, Boolean> acceptValueType =
        Arrays.stream(ValueType.values())
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    type -> recordFilters.stream().anyMatch(f -> f.acceptValue(type))));

    LOG.info(
        "Setting event filter for exporters which accepts records of type {} and value {}",
        acceptRecordType,
        acceptValueType);

    final RecordMetadata metadata = new RecordMetadata();
    context.setEventFilter(
        event -> {
          event.readMetadata(metadata);

          final RecordType recordType = metadata.getRecordType();
          final ValueType valueType = metadata.getValueType();

          return acceptRecordType.get(recordType) && acceptValueType.get(valueType);
        });
  }

  @Override
  public long getPositionToRecoverFrom() {
    return state.getLowestPosition();
  }

  @Override
  public void onRecovered() {
    for (final ExporterContainer container : containers) {
      container.position = state.getPosition(container.getId());
      if (container.position == ExporterStreamProcessorState.VALUE_NOT_FOUND) {
        state.setPosition(container.getId(), -1L);
      }
      container.exporter.open(container);
    }

    clearExporterState();
  }

  private void clearExporterState() {
    final List<String> exporterIds =
        containers.stream().map(ExporterContainer::getId).collect(Collectors.toList());

    state.visitPositions(
        (exporterId, position) -> {
          if (!exporterIds.contains(exporterId)) {
            state.removePosition(exporterId);

            LOG.info(
                "The exporter '{}' is not configured anymore. Its position is removed from the state.",
                exporterId);
          }
        });
  }

  @Override
  public void onClose() {
    for (final ExporterContainer container : containers) {
      try {
        container.exporter.close();
      } catch (final Exception e) {
        container.context.getLogger().error("Error on close", e);
      }
    }
  }

  private class ExporterContainer implements Controller {
    private final ExporterContext context;
    private final Exporter exporter;
    private long position;

    ExporterContainer(ExporterDescriptor descriptor) {
      context =
          new ExporterContext(
              Loggers.getExporterLogger(descriptor.getId()), descriptor.getConfiguration());
      exporter = descriptor.newInstance();
    }

    @Override
    public void updateLastExportedRecordPosition(final long position) {
      actorControl.run(
          () -> {
            state.setPosition(getId(), position);
            this.position = position;
          });
    }

    @Override
    public void scheduleTask(final Duration delay, final Runnable task) {
      actorControl.runDelayed(delay, task);
    }

    private String getId() {
      return context.getConfiguration().getId();
    }
  }

  private class ExporterRecordProcessor implements EventProcessor {
    private final ExporterRecord record = new ExporterRecord();

    public void wrap(final LoggedEvent event) {
      event.readValue(record);
    }

    @Override
    public void processEvent() {
      for (final ExporterPosition position : record.getPositions()) {
        state.setPositionIfGreater(position.getId(), position.getPosition());
      }
    }
  }

  private class RecordExporter implements EventProcessor {
    private final ExporterObjectMapper objectMapper = new ExporterObjectMapper();
    private final ExporterRecordMapper recordMapper = new ExporterRecordMapper(objectMapper);
    private Record record;
    private boolean shouldExecuteSideEffects;
    private int exporterIndex;

    void wrap(LoggedEvent rawEvent) {
      final RecordMetadataImpl metadata =
          new RecordMetadataImpl(
              objectMapper,
              partitionId,
              rawMetadata.getIntent(),
              rawMetadata.getRecordType(),
              rawMetadata.getRejectionType(),
              BufferUtil.bufferAsString(rawMetadata.getRejectionReason()),
              rawMetadata.getValueType());

      record = recordMapper.map(rawEvent, metadata);
      exporterIndex = 0;
      shouldExecuteSideEffects = record != null;
    }

    @Override
    public boolean executeSideEffects() {
      if (!shouldExecuteSideEffects) {
        return true;
      }

      final int exportersCount = containers.size();

      // current error handling strategy is simply to repeat forever until the record can be
      // successfully exported.
      while (exporterIndex < exportersCount) {
        final ExporterContainer container = containers.get(exporterIndex);

        try {
          // TODO (saig0): check if the record is accepted by the exporter's filter
          if (container.position < record.getPosition()) {
            container.exporter.export(record);
          }

          exporterIndex++;
        } catch (final Exception ex) {
          container.context.getLogger().error("Error exporting record {}", record, ex);
          return false;
        }
      }

      return true;
    }
  }
}
