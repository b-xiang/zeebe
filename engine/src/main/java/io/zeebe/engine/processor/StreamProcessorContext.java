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

import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.spi.SnapshotController;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.ActorScheduler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class StreamProcessorContext {
  protected int id;
  protected String name;

  protected LogStream logStream;

  private CommandResponseWriter commandResponseWriter;
  private LogStreamReader logStreamReader;
  private TypedStreamWriter typedStreamWriter;

  protected Duration snapshotPeriod;
  protected SnapshotController snapshotController;

  protected ActorScheduler actorScheduler;
  protected final List<StreamProcessorLifecycleAware> lifecycleListeners = new ArrayList<>();
  private ActorControl actorControl;

  private EventFilter eventFilter;

  private Runnable suspendRunnable;
  private Runnable resumeRunnable;
  private int maxSnapshots;
  private boolean deleteDataOnSnapshot;
  private StreamProcessorFactory streamProcessorFactory;

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public LogStream getLogStream() {
    return logStream;
  }

  public CommandResponseWriter getCommandResponseWriter() {
    return commandResponseWriter;
  }

  public LogStreamReader getLogStreamReader() {
    return logStreamReader;
  }

  public TypedStreamWriter getTypedStreamWriter() {
    return typedStreamWriter;
  }

  public Duration getSnapshotPeriod() {
    return snapshotPeriod;
  }

  public SnapshotController getSnapshotController() {
    return snapshotController;
  }

  public ActorScheduler getActorScheduler() {
    return actorScheduler;
  }

  public ActorControl getActorControl() {
    return actorControl;
  }

  public EventFilter getEventFilter() {
    return eventFilter;
  }

  public Runnable getSuspendRunnable() {
    return suspendRunnable;
  }

  public Runnable getResumeRunnable() {
    return resumeRunnable;
  }

  public int getMaxSnapshots() {
    return maxSnapshots;
  }

  public boolean isDeleteDataOnSnapshot() {
    return deleteDataOnSnapshot;
  }

  public StreamProcessorFactory getStreamProcessorFactory() {
    return streamProcessorFactory;
  }

  public List<StreamProcessorLifecycleAware> getLifecycleListeners() {
    return lifecycleListeners;
  }

  public StreamProcessorContext id(int id) {
    this.id = id;
    return this;
  }

  public StreamProcessorContext name(String name) {
    this.name = name;
    return this;
  }

  public StreamProcessorContext logStream(LogStream logStream) {
    this.logStream = logStream;
    return this;
  }

  public StreamProcessorContext commandResponseWriter(CommandResponseWriter commandResponseWriter) {
    this.commandResponseWriter = commandResponseWriter;
    return this;
  }

  public StreamProcessorContext logStreamReader(LogStreamReader logStreamReader) {
    this.logStreamReader = logStreamReader;
    return this;
  }

  public StreamProcessorContext typedStreamWriter(TypedStreamWriter typedStreamWriter) {
    this.typedStreamWriter = typedStreamWriter;
    return this;
  }

  public StreamProcessorContext snapshotPeriod(Duration snapshotPeriod) {
    this.snapshotPeriod = snapshotPeriod;
    return this;
  }

  public StreamProcessorContext snapshotController(SnapshotController snapshotController) {
    this.snapshotController = snapshotController;
    return this;
  }

  public StreamProcessorContext actorScheduler(ActorScheduler actorScheduler) {
    this.actorScheduler = actorScheduler;
    return this;
  }

  public StreamProcessorContext actorControl(ActorControl actorControl) {
    this.actorControl = actorControl;
    return this;
  }

  public StreamProcessorContext eventFilter(EventFilter eventFilter) {
    this.eventFilter = eventFilter;
    return this;
  }

  public StreamProcessorContext suspendRunnable(Runnable suspendRunnable) {
    this.suspendRunnable = suspendRunnable;
    return this;
  }

  public StreamProcessorContext resumeRunnable(Runnable resumeRunnable) {
    this.resumeRunnable = resumeRunnable;
    return this;
  }

  public StreamProcessorContext maxSnapshots(int maxSnapshots) {
    this.maxSnapshots = maxSnapshots;
    return this;
  }

  public StreamProcessorContext deleteDataOnSnapshot(boolean deleteDataOnSnapshot) {
    this.deleteDataOnSnapshot = deleteDataOnSnapshot;
    return this;
  }

  public StreamProcessorContext streamProcessorFactory(
      StreamProcessorFactory streamProcessorFactory) {
    this.streamProcessorFactory = streamProcessorFactory;
    return this;
  }

  public StreamProcessorContext lifecycleListener(StreamProcessorLifecycleAware lifecycleListener) {
    this.lifecycleListeners.add(lifecycleListener);
    return this;
  }
}
