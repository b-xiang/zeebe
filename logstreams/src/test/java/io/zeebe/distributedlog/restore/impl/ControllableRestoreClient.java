/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.distributedlog.restore.impl;

import io.atomix.cluster.MemberId;
import io.zeebe.distributedlog.restore.RestoreClient;
import io.zeebe.distributedlog.restore.RestoreInfoRequest;
import io.zeebe.distributedlog.restore.RestoreInfoResponse;
import io.zeebe.distributedlog.restore.log.LogReplicationRequest;
import io.zeebe.distributedlog.restore.log.LogReplicationResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ControllableRestoreClient implements RestoreClient {
  private final Map<Long, CompletableFuture<LogReplicationResponse>> logReplicationRequests =
      new HashMap<>();

  @Override
  public CompletableFuture<Integer> requestSnapshotInfo(MemberId server) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void requestLatestSnapshot(MemberId server) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public CompletableFuture<LogReplicationResponse> requestLogReplication(
      MemberId server, LogReplicationRequest request) {
    final CompletableFuture<LogReplicationResponse> result = new CompletableFuture<>();
    logReplicationRequests.put(request.getFromPosition(), result);
    return result;
  }

  @Override
  public CompletableFuture<RestoreInfoResponse> requestRestoreInfo(
      MemberId server, RestoreInfoRequest request) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void completeLogReplication(long from, Throwable ex) {
    logReplicationRequests.get(from).completeExceptionally(ex);
  }

  public void completeLogReplication(long from, LogReplicationResponse response) {
    logReplicationRequests.get(from).complete(response);
  }

  public Map<Long, CompletableFuture<LogReplicationResponse>> getLogReplicationRequests() {
    return logReplicationRequests;
  }

  public void reset() {
    logReplicationRequests.clear();
  }
}
