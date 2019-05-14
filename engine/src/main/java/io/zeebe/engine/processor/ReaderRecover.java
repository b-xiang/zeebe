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

import io.zeebe.logstreams.log.LogStreamReader;

public class ReaderRecover {

  private static final String ERROR_MESSAGE_RECOVER_FROM_SNAPSHOT_FAILED =
      "Expected to find event with the snapshot position %s in log stream, but nothing was found. Failed to recover '%s'.";

  public static void recoverReader(
      LogStreamReader logStreamReader,
      long lowerBoundPosition,
      long snapshotPosition,
      String name) {
    logStreamReader.seekToFirstEvent();
    if (lowerBoundPosition > -1 && snapshotPosition > -1) {
      final boolean found = logStreamReader.seek(snapshotPosition);
      if (found && logStreamReader.hasNext()) {
        logStreamReader.seek(snapshotPosition + 1);
      } else {
        throw new IllegalStateException(
            String.format(ERROR_MESSAGE_RECOVER_FROM_SNAPSHOT_FAILED, snapshotPosition, name));
      }
    }
  }
}
