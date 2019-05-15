/*
 * Copyright © 2019  camunda services GmbH (info@camunda.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package io.zeebe.engine.processor;

import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.zeebe.protocol.impl.record.value.error.ErrorRecord;
import io.zeebe.protocol.impl.record.value.incident.IncidentRecord;
import io.zeebe.protocol.impl.record.value.job.JobBatchRecord;
import io.zeebe.protocol.impl.record.value.job.JobRecord;
import io.zeebe.protocol.impl.record.value.message.MessageRecord;
import io.zeebe.protocol.impl.record.value.message.MessageStartEventSubscriptionRecord;
import io.zeebe.protocol.impl.record.value.message.MessageSubscriptionRecord;
import io.zeebe.protocol.impl.record.value.message.WorkflowInstanceSubscriptionRecord;
import io.zeebe.protocol.impl.record.value.timer.TimerRecord;
import io.zeebe.protocol.impl.record.value.variable.VariableDocumentRecord;
import io.zeebe.protocol.impl.record.value.variable.VariableRecord;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceCreationRecord;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import java.util.EnumMap;

public class TypedEventRegistry {

  public static final EnumMap<ValueType, Class<? extends UnpackedObject>> EVENT_REGISTRY =
      new EnumMap<>(ValueType.class);

  static {
    EVENT_REGISTRY.put(ValueType.DEPLOYMENT, DeploymentRecord.class);
    EVENT_REGISTRY.put(ValueType.JOB, JobRecord.class);
    EVENT_REGISTRY.put(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceRecord.class);
    EVENT_REGISTRY.put(ValueType.INCIDENT, IncidentRecord.class);
    EVENT_REGISTRY.put(ValueType.MESSAGE, MessageRecord.class);
    EVENT_REGISTRY.put(ValueType.MESSAGE_SUBSCRIPTION, MessageSubscriptionRecord.class);
    EVENT_REGISTRY.put(
        ValueType.MESSAGE_START_EVENT_SUBSCRIPTION, MessageStartEventSubscriptionRecord.class);
    EVENT_REGISTRY.put(
        ValueType.WORKFLOW_INSTANCE_SUBSCRIPTION, WorkflowInstanceSubscriptionRecord.class);
    EVENT_REGISTRY.put(ValueType.JOB_BATCH, JobBatchRecord.class);
    EVENT_REGISTRY.put(ValueType.TIMER, TimerRecord.class);
    EVENT_REGISTRY.put(ValueType.VARIABLE, VariableRecord.class);
    EVENT_REGISTRY.put(ValueType.VARIABLE_DOCUMENT, VariableDocumentRecord.class);
    EVENT_REGISTRY.put(ValueType.WORKFLOW_INSTANCE_CREATION, WorkflowInstanceCreationRecord.class);
    EVENT_REGISTRY.put(ValueType.ERROR, ErrorRecord.class);
  }
}
