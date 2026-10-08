/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.services.iceberg.hive.cluster;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;

/**
 * Ranger audit event written by a plugin as one JSON line in the file audit destination
 *
 * @param user Short name of the requesting user
 * @param resource Requested HDFS path or Ozone volume, bucket and key
 * @param resourceType Ozone resource type such as volume, bucket or key, or path for HDFS
 * @param access Operation such as mkdirs or create for HDFS and the requested access type for Ozone
 * @param action Access type such as read or write for HDFS and read, create or write for Ozone
 * @param result Access result: 1 for allowed and 0 for denied
 * @param policyId Identifier of the deciding policy or -1 when no policy decided
 * @param enforcer Authorizer that decided: ranger-acl or hadoop-acl for the HDFS permission fallback
 */
public record AuditEvent(String user, String resource, String resourceType, String access, String action, int result, long policyId, String enforcer) {
    public static final int DENIED = 0;

    public static final int ALLOWED = 1;

    public static final long NO_POLICY = -1;

    private static final int UNKNOWN_RESULT = -1;

    private static final String USER_FIELD = "reqUser";

    private static final String RESOURCE_FIELD = "resource";

    private static final String RESOURCE_TYPE_FIELD = "resType";

    private static final String ACCESS_FIELD = "access";

    private static final String ACTION_FIELD = "action";

    private static final String RESULT_FIELD = "result";

    private static final String POLICY_FIELD = "policy";

    private static final String ENFORCER_FIELD = "enforcer";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Parse an audit line, ignoring lines that are not complete JSON objects
     *
     * @param line Audit line
     * @return Audit event or empty when the line is not a JSON object
     */
    public static Optional<AuditEvent> parse(final String line) {
        try {
            final JsonNode event = OBJECT_MAPPER.readTree(line);
            if (event == null || !event.isObject()) {
                return Optional.empty();
            }
            return Optional.of(new AuditEvent(
                    event.path(USER_FIELD).asText(),
                    event.path(RESOURCE_FIELD).asText(),
                    event.path(RESOURCE_TYPE_FIELD).asText(),
                    event.path(ACCESS_FIELD).asText(),
                    event.path(ACTION_FIELD).asText(),
                    event.path(RESULT_FIELD).asInt(UNKNOWN_RESULT),
                    event.path(POLICY_FIELD).asLong(NO_POLICY),
                    event.path(ENFORCER_FIELD).asText()));
        } catch (final JsonProcessingException e) {
            return Optional.empty();
        }
    }
}
