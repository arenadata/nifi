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
package org.apache.nifi.gpfdist.service;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public final class GpfdistProperties {
    private GpfdistProperties() {
    }

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Listening Port")
            .displayName("Listening Port")
            .description("The Port to listen on for incoming gpfdist requests")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("80")
            .build();
    public static final PropertyDescriptor GPFDIST_SERVER_MIN_THREADS = new PropertyDescriptor.Builder()
            .name("Minimum Gpfdist Server Threads")
            .displayName("Minimum Gpfdist Server Threads")
            .description("The minimum amount of threads that are used to run the Gpfdist server")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();
    public static final PropertyDescriptor GPFDIST_SERVER_MAX_THREADS = new PropertyDescriptor.Builder()
            .name("Maximum Gpfdist Server Threads")
            .displayName("Maximum Gpfdist Server Threads")
            .description("The maximum amount of threads that are used to run the Gpfdist server")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("8")
            .build();
    public static final PropertyDescriptor GPFDIST_SERVER_THREAD_IDLE_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("Maximum Gpfdist Server Threads Idle Timeout")
            .displayName("Maximum Gpfdist Server Threads Idle Timeout")
            .description("The maximum Gpfdist server threads idle timeout in milliseconds")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("60000")
            .build();
    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("put-greengage-record-dcbp-service")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain a connection to the greengage for executing queries.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();
    public static final PropertyDescriptor GPFDIST_REQUEST_PROCESSOR_MAX_THREADS = new PropertyDescriptor.Builder()
            .name("Maximum Gpfdist Request Processor Threads")
            .displayName("Maximum Gpfdist Request Processor Threads")
            .description("The maximum amount of threads that are used to process the gpfdist requests")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("8")
            .build();
    public static final PropertyDescriptor GPFDIST_PER_GREENGAGE_SEGMENT_STREAM_MAX_BUFFER_SIZE =
            new PropertyDescriptor.Builder()
                    .name("gpfdist-per-greengage-segment-stream-max-buffer-size")
                    .displayName("Gpfdist Per Greengage Segment Stream Max Buffer Size")
                    .description(
                            "Maximum amount of data that may be buffered in memory for a single gpfdist " +
                                    "write stream per Greengage segment before backpressure is applied. " +
                                    "This limits in-flight data when writing records to Greengage via gpfdist.")
                    .required(false)
                    .defaultValue("32 MB")
                    .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                    .build();
    public static final PropertyDescriptor GPFDIST_PER_GREENGAGE_SEGMENT_STREAM_BUFFER_ENQUEUE_TIMEOUT =
            new PropertyDescriptor.Builder()
                    .name("gpfdist-per-greengage-segment-stream-enqueue-timeout")
                    .displayName("Gpfdist Per Greengage Segment Stream Enqueue Timeout")
                    .description(
                            "Maximum time to wait when attempting to enqueue data into a gpfdist " +
                                    "write stream buffer for a Greengage segment. If the timeout is exceeded, " +
                                    "the operation fails fast to prevent unbounded blocking under backpressure.")
                    .required(false)
                    .defaultValue("200 ms")
                    .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                    .build();
}
