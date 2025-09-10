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
            .description("The Port to listen on for incoming gpfdist requests")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("80")
            .build();
    public static final PropertyDescriptor GPFDIST_SERVER_MIN_THREADS = new PropertyDescriptor.Builder()
            .name("Minimum Gpffist Server Threads")
            .description("The minimum amount of threads that are used to run the Gpffist server")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();
    public static final PropertyDescriptor GPFDIST_SERVER_MAX_THREADS = new PropertyDescriptor.Builder()
            .name("Maximum Gpffist Server Threads")
            .description("The maximum amount of threads that are used to run the Gpffist server")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("8")
            .build();
    public static final PropertyDescriptor GPFDIST_SERVER_THREAD_IDLE_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("The Maximum Gpffist Server Threads Idle Timeout")
            .description("The maximum gpffist server threads idle timeout in milliseconds")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("60000")
            .build();
    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("put-greenplum-record-dcbp-service")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain a connection to the greenplum for executing queries.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();
    public static final PropertyDescriptor WRITE_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("put-greenplum-record-write-buffer-size")
            .displayName("Write buffer size in bytes")
            .description("Write byte buffer size for serialized records.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .required(false)
            .defaultValue("1 MB")
            .build();
    public static final PropertyDescriptor RECORD_PROCESSOR_MAX_THREADS = new PropertyDescriptor.Builder()
            .name("Maximum Record Processor Threads")
            .description("The maximum amount of threads that are used to process the records")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("8")
            .build();
    public static final PropertyDescriptor GPFDIST_REQUEST_PROCESSOR_MAX_THREADS = new PropertyDescriptor.Builder()
            .name("Maximum Gpfdist Request Processor Threads")
            .description("The maximum amount of threads that are used to process the gpfdist requests")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("8")
            .build();
}
