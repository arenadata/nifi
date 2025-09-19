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
package org.apache.nifi.gpfdist.server.servlet;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.nifi.gpfdist.server.request.GpfdistReadableRequest;
import org.apache.nifi.gpfdist.service.context.GpfdistContextId;
import org.apache.nifi.gpfdist.service.load.context.WriteContext;
import org.apache.nifi.gpfdist.service.load.context.WriteContextManager;
import org.apache.nifi.gpfdist.service.load.process.GpfdistPacketBuilder;
import org.apache.nifi.gpfdist.service.load.process.GpfdistRecordProcessor;
import org.apache.nifi.gpfdist.service.load.process.RecordProcessorFactory;
import org.apache.nifi.logging.ComponentLog;

import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_PROTO;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.COMPONENT_LOG_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.RECORD_PROCESSING_EXECUTOR_SERVICE_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.RECORD_PROCESSOR_FACTORY_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.WRITE_CONTEXT_MANAGER_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.createGpfdistFileName;

public class GpfdistAsyncServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        AsyncContext asyncCtx = request.startAsync();
        String tableName = getExternalTableName(request);
        Map<String, String> headers = getHeaderMap(request);
        GpfdistReadableRequest readableRequest = GpfdistReadableRequest.create(tableName, headers);
        ExecutorService executorService = (ExecutorService) getServletContext().getAttribute(RECORD_PROCESSING_EXECUTOR_SERVICE_ATTR);
        WriteContextManager contextManager = (WriteContextManager) getServletContext().getAttribute(WRITE_CONTEXT_MANAGER_ATTR);
        RecordProcessorFactory recordProcessorFactory = (RecordProcessorFactory) getServletContext().getAttribute(RECORD_PROCESSOR_FACTORY_ATTR);
        ComponentLog logger = (ComponentLog) getServletContext().getAttribute(COMPONENT_LOG_ATTR);
        Optional<WriteContext> writeContextOptional = contextManager.get(new GpfdistContextId(tableName));
        logger.info("Input GET gpfdist request: {}", readableRequest);

        HttpServletResponse asyncResponse = (HttpServletResponse) asyncCtx.getResponse();
        asyncResponse.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN.getMimeType());
        asyncResponse.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
        asyncResponse.setHeader(X_GP_PROTO, String.valueOf(readableRequest.getGpProtocol()));

        if (writeContextOptional.isPresent()) {
            WriteContext writeContext = writeContextOptional.get();
            executorService.submit(() -> {
                logger.info("Start handling input GET gpfdist request: {}", readableRequest);
                try {
                    asyncResponse.setStatus(HttpServletResponse.SC_OK);
                    try (PipedOutputStream outputStream = new PipedOutputStream();
                         PipedInputStream inputStream = new PipedInputStream(outputStream, writeContext.getBufferSize())) {
                        GpfdistRecordProcessor recordProcessor = (GpfdistRecordProcessor) recordProcessorFactory.create(readableRequest,
                                writeContext,
                                outputStream);
                        boolean isAdded = writeContext.getRecordProcessorProvider().register(recordProcessor);
                        ServletOutputStream out = asyncResponse.getOutputStream();
                        if (isAdded) {
                            byte[] buf = new byte[writeContext.getBufferSize()];
                            int readLen;
                            while ((readLen = inputStream.read(buf, 0, buf.length)) != -1) {
                                out.write(buf, 0, readLen);
                                out.flush();
                            }
                        } else {
                            out.write(new GpfdistPacketBuilder(createGpfdistFileName(tableName)).createSingleEmptyDataPacket());
                        }
                        logger.info("Request completed successfully: {}", readableRequest);
                    }
                } catch (Exception e) {
                    asyncResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    getServletContext().log("Failed to load data. Request: " + readableRequest, e);
                } finally {
                    asyncCtx.complete();
                }
            });
        } else {
            asyncResponse.setStatus(HttpServletResponse.SC_OK);
            logger.info("There is no data for loading responded by request: {}", readableRequest);
            asyncResponse.getOutputStream().write(new GpfdistPacketBuilder(createGpfdistFileName(tableName)).createSingleEmptyDataPacket());
            asyncCtx.complete();
        }
    }

    private String getExternalTableName(HttpServletRequest request) {
        return request.getPathInfo().substring(1);
    }

    private Map<String, String> getHeaderMap(HttpServletRequest request) {
        Map<String, String> headers = new HashMap<>();
        Enumeration<String> headerNames = request.getHeaderNames();
        if (headerNames != null) {
            while (headerNames.hasMoreElements()) {
                String header = headerNames.nextElement();
                headers.put(header, request.getHeader(header));
            }
        }
        return headers;
    }
}
