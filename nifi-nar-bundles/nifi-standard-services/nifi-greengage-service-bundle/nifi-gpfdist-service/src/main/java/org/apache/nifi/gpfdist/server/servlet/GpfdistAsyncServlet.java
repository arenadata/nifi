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
import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextManager;
import org.apache.nifi.gpfdist.server.request.GpfdistReadableRequest;
import org.apache.nifi.gpfdist.server.request.GpfdistWritableRequest;
import org.apache.nifi.gpfdist.service.context.GpfdistContextId;
import org.apache.nifi.gpfdist.service.load.context.WriteContext;
import org.apache.nifi.gpfdist.service.load.process.GpfdistPacketBuilder;
import org.apache.nifi.gpfdist.service.load.process.GpfdistRecordProcessor;
import org.apache.nifi.gpfdist.service.load.process.RecordProcessorFactory;
import org.apache.nifi.gpfdist.service.unload.context.ReadContext;
import org.apache.nifi.gpfdist.service.unload.dto.GreengageChunkId;
import org.apache.nifi.gpfdist.service.unload.dto.ProcessingChunkId;
import org.apache.nifi.gpfdist.service.unload.process.GpfdistChunkRequestProcessor;
import org.apache.nifi.gpfdist.service.unload.process.InputDataProcessor;
import org.apache.nifi.gpfdist.service.unload.process.InputDataProcessorFactory;
import org.apache.nifi.gpfdist.service.unload.process.RecordProcessingService;
import org.apache.nifi.logging.ComponentLog;

import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.apache.nifi.gpfdist.server.request.GpfdistRequestHeader.X_GP_PROTO;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.COMPONENT_LOG_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.INPUT_DATA_PROCESSOR_FACTORY_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.READ_CONTEXT_MANAGER_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.RECORD_PROCESSING_EXECUTOR_SERVICE_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.RECORD_PROCESSOR_FACTORY_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.WRITE_CONTEXT_MANAGER_ATTR;
import static org.apache.nifi.gpfdist.service.util.GpfdistUtil.createGpfdistFileName;

public class GpfdistAsyncServlet extends HttpServlet {

    private static final int GPFDIST_FOR_WRITE_PROTOCOL_VERSION = 0;

    @SuppressWarnings("unchecked")
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        AsyncContext asyncCtx = request.startAsync();
        //todo refactor for using contextId and processorTaskId
        String tableName = getExternalTableName(request);
        Map<String, String> headers = getHeaderMap(request);
        GpfdistReadableRequest readableRequest = GpfdistReadableRequest.create(tableName, headers);
        ExecutorService executorService = (ExecutorService) getServletContext().getAttribute(RECORD_PROCESSING_EXECUTOR_SERVICE_ATTR);
        ContextManager<Context> contextManager = (ContextManager<Context>) getServletContext().getAttribute(WRITE_CONTEXT_MANAGER_ATTR);
        RecordProcessorFactory recordProcessorFactory = (RecordProcessorFactory) getServletContext().getAttribute(RECORD_PROCESSOR_FACTORY_ATTR);
        ComponentLog logger = (ComponentLog) getServletContext().getAttribute(COMPONENT_LOG_ATTR);
        Optional<Context> writeContextOptional = contextManager.get(new GpfdistContextId(tableName));
        logger.info("Input GET gpfdist request: {}", readableRequest);

        HttpServletResponse asyncResponse = (HttpServletResponse) asyncCtx.getResponse();
        asyncResponse.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN.getMimeType());
        asyncResponse.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
        asyncResponse.setHeader(X_GP_PROTO, String.valueOf(readableRequest.getGpProtocol()));

        if (writeContextOptional.isPresent()) {
            WriteContext writeContext = (WriteContext) writeContextOptional.get();
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

    @SuppressWarnings("unchecked")
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) {
        //process POST request for endpoint: "gpfdist://<host>:<port>/gpfdist/read/<contextId>/<processorTaskId>/<external_table_name>
        AsyncContext asyncCtx = request.startAsync();
        ComponentLog logger = (ComponentLog) getServletContext().getAttribute(COMPONENT_LOG_ATTR);
        try {
            GpfdistUrlMetadata metadata = getMetadata(request.getRequestURI());
            Map<String, String> headers = getHeaderMap(request);
            ContextManager<Context> contextManager = (ContextManager<Context>) getServletContext().getAttribute(READ_CONTEXT_MANAGER_ATTR);
            InputDataProcessorFactory inputDataProcessorFactory = (InputDataProcessorFactory) getServletContext().getAttribute(INPUT_DATA_PROCESSOR_FACTORY_ATTR);
            ExecutorService executorService = (ExecutorService) getServletContext().getAttribute(RECORD_PROCESSING_EXECUTOR_SERVICE_ATTR);
            GpfdistWritableRequest writableRequest = GpfdistWritableRequest.create(metadata.getTableName(), headers);
            logger.info("Input POST gpfdist request: {}", writableRequest);
            HttpServletResponse asyncResponse = (HttpServletResponse) asyncCtx.getResponse();
            if (writableRequest.getGpProtocol() != GPFDIST_FOR_WRITE_PROTOCOL_VERSION) {
                logger.error("Invalid gpfdist writing protocol version: {}, expected: {}",
                        writableRequest.getGpProtocol(),
                        GPFDIST_FOR_WRITE_PROTOCOL_VERSION);
                asyncResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                asyncCtx.complete();
            }
            Optional<Context> readContextOptional = contextManager.get(new GpfdistContextId(metadata.getContextId()));
            if (readContextOptional.isEmpty()) {
                asyncResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "No active query for contextId: " + metadata.getContextId());
                logger.warn("No active query for contextId: {}", metadata.getContextId());
                asyncCtx.complete();
            } else {
                ReadContext readContext = (ReadContext) readContextOptional.get();
                if (initialRequest(writableRequest)) {
                    processInitialRequest(asyncResponse,
                            metadata.getProcessorTaskId(),
                            readContext,
                            writableRequest,
                            logger,
                            inputDataProcessorFactory);
                    asyncCtx.complete();
                } else if (!isLast(writableRequest)) {
                    processDataRequest(
                            asyncCtx,
                            request.getInputStream(),
                            asyncResponse,
                            metadata.getProcessorTaskId(),
                            readContext,
                            writableRequest,
                            executorService,
                            logger);
                } else {
                    processTearDownRequest(asyncResponse,
                            readContext,
                            metadata.getProcessorTaskId(),
                            writableRequest,
                            logger);
                    asyncCtx.complete();
                }
            }
        } catch (Exception e) {
            getServletContext().log("Failed to load data", e);
            logger.error("Failed to process request", e);
            asyncCtx.complete();
        }
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

    private String getExternalTableName(HttpServletRequest request) {
        return request.getPathInfo().substring(1);
    }

    private GpfdistUrlMetadata getMetadata(String path) {
        // pathParts[0] = "", pathParts[1] = "gpfdist", pathParts[2] = "read", pathParts[3] = contextId, pathParts[4] = processorTaskId, pathParts[5] = external_table_name
        String[] pathParts = path.split("/");
        if (pathParts.length >= 5) {
            String contextId = pathParts[3];
            String processorTaskId = pathParts[4];
            String externalTableName = pathParts[5];
            return new GpfdistUrlMetadata(contextId, processorTaskId, externalTableName);
        } else {
            throw new IllegalStateException("Invalid gpfdist URL format");
        }
    }

    private boolean initialRequest(GpfdistWritableRequest request) {
        return request.getGpSequence() == 1;
    }

    private void processInitialRequest(HttpServletResponse asyncResponse,
                                       String processorTaskId,
                                       ReadContext readContext,
                                       GpfdistWritableRequest request,
                                       ComponentLog logger,
                                       InputDataProcessorFactory inputDataProcessorFactory) {
        GreengageChunkId chunkId = request.getChunkId();
        RecordProcessingService recordProcessingService = readContext.getRecordProcessingService(processorTaskId);
        InputDataProcessor dataProcessor = inputDataProcessorFactory.create(readContext,
                new ProcessingChunkId(processorTaskId, chunkId.getTransactionId(), chunkId.getSegmentId()),
                recordProcessingService);
        recordProcessingService.addChunkRequestProcessor(new GpfdistChunkRequestProcessor(chunkId, dataProcessor, logger));
        asyncResponse.setStatus(HttpServletResponse.SC_OK);
        asyncResponse.setHeader(X_GP_PROTO, String.valueOf(request.getGpProtocol()));
        logger.debug("Request for initial data transferring completed successfully: {}", request);
    }

    private void processDataRequest(AsyncContext asyncCtx,
                                    InputStream dataStream,
                                    HttpServletResponse asyncResponse,
                                    String processorTaskId,
                                    ReadContext readContext,
                                    GpfdistWritableRequest request,
                                    ExecutorService executorService,
                                    ComponentLog logger) {
        executorService.submit(() -> {
            try {
                GpfdistChunkRequestProcessor processor = readContext.getRecordProcessingService(processorTaskId)
                        .getChunkRequestProcessor(request.getChunkId());
                processor.process(dataStream);
                asyncResponse.setStatus(HttpServletResponse.SC_OK);
                asyncResponse.setHeader(X_GP_PROTO, String.valueOf(request.getGpProtocol()));
                logger.debug("Request for data transferring completed successfully: {}", request);
            } catch (Exception e) {
                logger.error("Failed to process request: {}. ", e.getMessage(), e);
                asyncResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                asyncResponse.setHeader(X_GP_PROTO, String.valueOf(request.getGpProtocol()));
            } finally {
                asyncCtx.complete();
            }
        });
    }

    private void processTearDownRequest(HttpServletResponse asyncResponse,
                                        ReadContext readContext,
                                        String processorTaskId,
                                        GpfdistWritableRequest request,
                                        ComponentLog logger) {
        GpfdistChunkRequestProcessor processor = readContext.getRecordProcessingService(processorTaskId)
                .getChunkRequestProcessor(request.getChunkId());
        processor.stop();
        asyncResponse.setStatus(HttpServletResponse.SC_OK);
        asyncResponse.setHeader(X_GP_PROTO, String.valueOf(request.getGpProtocol()));
        logger.debug("Processing request for finishing data transferring completed successfully: {}", request);
    }

    private boolean isLast(GpfdistWritableRequest request) {
        return request.isLastChunk().isPresent() && request.isLastChunk().get();
    }

    private static class GpfdistUrlMetadata {
        private final String contextId;
        private final String processorTaskId;
        private final String tableName;

        public GpfdistUrlMetadata(String contextId, String processorTaskId, String tableName) {
            this.contextId = contextId;
            this.processorTaskId = processorTaskId;
            this.tableName = tableName;
        }

        public String getContextId() {
            return contextId;
        }

        public String getProcessorTaskId() {
            return processorTaskId;
        }

        public String getTableName() {
            return tableName;
        }
    }
}
