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
package org.apache.nifi.gpfdist.processor;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.gpfdist.metadata.Context;
import org.apache.nifi.gpfdist.metadata.ContextManager;
import org.apache.nifi.gpfdist.service.GpfdistLoadMetadataFactory;
import org.apache.nifi.gpfdist.service.GpfdistService;
import org.apache.nifi.gpfdist.service.GpfdistUnloadMetadataFactory;
import org.apache.nifi.gpfdist.service.GreengageService;
import org.apache.nifi.gpfdist.service.NodeIndexService;
import org.apache.nifi.gpfdist.service.TransferDataQueryExecutor;
import org.apache.nifi.reporting.InitializationException;

import static org.mockito.Mockito.mock;

public class MockGpfdistService extends AbstractConfigurableComponent implements GpfdistService {
    private final GreengageService greengageService = mock(GreengageService.class);
    private final TransferDataQueryExecutor unloadDataQueryExecutor = mock(TransferDataQueryExecutor.class);
    private final TransferDataQueryExecutor transferDataQueryExecutor = mock(TransferDataQueryExecutor.class);
    private final TransferDataQueryExecutor insertDataQueryExecutor = mock(TransferDataQueryExecutor.class);
    private final TransferDataQueryExecutor dropExternalTableQueryExecutor = mock(TransferDataQueryExecutor.class);
    private final GpfdistUnloadMetadataFactory unloadMetadataFactory = mock(GpfdistUnloadMetadataFactory.class);
    private final GpfdistLoadMetadataFactory loadMetadataFactory = mock(GpfdistLoadMetadataFactory.class);
    private final ContextManager<? extends Context> readContextManager = mock(ContextManager.class);
    private final ContextManager<? extends Context> writeContextManager = mock(ContextManager.class);
    private final NodeIndexService nodeIndexService = mock(NodeIndexService.class);

    @Override
    public GreengageService getGreengageMetadataService() {
        return greengageService;
    }

    @Override
    public TransferDataQueryExecutor getCreateReadExternalTableQueryExecutor() {
        return transferDataQueryExecutor;
    }

    @Override
    public TransferDataQueryExecutor getInsertDataQueryExecutor() {
        return insertDataQueryExecutor;
    }

    @Override
    public TransferDataQueryExecutor getDropExternalTableQueryExecutor() {
        return dropExternalTableQueryExecutor;
    }

    @Override
    public TransferDataQueryExecutor getUnloadDataQueryExecutor() {
        return unloadDataQueryExecutor;
    }

    @Override
    public GpfdistUnloadMetadataFactory getGpfdistUnloadMetadataFactory() {
        return unloadMetadataFactory;
    }

    @Override
    public GpfdistLoadMetadataFactory getGpfdistLoadMetadataFactory() {
        return loadMetadataFactory;
    }

    @Override
    public ContextManager<? extends Context> getReadContextManager() {
        return readContextManager;
    }

    @Override
    public ContextManager<? extends Context> getWriteContextManager() {
        return writeContextManager;
    }

    @Override
    public NodeIndexService getNodeIndexService() {
        return nodeIndexService;
    }

    @Override
    public void initialize(ControllerServiceInitializationContext context) throws InitializationException {

    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

    }

    @Override
    public String getIdentifier() {
        return "MockGpfdistService";
    }
}
