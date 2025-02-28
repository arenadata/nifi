package org.apache.nifi.gpfdist.processor;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.gpfdist.service.GpfdistService;
import org.apache.nifi.gpfdist.service.GreenplumService;
import org.apache.nifi.gpfdist.service.RecordSinkProvider;
import org.apache.nifi.gpfdist.service.TransferDataQueryExecutor;
import org.apache.nifi.reporting.InitializationException;

import static org.mockito.Mockito.mock;

public class MockGpfdistService extends AbstractConfigurableComponent implements GpfdistService {
    private final RecordSinkProvider recordSinkProvider = mock(RecordSinkProvider.class);
    private final GreenplumService greenplumService = mock(GreenplumService.class);;
    private final TransferDataQueryExecutor transferDataQueryExecutor = mock(TransferDataQueryExecutor.class);

    @Override
    public RecordSinkProvider getRecordSinkProvider() {
        return recordSinkProvider;
    }

    @Override
    public GreenplumService getGreenplumTableService() {
        return greenplumService;
    }

    @Override
    public TransferDataQueryExecutor getQueryExecutor() {
        return transferDataQueryExecutor;
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
