package org.apache.nifi.gpfdist.service;

import org.apache.nifi.controller.ControllerService;

public interface GpfdistService extends ControllerService {

    RecordSinkProvider getRecordSinkProvider();

    GreenplumService getGreenplumTableService();

    TransferDataQueryExecutor getQueryExecutor();
}
