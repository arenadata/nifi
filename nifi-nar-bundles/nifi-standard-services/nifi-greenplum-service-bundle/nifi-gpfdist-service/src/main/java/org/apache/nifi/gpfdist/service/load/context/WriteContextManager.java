package org.apache.nifi.gpfdist.service.load.context;

import org.apache.nifi.gpfdist.service.context.AbstractContextManager;
import org.apache.nifi.logging.ComponentLog;

public class WriteContextManager extends AbstractContextManager<WriteContext> {

    public WriteContextManager(ComponentLog logger) {
        super(logger);
    }
}
