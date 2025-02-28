package org.apache.nifi.gpfdist.service;

import org.apache.nifi.gpfdist.metadata.GpfdistMetadata;

import java.util.concurrent.CompletableFuture;

public interface TransferDataQueryExecutor {
    CompletableFuture<Void> execute(GpfdistMetadata context);
}
