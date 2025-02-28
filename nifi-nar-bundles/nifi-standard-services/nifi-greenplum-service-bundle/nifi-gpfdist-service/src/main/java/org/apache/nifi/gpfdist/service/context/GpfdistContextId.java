package org.apache.nifi.gpfdist.service.context;

import org.apache.nifi.gpfdist.metadata.ContextId;

import java.util.Objects;

public class GpfdistContextId implements ContextId {
    private final String id;

    public GpfdistContextId(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        GpfdistContextId contextId = (GpfdistContextId) o;
        return Objects.equals(id, contextId.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        return "ContextId{" +
                "id='" + id + '\'' +
                '}';
    }
}
