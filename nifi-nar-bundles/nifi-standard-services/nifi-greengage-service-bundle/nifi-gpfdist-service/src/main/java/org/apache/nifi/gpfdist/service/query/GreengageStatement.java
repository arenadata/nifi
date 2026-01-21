package org.apache.nifi.gpfdist.service.query;

import java.sql.SQLException;

public interface GreengageStatement {

    void execute() throws SQLException;

    void cancel() throws SQLException;
}
