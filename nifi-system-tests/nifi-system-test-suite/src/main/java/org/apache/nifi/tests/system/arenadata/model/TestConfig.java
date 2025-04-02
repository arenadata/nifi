package org.apache.nifi.tests.system.arenadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TestConfig {
    private DataSourceProperties adb;
    private DataSourceProperties postgres;
    private String gpfdistPort;
    private int generalTimeout;
    private int pollInterval;
    private String dockerHostIp;
}
