package org.apache.nifi.tests.system.arenadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataSourceProperties {
    private String url;
    private String driverName;
    private String driverLocation;
    private String username;
    private String password;
}
