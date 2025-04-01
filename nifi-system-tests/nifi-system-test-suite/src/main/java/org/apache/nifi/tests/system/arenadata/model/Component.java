package org.apache.nifi.tests.system.arenadata.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum Component {
    MDW("mdw", 6000),
    SDW1("sdw1", 0),
    SDW2("sdw2", 0),
    SMDW("smdw", 0),
    POSTGRES("postgres", 5432);

    private final String name;
    private final int port;
}
