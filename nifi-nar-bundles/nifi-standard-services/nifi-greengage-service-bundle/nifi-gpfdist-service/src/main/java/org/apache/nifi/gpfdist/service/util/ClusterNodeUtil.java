package org.apache.nifi.gpfdist.service.util;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public final class ClusterNodeUtil {

    private ClusterNodeUtil() {
    }

    public static Set<String> getNodesHostnames(String hostsStr) {
        return Arrays.stream(hostsStr.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }
}
