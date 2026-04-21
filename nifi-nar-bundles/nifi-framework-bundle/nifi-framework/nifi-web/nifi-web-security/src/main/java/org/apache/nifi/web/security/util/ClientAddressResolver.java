/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.web.security.util;

import org.apache.commons.lang3.StringUtils;

import javax.servlet.http.HttpServletRequest;

public class ClientAddressResolver {
    private static final String X_FORWARDED_FOR_HEADER = "X-Forwarded-For";

    public String getClientAddress(final HttpServletRequest request) {
        final String forwardedFor = request.getHeader(X_FORWARDED_FOR_HEADER);
        if (StringUtils.isNotBlank(forwardedFor)) {
            final int delimiter = forwardedFor.indexOf(',');
            return delimiter >= 0 ? forwardedFor.substring(0, delimiter).trim() : forwardedFor.trim();
        }

        return request.getRemoteAddr();
    }
}
