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
package org.apache.nifi.web.security.jwt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.nifi.web.security.NiFiWebAuthenticationDetails;
import org.apache.nifi.web.security.jwt.converter.RequestDetailsJwtAuthenticationConverter;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.server.resource.InvalidBearerTokenException;
import org.springframework.security.oauth2.server.resource.authentication.BearerTokenAuthenticationToken;
import org.springframework.util.Assert;

public class RequestDetailsJwtAuthenticationProvider implements AuthenticationProvider {
    private final Log logger = LogFactory.getLog(getClass());

    private final JwtDecoder jwtDecoder;

    private final RequestDetailsJwtAuthenticationConverter jwtAuthenticationConverter;

    public RequestDetailsJwtAuthenticationProvider(
            final JwtDecoder jwtDecoder,
            final RequestDetailsJwtAuthenticationConverter jwtAuthenticationConverter
    ) {
        Assert.notNull(jwtDecoder, "jwtDecoder cannot be null");
        Assert.notNull(jwtAuthenticationConverter, "jwtAuthenticationConverter cannot be null");
        this.jwtDecoder = jwtDecoder;
        this.jwtAuthenticationConverter = jwtAuthenticationConverter;
    }

    @Override
    public Authentication authenticate(final Authentication authentication) throws AuthenticationException {
        final BearerTokenAuthenticationToken bearerToken = (BearerTokenAuthenticationToken) authentication;
        final Jwt jwt = getJwt(bearerToken);
        final String clientAddress = getClientAddress(bearerToken.getDetails());
        final NiFiAuthenticationToken authenticationToken = jwtAuthenticationConverter.convertWithClientAddress(jwt, clientAddress);
        authenticationToken.setDetails(bearerToken.getDetails());
        logger.debug("Authenticated token");
        return authenticationToken;
    }

    @Override
    public boolean supports(final Class<?> authentication) {
        return BearerTokenAuthenticationToken.class.isAssignableFrom(authentication);
    }

    private Jwt getJwt(final BearerTokenAuthenticationToken bearerToken) {
        try {
            return jwtDecoder.decode(bearerToken.getToken());
        } catch (final BadJwtException e) {
            logger.debug("Failed to authenticate since the JWT was invalid");
            throw new InvalidBearerTokenException(e.getMessage(), e);
        } catch (final JwtException e) {
            throw new AuthenticationServiceException(e.getMessage(), e);
        }
    }

    private String getClientAddress(final Object details) {
        if (details instanceof NiFiWebAuthenticationDetails webAuthenticationDetails) {
            return webAuthenticationDetails.getRemoteAddress();
        }

        return null;
    }
}
