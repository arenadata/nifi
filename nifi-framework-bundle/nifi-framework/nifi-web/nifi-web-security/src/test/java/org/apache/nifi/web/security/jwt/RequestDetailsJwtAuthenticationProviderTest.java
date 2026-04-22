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

import org.apache.nifi.web.security.NiFiWebAuthenticationDetails;
import org.apache.nifi.web.security.jwt.converter.RequestDetailsJwtAuthenticationConverter;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.BearerTokenAuthenticationToken;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class RequestDetailsJwtAuthenticationProviderTest {
    private static final String TOKEN = "bearer-token";
    private static final String CLIENT_ADDRESS = "10.20.30.40";

    @Mock
    private JwtDecoder jwtDecoder;

    @Mock
    private RequestDetailsJwtAuthenticationConverter jwtAuthenticationConverter;

    @Mock
    private Jwt jwt;

    @Mock
    private NiFiAuthenticationToken niFiAuthenticationToken;

    @Mock
    private NiFiWebAuthenticationDetails webAuthenticationDetails;

    @Test
    public void testConstructorRequiresJwtDecoder() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new RequestDetailsJwtAuthenticationProvider(null, jwtAuthenticationConverter)
        );

        assertEquals("jwtDecoder cannot be null", exception.getMessage());
    }

    @Test
    public void testConstructorRequiresJwtAuthenticationConverter() {
        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new RequestDetailsJwtAuthenticationProvider(jwtDecoder, null)
        );

        assertEquals("jwtAuthenticationConverter cannot be null", exception.getMessage());
    }

    @Test
    public void testAuthenticateUsesRequestClientAddress() {
        final RequestDetailsJwtAuthenticationProvider provider = new RequestDetailsJwtAuthenticationProvider(jwtDecoder, jwtAuthenticationConverter);
        final BearerTokenAuthenticationToken authenticationToken = new BearerTokenAuthenticationToken(TOKEN);
        authenticationToken.setDetails(webAuthenticationDetails);

        when(jwtDecoder.decode(TOKEN)).thenReturn(jwt);
        when(webAuthenticationDetails.getRemoteAddress()).thenReturn(CLIENT_ADDRESS);
        when(jwtAuthenticationConverter.convertWithClientAddress(jwt, CLIENT_ADDRESS)).thenReturn(niFiAuthenticationToken);

        assertSame(niFiAuthenticationToken, provider.authenticate(authenticationToken));
        verify(jwtAuthenticationConverter).convertWithClientAddress(jwt, CLIENT_ADDRESS);
        verify(niFiAuthenticationToken).setDetails(webAuthenticationDetails);
    }
}
