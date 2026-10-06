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
package org.apache.nifi.services.iceberg.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Covers the Hadoop Identity without a Key Distribution Center: the identity used for actions, File System caching per
 * identity, and Kerberos User handling with a mocked Kerberos User.
 */
class HadoopIdentityTest {
    private static final URI LOCAL_FILE_SYSTEM = URI.create("file:///");

    private static final String KERBEROS_USER_NAME = "nifi";

    private final ComponentLog logger = mock(ComponentLog.class);

    private final List<HadoopIdentity> identities = new ArrayList<>();

    @AfterEach
    void closeIdentities() {
        identities.forEach(HadoopIdentity::close);
    }

    @Test
    void testDoAsRunsAsIdentityWithCurrentUserName() throws IOException {
        final HadoopIdentity identity = login(null);

        final UserGroupInformation actionUser = identity.doAs(HadoopIdentityTest::getCurrentUser);

        assertEquals(identity.getUserGroupInformation(), actionUser);
        assertEquals(UserGroupInformation.getCurrentUser().getShortUserName(), actionUser.getShortUserName());
    }

    @Test
    void testFileSystemsCachedPerIdentity() throws IOException {
        final HadoopIdentity first = login(null);
        final HadoopIdentity second = login(null);

        final FileSystem firstFileSystem = first.doAs(HadoopIdentityTest::getLocalFileSystem);

        assertSame(firstFileSystem, first.doAs(HadoopIdentityTest::getLocalFileSystem));
        assertNotSame(firstFileSystem, second.doAs(HadoopIdentityTest::getLocalFileSystem));
    }

    @Test
    void testCloseRemovesCachedFileSystemsForIdentityOnly() throws IOException {
        final HadoopIdentity first = login(null);
        final HadoopIdentity second = login(null);
        final FileSystem firstFileSystem = first.doAs(HadoopIdentityTest::getLocalFileSystem);
        final FileSystem secondFileSystem = second.doAs(HadoopIdentityTest::getLocalFileSystem);

        first.close();

        assertNotSame(firstFileSystem, first.doAs(HadoopIdentityTest::getLocalFileSystem));
        assertSame(secondFileSystem, second.doAs(HadoopIdentityTest::getLocalFileSystem));
    }

    @Test
    void testKerberosLoginChecksTicketBeforeAction() throws IOException {
        final KerberosUser kerberosUser = getKerberosUser();
        final HadoopIdentity identity = login(kerberosUser);

        identity.doAs(HadoopIdentityTest::getCurrentUser);
        identity.checkLogin();

        verify(kerberosUser).login();
        verify(kerberosUser, times(2)).checkTGTAndRelogin();
    }

    @Test
    void testKerberosUserLoggedOutOnClose() throws IOException {
        final KerberosUser kerberosUser = getKerberosUser();
        final HadoopIdentity identity = login(kerberosUser);

        identity.close();

        verify(kerberosUser).logout();
    }

    @Test
    void testKerberosUserLoggedOutOnLoginFailure() {
        final KerberosUser kerberosUser = mock(KerberosUser.class);
        doThrow(new KerberosLoginException("Login failed")).when(kerberosUser).login();

        assertThrows(KerberosLoginException.class, () -> login(kerberosUser));

        verify(kerberosUser).logout();
    }

    @Test
    void testCloseCompletedWhenLogoutFails() throws IOException {
        final KerberosUser kerberosUser = getKerberosUser();
        doThrow(new KerberosLoginException("Logout failed")).when(kerberosUser).logout();
        final HadoopIdentity identity = login(kerberosUser);

        identity.close();

        verify(kerberosUser).logout();
    }

    private HadoopIdentity login(final KerberosUser kerberosUser) throws IOException {
        final HadoopIdentity identity = HadoopIdentity.login(new Configuration(false), kerberosUser, logger);
        identities.add(identity);
        return identity;
    }

    private static KerberosUser getKerberosUser() {
        final KerberosUser kerberosUser = mock(KerberosUser.class);
        try {
            doReturn(UserGroupInformation.createRemoteUser(KERBEROS_USER_NAME)).when(kerberosUser).doAs(any(PrivilegedExceptionAction.class));
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
        return kerberosUser;
    }

    private static UserGroupInformation getCurrentUser() {
        try {
            return UserGroupInformation.getCurrentUser();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static FileSystem getLocalFileSystem() {
        try {
            return FileSystem.get(LOCAL_FILE_SYSTEM, new Configuration());
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
