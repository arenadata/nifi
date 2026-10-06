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
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosPasswordUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Runs the Hadoop Identity against an embedded Kerberos Key Distribution Center with real logins. Keytab users log in
 * through Hadoop, which the Hadoop KMS client requires, and password users through their Subject. Both keep their own
 * identity in one process without changing the process-wide login user, and closing one identity leaves the others.
 * The realm matches the default realm of the krb5.conf configured for tests, which the JDK caches on first use.
 */
class HadoopIdentityKerberosTest {
    private static final String KRB5_CONF_PROPERTY = "java.security.krb5.conf";

    private static final String FIRST_USER = "nifi-first";

    private static final String SECOND_USER = "nifi-second";

    private static final String PASSWORD_USER = "nifi-password";

    private static final String PASSWORD = "nifi-password-secret";

    private static final String PRINCIPAL_FORMAT = "%s@%s";

    private static final String KEYTAB_FORMAT = "%s.keytab";

    private static String krb5Configuration;

    private static MiniKdc kdc;

    private static String firstPrincipal;

    private static String secondPrincipal;

    private static String passwordPrincipal;

    private static File firstKeytab;

    private static File secondKeytab;

    private final List<KerberosUser> kerberosUsers = new ArrayList<>();

    private final List<HadoopIdentity> identities = new ArrayList<>();

    @BeforeAll
    static void startKeyDistributionCenter(@TempDir final Path directory) throws Exception {
        krb5Configuration = System.getProperty(KRB5_CONF_PROPERTY);

        final Properties properties = MiniKdc.createConf();
        properties.setProperty(MiniKdc.ORG_NAME, "NIFI");
        properties.setProperty(MiniKdc.ORG_DOMAIN, "COM");

        kdc = new MiniKdc(properties, directory.toFile());
        kdc.start();
        System.setProperty(KRB5_CONF_PROPERTY, kdc.getKrb5conf().getAbsolutePath());

        firstKeytab = directory.resolve(KEYTAB_FORMAT.formatted(FIRST_USER)).toFile();
        kdc.createPrincipal(firstKeytab, FIRST_USER);
        firstPrincipal = PRINCIPAL_FORMAT.formatted(FIRST_USER, kdc.getRealm());

        secondKeytab = directory.resolve(KEYTAB_FORMAT.formatted(SECOND_USER)).toFile();
        kdc.createPrincipal(secondKeytab, SECOND_USER);
        secondPrincipal = PRINCIPAL_FORMAT.formatted(SECOND_USER, kdc.getRealm());

        kdc.createPrincipal(PASSWORD_USER, PASSWORD);
        passwordPrincipal = PRINCIPAL_FORMAT.formatted(PASSWORD_USER, kdc.getRealm());
    }

    @AfterAll
    static void stopKeyDistributionCenter() {
        UserGroupInformation.reset();
        if (krb5Configuration == null) {
            System.clearProperty(KRB5_CONF_PROPERTY);
        } else {
            System.setProperty(KRB5_CONF_PROPERTY, krb5Configuration);
        }

        if (kdc != null) {
            kdc.stop();
        }
    }

    @AfterEach
    void closeIdentities() {
        identities.forEach(HadoopIdentity::close);
        kerberosUsers.stream().filter(KerberosUser::isLoggedIn).forEach(KerberosUser::logout);
    }

    @Test
    void testKeytabUserLoggedInThroughHadoop() throws IOException {
        final HadoopIdentity identity = login(new KerberosKeytabUser(firstPrincipal, firstKeytab.getAbsolutePath()));

        final UserGroupInformation userGroupInformation = identity.getUserGroupInformation();
        assertTrue(userGroupInformation.isFromKeytab());
        assertTrue(userGroupInformation.shouldRelogin());
        assertEquals(firstPrincipal, identity.doAs(HadoopIdentityKerberosTest::getCurrentUserName));
    }

    @Test
    void testPrincipalsKeepIdentityWithoutChangingLoginUser() throws IOException {
        final HadoopIdentity first = login(new KerberosKeytabUser(firstPrincipal, firstKeytab.getAbsolutePath()));
        final HadoopIdentity second = login(new KerberosKeytabUser(secondPrincipal, secondKeytab.getAbsolutePath()));
        final HadoopIdentity password = login(new KerberosPasswordUser(passwordPrincipal, PASSWORD));

        assertEquals(firstPrincipal, first.doAs(HadoopIdentityKerberosTest::getCurrentUserName));
        assertEquals(secondPrincipal, second.doAs(HadoopIdentityKerberosTest::getCurrentUserName));
        assertEquals(passwordPrincipal, password.doAs(HadoopIdentityKerberosTest::getCurrentUserName));

        final String loginUserName = UserGroupInformation.getLoginUser().getUserName();
        assertNotEquals(firstPrincipal, loginUserName);
        assertNotEquals(secondPrincipal, loginUserName);
        assertNotEquals(passwordPrincipal, loginUserName);
    }

    @Test
    void testCloseLogsOutOnlyClosedIdentity() throws IOException {
        final HadoopIdentity first = login(new KerberosKeytabUser(firstPrincipal, firstKeytab.getAbsolutePath()));
        final HadoopIdentity second = login(new KerberosKeytabUser(secondPrincipal, secondKeytab.getAbsolutePath()));

        first.close();

        assertTrue(getTickets(first).isEmpty());
        assertFalse(getTickets(second).isEmpty());
        assertEquals(secondPrincipal, second.doAs(HadoopIdentityKerberosTest::getCurrentUserName));
    }

    @Test
    void testPasswordUserRecoveredBeforeActionAfterLogout() throws IOException {
        final KerberosPasswordUser kerberosUser = new KerberosPasswordUser(passwordPrincipal, PASSWORD);
        final HadoopIdentity identity = login(kerberosUser);

        kerberosUser.logout();
        assertFalse(kerberosUser.isLoggedIn());

        assertEquals(passwordPrincipal, identity.doAs(HadoopIdentityKerberosTest::getCurrentUserName));
        assertTrue(kerberosUser.isLoggedIn());
    }

    @Test
    void testPasswordUserLoggedOutOnClose() throws IOException {
        final KerberosPasswordUser kerberosUser = new KerberosPasswordUser(passwordPrincipal, PASSWORD);
        final HadoopIdentity identity = login(kerberosUser);

        identity.close();

        assertFalse(kerberosUser.isLoggedIn());
    }

    private HadoopIdentity login(final KerberosUser kerberosUser) throws IOException {
        kerberosUsers.add(kerberosUser);

        final Configuration configuration = new Configuration();
        configuration.set(SecurityUtil.HADOOP_SECURITY_AUTHENTICATION, SecurityUtil.KERBEROS);

        final HadoopIdentity identity = HadoopIdentity.login(configuration, kerberosUser, mock(ComponentLog.class));
        identities.add(identity);
        return identity;
    }

    private static Set<KerberosTicket> getTickets(final HadoopIdentity identity) {
        final Subject subject = identity.getUserGroupInformation().doAs((PrivilegedAction<Subject>) Subject::current);
        return subject.getPrivateCredentials(KerberosTicket.class);
    }

    private static String getCurrentUserName() {
        try {
            return UserGroupInformation.getCurrentUser().getUserName();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
