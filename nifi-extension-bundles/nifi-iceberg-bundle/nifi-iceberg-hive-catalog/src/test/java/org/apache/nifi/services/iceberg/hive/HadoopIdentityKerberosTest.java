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
import org.apache.iceberg.CatalogProperties;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosPasswordUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
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

    private static final String SUBJECT_CONTEXT_SUFFIX = "-subject";

    private static final String METASTORE_URI = "thrift://127.0.0.1:1";

    private static final String KEYTAB_LOGIN_RENEWAL_PROPERTY = "hadoop.kerberos.keytab.login.autorenewal.enabled";

    private static final String LOGIN_RENEWAL_EXECUTOR_FIELD = "kerberosLoginRenewalExecutor";

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

    @Test
    void testKmsWarningOnlyForUsersWithoutKeytab() throws IOException {
        final MockComponentLog keytabLog = new MockComponentLog(FIRST_USER, this);
        login(new KerberosKeytabUser(firstPrincipal, firstKeytab.getAbsolutePath()), keytabLog);
        final MockComponentLog passwordLog = new MockComponentLog(PASSWORD_USER, this);
        login(new KerberosPasswordUser(passwordPrincipal, PASSWORD), passwordLog);

        assertTrue(keytabLog.getWarnMessages().isEmpty(), keytabLog.getWarnMessages()::toString);
        assertEquals(1, passwordLog.getWarnMessages().size(), passwordLog.getWarnMessages()::toString);
    }

    @Test
    void testClientContextSeparatesKeytabAndSubjectLogins() throws IOException {
        final String keytabContext = getClientContext(login(new KerberosKeytabUser(firstPrincipal, firstKeytab.getAbsolutePath())));
        final String passwordContext = getClientContext(login(new KerberosPasswordUser(passwordPrincipal, PASSWORD)));

        assertTrue(keytabContext.endsWith(firstPrincipal), keytabContext);
        assertTrue(passwordContext.endsWith(SUBJECT_CONTEXT_SUFFIX), passwordContext);
        assertTrue(passwordContext.contains(passwordPrincipal), passwordContext);
    }

    @Test
    void testCloseKeepsLoginUserTicketRenewal() throws Exception {
        final Configuration configuration = getConfiguration();
        configuration.setBoolean(KEYTAB_LOGIN_RENEWAL_PROPERTY, true);
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab(secondPrincipal, secondKeytab.getAbsolutePath());
        try {
            final ExecutorService renewal = getLoginRenewalExecutor();
            assertFalse(renewal.isShutdown());

            final HadoopIdentity identity = login(new KerberosKeytabUser(firstPrincipal, firstKeytab.getAbsolutePath()));
            identity.close();

            assertTrue(getTickets(identity).isEmpty());
            assertSame(renewal, getLoginRenewalExecutor());
            assertFalse(renewal.isShutdown());
        } finally {
            UserGroupInformation.getLoginUser().logoutUserFromKeytab();
            UserGroupInformation.reset();
        }
    }

    @SuppressWarnings("unchecked")
    private static ExecutorService getLoginRenewalExecutor() throws ReflectiveOperationException {
        final Field field = UserGroupInformation.class.getDeclaredField(LOGIN_RENEWAL_EXECUTOR_FIELD);
        field.setAccessible(true);
        return ((Optional<ExecutorService>) field.get(null)).orElseThrow();
    }

    private HadoopIdentity login(final KerberosUser kerberosUser) throws IOException {
        return login(kerberosUser, mock(ComponentLog.class));
    }

    private static String getClientContext(final HadoopIdentity identity) throws IOException {
        try (HiveMetastoreCatalog catalog = new HiveMetastoreCatalog(identity)) {
            catalog.setConf(new Configuration(false));
            catalog.initialize(FIRST_USER, Map.of(CatalogProperties.URI, METASTORE_URI));
            return catalog.getConf().get(HiveMetastoreCatalog.CLIENT_CONTEXT_PROPERTY);
        }
    }

    private HadoopIdentity login(final KerberosUser kerberosUser, final ComponentLog componentLog) throws IOException {
        kerberosUsers.add(kerberosUser);

        final HadoopIdentity identity = HadoopIdentity.login(getConfiguration(), kerberosUser, componentLog);
        identities.add(identity);
        return identity;
    }

    private static Configuration getConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(SecurityUtil.HADOOP_SECURITY_AUTHENTICATION, SecurityUtil.KERBEROS);
        return configuration;
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
