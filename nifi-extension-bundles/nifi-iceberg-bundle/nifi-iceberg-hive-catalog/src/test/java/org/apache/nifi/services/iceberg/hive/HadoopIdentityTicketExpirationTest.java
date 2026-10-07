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
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Runs Hadoop Identities across the expiration of their Ticket Granting Tickets, issued by an embedded Key Distribution
 * Center with a short ticket lifetime. Requesting a service ticket with the expired credentials fails with GSSException
 * NO_CRED, while actions through the Hadoop Identity log in again before running and obtain service tickets.
 */
class HadoopIdentityTicketExpirationTest {
    private static final String KRB5_CONF_PROPERTY = "java.security.krb5.conf";

    private static final String MINIMUM_RELOGIN_SECONDS_PROPERTY = "hadoop.kerberos.min.seconds.before.relogin";

    private static final Duration TICKET_LIFETIME = Duration.ofSeconds(15);

    private static final Duration EXPIRATION_MARGIN = Duration.ofSeconds(3);

    private static final String KEYTAB_USER = "nifi-keytab";

    private static final String PASSWORD_USER = "nifi-password";

    private static final String PASSWORD = "nifi-password-secret";

    private static final String SERVICE = "hive/localhost";

    private static final String PRINCIPAL_FORMAT = "%s@%s";

    private static final String KERBEROS_MECHANISM = "1.2.840.113554.1.2.2";

    private static String krb5Configuration;

    private static MiniKdc kdc;

    private static String keytabPrincipal;

    private static String passwordPrincipal;

    private static String servicePrincipal;

    private static File keytab;

    private final List<KerberosUser> kerberosUsers = new ArrayList<>();

    private final List<HadoopIdentity> identities = new ArrayList<>();

    @BeforeAll
    static void startKeyDistributionCenter(@TempDir final Path directory) throws Exception {
        krb5Configuration = System.getProperty(KRB5_CONF_PROPERTY);

        final Properties properties = MiniKdc.createConf();
        properties.setProperty(MiniKdc.ORG_NAME, "NIFI");
        properties.setProperty(MiniKdc.ORG_DOMAIN, "COM");
        properties.setProperty(MiniKdc.MIN_TICKET_LIFETIME, "1");
        properties.setProperty(MiniKdc.MAX_TICKET_LIFETIME, Long.toString(TICKET_LIFETIME.toSeconds()));
        properties.setProperty(MiniKdc.MAX_RENEWABLE_LIFETIME, Long.toString(TICKET_LIFETIME.toSeconds()));

        kdc = new MiniKdc(properties, directory.toFile());
        kdc.start();
        System.setProperty(KRB5_CONF_PROPERTY, kdc.getKrb5conf().getAbsolutePath());

        keytab = directory.resolve("nifi.keytab").toFile();
        kdc.createPrincipal(keytab, KEYTAB_USER, SERVICE);
        keytabPrincipal = PRINCIPAL_FORMAT.formatted(KEYTAB_USER, kdc.getRealm());
        servicePrincipal = PRINCIPAL_FORMAT.formatted(SERVICE, kdc.getRealm());

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
    void testActionsContinueAfterTicketExpiration() throws Exception {
        final HadoopIdentity keytabIdentity = login(new KerberosKeytabUser(keytabPrincipal, keytab.getAbsolutePath()));
        final HadoopIdentity passwordIdentity = login(new KerberosPasswordUser(passwordPrincipal, PASSWORD));
        assertNotNull(keytabIdentity.doAs(HadoopIdentityTicketExpirationTest::getServiceToken));
        assertNotNull(passwordIdentity.doAs(HadoopIdentityTicketExpirationTest::getServiceToken));

        Thread.sleep(TICKET_LIFETIME.plus(EXPIRATION_MARGIN).toMillis());

        assertCredentialsNotFound(keytabIdentity);
        assertCredentialsNotFound(passwordIdentity);
        assertNotNull(keytabIdentity.doAs(HadoopIdentityTicketExpirationTest::getServiceToken));
        assertNotNull(passwordIdentity.doAs(HadoopIdentityTicketExpirationTest::getServiceToken));
    }

    private void assertCredentialsNotFound(final HadoopIdentity identity) {
        final PrivilegedAction<byte[]> action = HadoopIdentityTicketExpirationTest::getServiceToken;
        final IllegalStateException exception = assertThrows(IllegalStateException.class, () -> identity.getUserGroupInformation().doAs(action));
        final GSSException cause = (GSSException) exception.getCause();
        assertEquals(GSSException.NO_CRED, cause.getMajor(), cause::getMessage);
    }

    private HadoopIdentity login(final KerberosUser kerberosUser) throws IOException {
        kerberosUsers.add(kerberosUser);

        final Configuration configuration = new Configuration();
        configuration.set(SecurityUtil.HADOOP_SECURITY_AUTHENTICATION, SecurityUtil.KERBEROS);
        configuration.set(MINIMUM_RELOGIN_SECONDS_PROPERTY, "1");

        final HadoopIdentity identity = HadoopIdentity.login(configuration, kerberosUser, mock(ComponentLog.class));
        identities.add(identity);
        return identity;
    }

    private static byte[] getServiceToken() {
        try {
            final GSSManager manager = GSSManager.getInstance();
            final GSSName serviceName = manager.createName(servicePrincipal, GSSName.NT_USER_NAME);
            final GSSContext context = manager.createContext(serviceName, new Oid(KERBEROS_MECHANISM), null, GSSContext.DEFAULT_LIFETIME);
            try {
                return context.initSecContext(new byte[0], 0, 0);
            } finally {
                context.dispose();
            }
        } catch (final GSSException e) {
            throw new IllegalStateException("Service ticket request failed", e);
        }
    }
}
