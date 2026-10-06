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
import org.apache.nifi.security.krb.KerberosUser;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;

/**
 * Hadoop identity of one Controller Service instance. Hive Metastore connections and Hadoop File Systems are created as
 * this identity instead of the process-wide login user, so instances with different principals do not affect each other,
 * and File Systems cached for the identity are closed together with it. The Kerberos Ticket Granting Ticket is refreshed
 * before each privileged action. Keytab users log in through Hadoop, because the Hadoop KMS client accepts only a
 * Hadoop login for encryption zones and falls back to the process-wide login user otherwise. Other Kerberos users are
 * used through their Subject.
 */
final class HadoopIdentity implements Closeable {
    private static final String KEYTAB_OPTION = "keyTab";

    private static final String USE_KEYTAB_OPTION = "useKeyTab";

    private final UserGroupInformation userGroupInformation;

    private final KerberosUser kerberosUser;

    private final ComponentLog logger;

    private HadoopIdentity(final UserGroupInformation userGroupInformation, final KerberosUser kerberosUser, final ComponentLog logger) {
        this.userGroupInformation = userGroupInformation;
        this.kerberosUser = kerberosUser;
        this.logger = logger;
    }

    /**
     * Create identity for the Kerberos User or for the current user name without Kerberos
     *
     * @param configuration Hadoop Configuration applied to User Group Information when using Kerberos
     * @param kerberosUser Kerberos User or null when Kerberos is not configured
     * @param logger Component Log
     * @return Hadoop Identity
     * @throws IOException Thrown on Kerberos login failures
     */
    static HadoopIdentity login(final Configuration configuration, final KerberosUser kerberosUser, final ComponentLog logger) throws IOException {
        if (kerberosUser == null) {
            final String userName = UserGroupInformation.getCurrentUser().getShortUserName();
            return new HadoopIdentity(UserGroupInformation.createRemoteUser(userName), null, logger);
        }

        // Authentication method is process-wide in Hadoop, so all instances in one process need the same setting
        final Configuration securityConfiguration = new Configuration(configuration);
        // Hadoop keeps the Configuration in a static field, which must not reference the instance ClassLoader
        securityConfiguration.setClassLoader(Configuration.class.getClassLoader());
        UserGroupInformation.setConfiguration(securityConfiguration);

        final String keytab = getKeytab(kerberosUser);
        if (keytab != null) {
            final UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(kerberosUser.getPrincipal(), keytab);
            return new HadoopIdentity(ugi, null, logger);
        }

        try {
            kerberosUser.login();
            final UserGroupInformation ugi = kerberosUser.doAs((PrivilegedExceptionAction<UserGroupInformation>) () -> UserGroupInformation.getUGIFromSubject(Subject.current()));
            return new HadoopIdentity(ugi, kerberosUser, logger);
        } catch (final PrivilegedActionException e) {
            logout(kerberosUser, logger);
            throw new IOException("Kerberos login failed for [%s]".formatted(kerberosUser.getPrincipal()), e.getException());
        } catch (final RuntimeException e) {
            logout(kerberosUser, logger);
            throw e;
        }
    }

    /**
     * Run action as this identity after refreshing the Kerberos Ticket Granting Ticket when required
     *
     * @param action Action to run
     * @return Action result
     * @param <T> Result type
     */
    <T> T doAs(final PrivilegedAction<T> action) {
        checkLogin();
        return userGroupInformation.doAs(action);
    }

    /**
     * Refresh the Kerberos Ticket Granting Ticket when close to expiration, which is a time comparison until then
     */
    void checkLogin() {
        if (kerberosUser != null) {
            kerberosUser.checkTGTAndRelogin();
        } else if (userGroupInformation.isFromKeytab()) {
            try {
                userGroupInformation.checkTGTAndReloginFromKeytab();
            } catch (final IOException e) {
                throw new UncheckedIOException("Kerberos login failed for [%s]".formatted(userGroupInformation.getUserName()), e);
            }
        }
    }

    UserGroupInformation getUserGroupInformation() {
        return userGroupInformation;
    }

    @Override
    public void close() {
        try {
            FileSystem.closeAllForUGI(userGroupInformation);
        } catch (final IOException e) {
            logger.warn("Close File Systems failed for [{}]", userGroupInformation.getUserName(), e);
        } finally {
            if (kerberosUser != null) {
                logout(kerberosUser, logger);
            } else if (userGroupInformation.isFromKeytab()) {
                try {
                    userGroupInformation.logoutUserFromKeytab();
                } catch (final IOException e) {
                    logger.warn("Kerberos logout failed for [{}]", userGroupInformation.getUserName(), e);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "%s[%s]".formatted(getClass().getSimpleName(), userGroupInformation.getUserName());
    }

    // Kerberos User implementations belong to the Kerberos User Service bundle, so the keytab is read from the login configuration
    private static String getKeytab(final KerberosUser kerberosUser) {
        final AppConfigurationEntry configurationEntry = kerberosUser.getConfigurationEntry();
        if (configurationEntry == null) {
            return null;
        }

        final Map<String, ?> options = configurationEntry.getOptions();
        final boolean useKeytab = Boolean.parseBoolean(String.valueOf(options.get(USE_KEYTAB_OPTION)));
        return useKeytab && options.get(KEYTAB_OPTION) instanceof String keytab ? keytab : null;
    }

    private static void logout(final KerberosUser kerberosUser, final ComponentLog logger) {
        try {
            kerberosUser.logout();
        } catch (final RuntimeException e) {
            logger.warn("Kerberos User logout failed for [{}]", kerberosUser.getPrincipal(), e);
        }
    }
}
