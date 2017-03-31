/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.client;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.sun.security.auth.module.Krb5LoginModule;
import io.airlift.units.Duration;
import okhttp3.Authenticator;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;

import javax.annotation.concurrent.GuardedBy;
import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.base.CharMatcher.whitespace;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static java.lang.Boolean.getBoolean;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;
import static org.ietf.jgss.GSSContext.INDEFINITE_LIFETIME;
import static org.ietf.jgss.GSSCredential.DEFAULT_LIFETIME;
import static org.ietf.jgss.GSSCredential.INITIATE_ONLY;
import static org.ietf.jgss.GSSName.NT_HOSTBASED_SERVICE;
import static org.ietf.jgss.GSSName.NT_USER_NAME;

public class SpnegoAuthenticator
        implements Authenticator
{
    private static final String NEGOTIATE = "Negotiate";
    private static final Duration MIN_CREDENTIAL_LIFETIME = new Duration(60, SECONDS);

    private static final GSSManager GSS_MANAGER = GSSManager.getInstance();

    private static final Oid SPNEGO_OID = createOid("1.3.6.1.5.5.2");
    private static final Oid KERBEROS_OID = createOid("1.2.840.113554.1.2.2");

    private final Optional<File> keytab;
    private final Optional<File> credentialCache;
    private final Optional<String> principal;
    private final String remoteServiceName;
    private final boolean useCanonicalHostname;

    @GuardedBy("this")
    private Session clientSession;

    public SpnegoAuthenticator(
            Optional<File> kerberosConfig,
            Optional<File> keytab,
            Optional<File> credentialCache,
            Optional<String> principal,
            String remoteServiceName,
            boolean useCanonicalHostname)
    {
        this.keytab = requireNonNull(keytab, "keytab is null");
        this.credentialCache = requireNonNull(credentialCache, "credentialCache is null");
        this.principal = requireNonNull(principal, "principal is null");
        this.remoteServiceName = requireNonNull(remoteServiceName, "remoteServiceName is null");
        this.useCanonicalHostname = useCanonicalHostname;

        kerberosConfig.ifPresent(file -> System.setProperty("java.security.krb5.conf", file.getAbsolutePath()));
    }

    @Override
    public Request authenticate(Route route, Response response)
            throws IOException
    {
        // skip if we already tried or were not asked for Kerberos
        if (response.request().headers(AUTHORIZATION).stream().anyMatch(SpnegoAuthenticator::isNegotiate) ||
                response.headers(WWW_AUTHENTICATE).stream().noneMatch(SpnegoAuthenticator::isNegotiate)) {
            return null;
        }

        byte[] token = generateToken(response.request().url().host());
        String credential = format("%s %s", NEGOTIATE, Base64.getEncoder().encodeToString(token));
        return response.request().newBuilder()
                .header(AUTHORIZATION, credential)
                .build();
    }

    private static boolean isNegotiate(String value)
    {
        return Splitter.on(whitespace()).split(value).iterator().next().equalsIgnoreCase(NEGOTIATE);
    }

    private byte[] generateToken(String hostName)
    {
        String servicePrincipal = makeServicePrincipal(remoteServiceName, hostName, useCanonicalHostname);

        GSSContext context = null;
        try {
            Session session = getSession();
            context = doAs(session.getLoginContext().getSubject(), () -> {
                GSSContext result = GSS_MANAGER.createContext(
                        GSS_MANAGER.createName(servicePrincipal, NT_HOSTBASED_SERVICE),
                        SPNEGO_OID,
                        session.getClientCredential(),
                        INDEFINITE_LIFETIME);

                result.requestMutualAuth(true);
                result.requestConf(true);
                result.requestInteg(true);
                result.requestCredDeleg(false);
                return result;
            });

            byte[] token = context.initSecContext(new byte[0], 0, 0);
            if (token != null) {
                return token;
            }
            throw new LoginException("No token generated from GSS context");
        }
        catch (GSSException | LoginException e) {
            throw new ClientException(format("Kerberos error for [%s]: %s", servicePrincipal, e.getMessage()), e);
        }
        finally {
            try {
                if (context != null) {
                    context.dispose();
                }
            }
            catch (GSSException ignored) {
            }
        }
    }

    private synchronized Session getSession()
            throws LoginException, GSSException
    {
        if ((clientSession == null) || clientSession.needsRefresh()) {
            clientSession = createSession();
        }
        return clientSession;
    }

    private Session createSession()
            throws LoginException, GSSException
    {
        // TODO: do we need to call logout() on the LoginContext?

        LoginContext loginContext = new LoginContext("", null, null, new Configuration()
        {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name)
            {
                ImmutableMap.Builder<String, String> options = ImmutableMap.builder();
                options.put("refreshKrb5Config", "true");
                options.put("doNotPrompt", "true");
                options.put("useKeyTab", "true");

                if (getBoolean("presto.client.debugKerberos")) {
                    options.put("debug", "true");
                }

                keytab.ifPresent(file -> options.put("keyTab", file.getAbsolutePath()));

                credentialCache.ifPresent(file -> {
                    options.put("ticketCache", file.getAbsolutePath());
                    options.put("useTicketCache", "true");
                    options.put("renewTGT", "true");
                });

                principal.ifPresent(value -> options.put("principal", value));

                return new AppConfigurationEntry[] {
                        new AppConfigurationEntry(Krb5LoginModule.class.getName(), REQUIRED, options.build())
                };
            }
        });

        loginContext.login();
        Subject subject = loginContext.getSubject();
        Principal clientPrincipal = subject.getPrincipals().iterator().next();
        GSSCredential clientCredential = doAs(subject, () -> GSS_MANAGER.createCredential(
                GSS_MANAGER.createName(clientPrincipal.getName(), NT_USER_NAME),
                DEFAULT_LIFETIME,
                KERBEROS_OID,
                INITIATE_ONLY));

        return new Session(loginContext, clientCredential);
    }

    private static String makeServicePrincipal(String serviceName, String hostName, boolean useCanonicalHostname)
    {
        String serviceHostName = hostName;
        if (useCanonicalHostname) {
            serviceHostName = canonicalizeServiceHostName(hostName);
        }
        return format("%s@%s", serviceName, serviceHostName.toLowerCase(Locale.US));
    }

    private static String canonicalizeServiceHostName(String hostName)
    {
        try {
            InetAddress address = InetAddress.getByName(hostName);
            String fullHostName;
            if ("localhost".equalsIgnoreCase(address.getHostName())) {
                fullHostName = InetAddress.getLocalHost().getCanonicalHostName();
            }
            else {
                fullHostName = address.getCanonicalHostName();
            }
            if (fullHostName.equalsIgnoreCase("localhost")) {
                throw new ClientException("Fully qualified name of localhost should not resolve to 'localhost'. System configuration error?");
            }
            return fullHostName;
        }
        catch (UnknownHostException e) {
            throw new ClientException("Failed to resolve host: " + hostName, e);
        }
    }

    private interface GssSupplier<T>
    {
        T get()
                throws GSSException;
    }

    private static <T> T doAs(Subject subject, GssSupplier<T> action)
            throws GSSException
    {
        try {
            return Subject.doAs(subject, (PrivilegedExceptionAction<T>) action::get);
        }
        catch (PrivilegedActionException e) {
            Throwable t = e.getCause();
            throwIfInstanceOf(t, GSSException.class);
            throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }

    private static Oid createOid(String value)
    {
        try {
            return new Oid(value);
        }
        catch (GSSException e) {
            throw new AssertionError(e);
        }
    }

    private static class Session
    {
        private final LoginContext loginContext;
        private final GSSCredential clientCredential;

        public Session(LoginContext loginContext, GSSCredential clientCredential)
                throws LoginException
        {
            requireNonNull(loginContext, "loginContext is null");
            requireNonNull(clientCredential, "gssCredential is null");

            this.loginContext = loginContext;
            this.clientCredential = clientCredential;
        }

        public LoginContext getLoginContext()
        {
            return loginContext;
        }

        public GSSCredential getClientCredential()
        {
            return clientCredential;
        }

        public boolean needsRefresh()
                throws GSSException
        {
            return clientCredential.getRemainingLifetime() < MIN_CREDENTIAL_LIFETIME.getValue(SECONDS);
        }
    }
}
