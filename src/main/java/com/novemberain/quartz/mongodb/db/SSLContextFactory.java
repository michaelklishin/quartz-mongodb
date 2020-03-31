package com.novemberain.quartz.mongodb.db;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;

/**
 * Allows to get an {@link SSLContext} from a Java truststore and keystore.
 */
public class SSLContextFactory {

    public SSLContext getSSLContext(String trustStorePath, String trustStorePassword, String trustStoreType,
            String keyStorePath, String keyStorePassword, String keyStoreType) throws SSLException {
        try {
            KeyStore trustStore = loadKeyStore(trustStorePath, trustStorePassword, trustStoreType);
            KeyStore keyStore = loadKeyStore(keyStorePath, keyStorePassword, keyStoreType);
            if (trustStore == null && keyStore == null) {
                return null;
            }
            SSLContextBuilder sslContextBuilder = SSLContexts.custom();
            if (trustStore != null) {
                sslContextBuilder.loadTrustMaterial(trustStore, null);
            }
            if (keyStore != null) {
                sslContextBuilder.loadKeyMaterial(keyStore,
                        StringUtils.isBlank(keyStorePassword) ? null : keyStorePassword.toCharArray());
            }
            return sslContextBuilder.build();
        } catch (GeneralSecurityException | IOException e) {
            throw new SSLException("Cannot setup SSL context", e);
        }
    }

    private KeyStore loadKeyStore(String path, String password, String type)
            throws GeneralSecurityException, IOException {
        if (StringUtils.isBlank(path)) {
            return null;
        }
        KeyStore keyStore = KeyStore.getInstance(StringUtils.defaultIfBlank(type, KeyStore.getDefaultType()));
        char[] passwordChars = StringUtils.isBlank(password) ? null : password.toCharArray();
        try (InputStream is = Files.newInputStream(Paths.get(path))) {
            keyStore.load(is, passwordChars);
        }
        return keyStore;
    }

}