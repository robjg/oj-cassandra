package org.oddjob.cassandra;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Ripped off from CassandraUnit.
 */
public class CassandraEmbedded {

    private static final Logger log = LoggerFactory.getLogger(CassandraEmbedded.class);

    public static final long DEFAULT_STARTUP_TIMEOUT = 20000;
    public static final String DEFAULT_TMP_DIR = "target/embeddedCassandra";
    /** Default configuration file. Starts embedded cassandra under the well known ports */
    public static final String DEFAULT_CASSANDRA_YML_FILE = "cu-cassandra.yaml";
    /** Configuration file which starts the embedded cassandra on a random free port */
    public static final String CASSANDRA_RNDPORT_YML_FILE = "cu-cassandra-rndport.yaml";
    public static final String DEFAULT_LOG4J_CONFIG_FILE = "/log4j-embedded-cassandra.properties";
    private static final String INTERNAL_CASSANDRA_KEYSPACE = "system";
    private static final String INTERNAL_CASSANDRA_AUTH_KEYSPACE = "system_auth";
    private static final String INTERNAL_CASSANDRA_DISTRIBUTED_KEYSPACE = "system_distributed";
    private static final String INTERNAL_CASSANDRA_SCHEMA_KEYSPACE = "system_schema";
    private static final String INTERNAL_CASSANDRA_TRACES_KEYSPACE = "system_traces";

    private static final Set<String> systemKeyspaces = new HashSet<>(Arrays.asList(INTERNAL_CASSANDRA_KEYSPACE,
            INTERNAL_CASSANDRA_AUTH_KEYSPACE, INTERNAL_CASSANDRA_DISTRIBUTED_KEYSPACE,
            INTERNAL_CASSANDRA_SCHEMA_KEYSPACE, INTERNAL_CASSANDRA_TRACES_KEYSPACE));

    public static Predicate<String> nonSystemKeyspaces() {
        return keyspace -> !systemKeyspaces.contains(keyspace);
    }

    private static CassandraDaemon cassandraDaemon = null;
    private static String launchedYamlFile;


    private File configFile;

    /**
     * Set embedded cassandra up and spawn it in a new thread.
     *
     * @throws ConfigurationException
     */

    public void start() throws ConfigurationException {
        if (cassandraDaemon != null) {
            /* nothing to do Cassandra is already started */
            return;
        }

        File file = Objects.requireNonNull(this.configFile);

        String tmpDir = DEFAULT_TMP_DIR;

        long timeout = DEFAULT_STARTUP_TIMEOUT;

        checkConfigNameForRestart(file.getAbsolutePath());

        log.debug("Starting cassandra...");
        log.debug("Initialization needed");

        String cassandraConfigFilePath = file.getAbsolutePath();
        cassandraConfigFilePath = (cassandraConfigFilePath.startsWith("/") ? "file://" : "file:/") + cassandraConfigFilePath;
        System.setProperty("cassandra.config", cassandraConfigFilePath);
        System.setProperty("cassandra-foreground", "true");
        System.setProperty("cassandra.native.epoll.enabled", "false"); // JNA doesnt cope with relocated netty
        System.setProperty("cassandra.unsafesystem", "true"); // disable fsync for a massive speedup on old platters

        DatabaseDescriptor.daemonInitialization();

            cassandraDaemon = new CassandraDaemon(true);
            cassandraDaemon.activate();
    }

    private static void checkConfigNameForRestart(String yamlFile) {
        boolean wasPreviouslyLaunched = launchedYamlFile != null;
        if (wasPreviouslyLaunched && !launchedYamlFile.equals(yamlFile)) {
            throw new UnsupportedOperationException("We can't launch two Cassandra configurations in the same JVM instance");
        }
        launchedYamlFile = yamlFile;
    }

    /**
     */
    public void stop() {
        cassandraDaemon.stop();
        cassandraDaemon.destroy();

    }

    public File getConfigFile() {
        return configFile;
    }

    public void setConfigFile(File configFile) {
        this.configFile = configFile;
    }

    /**
     * Get the embedded cassandra cluster name
     *
     * @return the cluster name
     */
    public String getClusterName() {
        return DatabaseDescriptor.getClusterName();
    }

    /**
     * Get embedded cassandra host.
     *
     * @return the cassandra host
     */
    public String getHost() {
        return DatabaseDescriptor.getRpcAddress().getHostName();
    }

    /**
     * Get embedded cassandra RPC port.
     *
     * @return the cassandra RPC port
     */
    public int getRpcPort() {
        return DatabaseDescriptor.getRpcPort();
    }

    /**
     * Get embedded cassandra native transport port.
     *
     * @return the cassandra native transport port.
     */
    public int getNativeTransportPort() {
        return DatabaseDescriptor.getNativeTransportPort();
    }

    private static int findUnusedLocalPort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }


}
