package org.oddjob.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * @oddjob.description Creates a Cassandra Session.
 */
public class CassandraSessionService {

    private static final Logger logger = LoggerFactory.getLogger(CassandraSessionService.class);

    private String name;

    private String node;

    private int port;

    private int readTimeout;

    private Cluster cluster;

    private Session session;

    public void start() {

        if (cluster != null) {
            throw new IllegalStateException("Started already");
        }

        String node = Optional.ofNullable(this.node)
                .orElseThrow(() -> new IllegalArgumentException("No node."));

        Cluster.Builder builder = Cluster.builder()
                .withoutJMXReporting()
                .addContactPoint(node);

        int port = this.port;
        if (port > 0) {
            builder = builder.withPort(port);
        }

        int readTimeout = this.readTimeout;
        if (readTimeout > 0) {
            builder = builder.withSocketOptions(
                new SocketOptions().setReadTimeoutMillis(readTimeout));
        }

        this.cluster = builder.build();

        logger.info("Connecting to " + this.cluster.getClusterName());

        try {
            this.session = cluster.connect();
        } catch (RuntimeException | Error throwable) {
            try {
                this.cluster.close();
            }
            finally {
                this.cluster  = null;
            }
            throw throwable;
        }
    }

    public void stop() {
        if (cluster == null) {
            throw new IllegalStateException("Not started.");
        }

        logger.info("Closing session and cluster" + this.cluster.getClusterName());

        try {
            Optional.ofNullable(session).ifPresent(Session::close);
            session = null;
        }
        finally {
            cluster.close();
            cluster = null;
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Session getSession() {
        return session;
    }

    @Override
    public String toString() {
        return Optional.ofNullable(name).orElseGet(() -> getClass().getSimpleName());
    }
}
