package org.oddjob.cassandra;

import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @oddjob.description Run CQL.
 */
public class CqlJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CqlJob.class);

    private String name;

    private Session session;

    private String cql;

    private InputStream input;

    @Override
    public void run() {

        Session session = Objects.requireNonNull(this.session, "No session");

        Reader input = Optional.ofNullable(this.input)
                .<Reader>map(InputStreamReader::new)
                .orElseGet(() -> Optional.ofNullable(this.cql)
                        .map(StringReader::new)
                        .orElseThrow(() -> new IllegalArgumentException("No Input")));

       List<String> statements = linesToCQLStatements(
               new BufferedReader(input).lines().collect(Collectors.toList()));

        for (String stmt : statements) {
            logger.info(("Executing: {}"), stmt);

            session.execute(stmt);
        }
    }

    private List<String> linesToCQLStatements(List<String> lines) {
        SimpleCQLLexer lexer = new SimpleCQLLexer(lines);
        return lexer.getStatements();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public String getCql() {
        return cql;
    }

    public void setCql(String cql) {
        this.cql = cql;
    }

    public InputStream getInput() {
        return input;
    }

    public void setInput(InputStream input) {
        this.input = input;
    }

    @Override
    public String toString() {
        return Optional.ofNullable(this.name)
                .orElse(getClass().getSimpleName());
    }
}

