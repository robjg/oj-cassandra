package org.oddjob.cassandra;


import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

@Disabled("To Fix")
public class CrudTest {

    @Test
    public void test() throws IOException {

        CassandraEmbedded cassie = new CassandraEmbedded();
        cassie.setConfigFile(new File(getClass().getResource("/cu-cassandra.yaml").getFile()));

        cassie.start();

        CassandraSessionService sessionService = new CassandraSessionService();
        sessionService.setPort(cassie.getNativeTransportPort());
        sessionService.setNode("localhost");
        sessionService.start();

        sessionService.getSession().execute("select * from system.local");

        sessionService.stop();

        cassie.stop();
    }
}
