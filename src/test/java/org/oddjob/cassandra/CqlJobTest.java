package org.oddjob.cassandra;

import com.datastax.driver.core.Session;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.times;

class CqlJobTest {


    @Test
    public void test() {

        Session session = Mockito.mock(Session.class);

        CqlJob cqlJob = new CqlJob();

        cqlJob.setSession(session);
        cqlJob.setCql("foo;\n" +
                "more " +
                "foo;");

        cqlJob.run();

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);

        Mockito.verify(session, times(2)).execute(captor.capture());

        List<String> results = captor.getAllValues();

        assertThat(results.get(0), is("foo;"));
        assertThat(results.get(1), is("more foo;"));

    }
}