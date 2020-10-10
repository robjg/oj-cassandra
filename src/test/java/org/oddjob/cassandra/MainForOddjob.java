package org.oddjob.cassandra;

/**
 * Because Oddjob is only on the test classpath, need this to run from Intellij.
 */
public class MainForOddjob {

    public static void main(String... args) throws Exception {

        org.oddjob.Main.main(args);;
    }
}
