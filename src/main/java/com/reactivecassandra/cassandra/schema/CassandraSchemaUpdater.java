package com.reactivecassandra.cassandra.schema;

import com.datastax.driver.core.Session;
import com.google.common.annotations.Beta;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

/**
 * Cassandra schema updater
 */
@Beta
public class CassandraSchemaUpdater {
    private static final Logger log = LoggerFactory.getLogger(CassandraSchemaUpdater.class);

    private final Session session;
    private final String separator;
    private final List<URL> resources;

    public CassandraSchemaUpdater(Session session, String separator, URL... resources) {
        Preconditions.checkArgument(session != null, "session is null");
        Preconditions.checkArgument(separator != null && !separator.isEmpty(), "separator is empty");

        this.session = session;
        this.separator = separator.trim();

        this.resources = Arrays.asList(resources);
        for (URL url : resources) {
            Preconditions.checkArgument(url != null, "url is null: %s", this.resources);
        }
    }

    public void process() throws IOException {
        for (URL url : resources) {
            process(url);
        }
    }

    private void process(URL url) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream(), Charsets.UTF_8))) {
            String line;
            StringWriter sw = null;
            PrintWriter pw = null;

            while ((line = br.readLine()) != null) {
                if (sw == null) {
                    sw = new StringWriter();
                    pw = new PrintWriter(sw);
                }

                if (line.trim().equals(separator)) {
                    process(sw.toString().trim());
                    sw = null;
                    pw = null;
                } else {
                    pw.println(line);
                }
            }
        }
    }

    private void process(String query) {
        log.info("Executing query: {}", query);
        try {
            session.execute(query);
        } catch (Exception e) {
            throw new RuntimeException("Error while executing query [" + query + "]", e);
        }
    }
}

/*
session.getCluster().getMetadata().exportSchemaAsString()
session.getCluster().getMetadata().tokenMap
*/
