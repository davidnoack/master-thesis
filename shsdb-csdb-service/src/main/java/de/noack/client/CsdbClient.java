package de.noack.client;

import de.noack.model.CSDB;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public interface CsdbClient {
    String INPUT_TOPIC_NAME = "csdb-vanilla";
    String OUTPUT_TOPIC_NAME = "csdb-amended";

    String produceCsdb(CSDB csdb) throws IOException;

    InputStream readCsdb(String messageKey) throws IOException;

    Set<CSDB> consumeCsdbs();
}