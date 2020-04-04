package de.noack.service;

import de.noack.client.CsdbClient;
import de.noack.client.pulsar.CsdbPulsarClient;
import de.noack.model.CSDB;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

@RequestScoped
public class CsdbService {
    private static final String IMPLEMENTATION_MISSING = "Not yet implemented!";
    private CsdbClient csdbClient;

    @Inject
    public CsdbService(@ConfigProperty(name = "commitlog") final CommitLog commitLog, CsdbPulsarClient csdbPulsarClient) {
        super();
        switch (commitLog) {
            case PULSAR:
                csdbClient = csdbPulsarClient;
                break;
            case KAFKA:
                // TODO: Implement
                throw new RuntimeException(IMPLEMENTATION_MISSING);
            default:
                throw new RuntimeException(IMPLEMENTATION_MISSING);
        }
    }

    public String produce(byte[] csdb) throws IOException {
        return csdbClient.produceVanillaCsdb(csdb);
    }

    public InputStream findVanillaCsdb(final String messageKey) {
        return csdbClient.findVanillaCsdb(messageKey);
    }

    public void allVanillaCsdbs(final OutputStream outputStream) {
        csdbClient.allVanillaCsdbs(outputStream);
    }

    public Set<CSDB> allTransformedCsdbs() {
        return csdbClient.allTransformedCsdbs();
    }

    public CSDB findTransformedCsdb(final String messageKey) {
        return csdbClient.findTransformedCsdb(messageKey);
    }
}