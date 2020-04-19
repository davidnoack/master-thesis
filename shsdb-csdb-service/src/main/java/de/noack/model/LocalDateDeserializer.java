package de.noack.model;

import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.time.LocalDate;

/**
 * This class is used to deserialize {@link LocalDate} objects.
 *
 * @author davidnoack
 */
public class LocalDateDeserializer extends StdDeserializer<LocalDate> {
    private static final long serialVersionUID = 3875354160417778269L;

    protected LocalDateDeserializer() {
        super(LocalDate.class);
    }

    @Override
    public LocalDate deserialize(final JsonParser jp, final DeserializationContext ctxt)
            throws IOException {
        return LocalDate.parse(jp.readValueAs(String.class));
    }
}