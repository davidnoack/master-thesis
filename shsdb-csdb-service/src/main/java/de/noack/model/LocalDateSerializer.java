package de.noack.model;

import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * This class is used to serialize {@link LocalDate} objects.
 *
 * @author davidnoack
 */
public class LocalDateSerializer extends StdSerializer<LocalDate> {

    private static final long serialVersionUID = 1L;

    public LocalDateSerializer() {
        super(LocalDate.class);
    }

    @Override
    public void serialize(final LocalDate value, final JsonGenerator gen, final SerializerProvider sp) throws IOException {
        gen.writeString(value.format(DateTimeFormatter.ISO_LOCAL_DATE));
    }
}