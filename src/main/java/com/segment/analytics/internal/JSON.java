package com.segment.analytics.internal;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.segment.analytics.dto.Batch;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public final class JSON {
    private JSON() {}

    public static final ObjectMapper objectMapper = new ObjectMapper().setDefaultPropertyInclusion(Include.NON_EMPTY);

    public static int sizeInBytes(final Object obj) {
        return toJson(obj).length;
    }

    public static byte[] toJson(final Object msg) {
        try {
            return objectMapper.writeValueAsBytes(msg);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void write(final Batch batch, final Writer file) {
        try {
            objectMapper.writeValue(file, batch);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static final class InstantSerializer extends StdSerializer<Instant> {
        static final long serialVersionUID = 1L;

        InstantSerializer() {
            super(Instant.class);
        }

        @Override
        public void serialize(final Instant value, final JsonGenerator gen, final SerializerProvider provider)
                throws IOException {
            gen.writeString(DateTimeFormatter.ISO_INSTANT.format(value.truncatedTo(ChronoUnit.MILLIS)));
        }
    }

    public static final class InstantDeserializer extends StdDeserializer<Instant> {
        static final long serialVersionUID = 1L;

        InstantDeserializer() {
            super(Instant.class);
        }

        @Override
        public Instant deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
            return Instant.from(DateTimeFormatter.ISO_INSTANT.parse(p.getText()));
        }
    }
}
