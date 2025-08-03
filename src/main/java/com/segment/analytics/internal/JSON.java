package com.segment.analytics.internal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.segment.analytics.dto.Batch;

public final class JSON {
    public static final ObjectMapper objectMapper = new ObjectMapper().setDefaultPropertyInclusion(Include.NON_EMPTY);

    public static int sizeInBytes(Object obj) {
        return toJson(obj).length;
    }

    public static byte[] toJson(final Object msg) {
        try {
            return objectMapper.writeValueAsBytes(msg);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void write(final Batch batch, Writer file) {
        try {
            objectMapper.writeValue(file, batch);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static final class InstantSerializer extends StdSerializer<Instant> {
        private static final long serialVersionUID = 1L;

        protected InstantSerializer() {
            super(Instant.class);
        }

        @Override
        public void serialize(Instant value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeString(DateTimeFormatter.ISO_INSTANT.format(value.truncatedTo(ChronoUnit.MILLIS)));
        }
    }

    public static final class InstantDeserializer extends StdDeserializer<Instant> {
        private static final long serialVersionUID = 1L;

        protected InstantDeserializer() {
            super(Instant.class);
        }

        @Override
        public Instant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            return Instant.from(DateTimeFormatter.ISO_INSTANT.parse(p.getText()));
        }
    }
}
