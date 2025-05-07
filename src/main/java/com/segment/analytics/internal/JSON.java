package com.segment.analytics.internal;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.segment.analytics.dto.Batch;
import java.io.IOException;
import java.io.Writer;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public final class JSON {
    public static ObjectMapper OBJECT_MAPPER = new ObjectMapper().setDefaultPropertyInclusion(Include.NON_EMPTY);

    public static int sizeInBytes(Object obj) {
        return toJson(obj).length;
    }

    public static byte[] toJson(final Object msg) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(msg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void write(final Batch batch, Writer file) {
        try {
            OBJECT_MAPPER.writeValue(file, batch);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static final class InstantSerializer extends StdSerializer<Instant> {
        private static final long serialVersionUID = -2276445460014609308L;

        protected InstantSerializer() {
            super(Instant.class);
        }

        @Override
        public void serialize(Instant value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeString(DateTimeFormatter.ISO_INSTANT.format(value.truncatedTo(ChronoUnit.MILLIS)));
        }
    }

    public static final class InstantDeserializer extends StdDeserializer<Instant> {
        private static final long serialVersionUID = -7976174277511994690L;

        protected InstantDeserializer() {
            super(Instant.class);
        }

        @Override
        public Instant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
            return Instant.from(DateTimeFormatter.ISO_INSTANT.parse(p.getText()));
        }
    }
}
