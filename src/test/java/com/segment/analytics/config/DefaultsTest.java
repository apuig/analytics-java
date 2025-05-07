package com.segment.analytics.config;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DefaultsTest {

    private static final String PROP = "segment.retry.at";

    @Before
    @After
    public void clearProperty() {
        System.clearProperty(PROP);
    }

    @Test
    public void testSecondsDefault() {
        System.setProperty(PROP, "10,20s,5");
        final List<Duration> result = Defaults.getDurationList(PROP, List.of());
        assertEquals(List.of(Duration.ofSeconds(5), Duration.ofSeconds(10), Duration.ofSeconds(20)), result);
    }

    @Test
    public void testMinutes() {
        System.setProperty(PROP, "2m,1m,60s");
        final List<Duration> result = Defaults.getDurationList(PROP, List.of());
        assertEquals(List.of(Duration.ofSeconds(60), Duration.ofMinutes(1), Duration.ofMinutes(2)), result);
    }

    @Test
    public void testHoursAndDays() {
        System.setProperty(PROP, "1h,2d,3h,1d");
        final List<Duration> result = Defaults.getDurationList(PROP, List.of());
        assertEquals(List.of(Duration.ofHours(1), Duration.ofHours(3), Duration.ofDays(1), Duration.ofDays(2)), result);
    }

    @Test
    public void testMixedUnitsAndSorting() {
        System.setProperty(PROP, "90s,1m,2m,30,1h");
        final List<Duration> result = Defaults.getDurationList(PROP, List.of());
        assertEquals(
                List.of(
                        Duration.ofSeconds(30),
                        Duration.ofMinutes(1),
                        Duration.ofSeconds(90),
                        Duration.ofMinutes(2),
                        Duration.ofHours(1)),
                result);
    }

    @Test
    public void testInvalidFallsBackToDefault() {
        System.setProperty(PROP, "bad,1x,");
        final List<Duration> def = List.of(Duration.ofSeconds(42));
        final List<Duration> result = Defaults.getDurationList(PROP, def);
        assertEquals(def, result);
    }

    @Test
    public void testEmptyFallsBackToDefault() {
        System.setProperty(PROP, "");
        final List<Duration> def = List.of(Duration.ofSeconds(99));
        final List<Duration> result = Defaults.getDurationList(PROP, def);
        assertEquals(def, result);
    }

    @Test
    public void testNoPropertyReturnsDefault() {
        final List<Duration> def = List.of(Duration.ofSeconds(77));
        final List<Duration> result = Defaults.getDurationList(PROP, def);
        assertEquals(def, result);
    }
}
