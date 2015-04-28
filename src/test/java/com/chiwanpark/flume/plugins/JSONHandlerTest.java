package com.chiwanpark.flume.plugins;

import org.apache.flume.Event;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.charset.UnsupportedCharsetException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class JSONHandlerTest {
  @Test
  public void testJson() throws Exception {
    String jsonified = "{\"body\": \"hello\"}";
    JSONHandler handler = new JSONHandler("utf-8");

    Event event = handler.getEvent(jsonified);
    assertArrayEquals("hello".getBytes("utf-8"), event.getBody());
  }

  @Test(expected = UnsupportedCharsetException.class)
  public void testJsonWithInvalidEncoding() throws Exception {
    new JSONHandler("utf-12");
  }

  @Test
  public void testJsonWithHeadersAndBody() throws Exception {
    String jsonified = "{\"body\": \"hello\", \"headers\": {\"a\": \"123\", \"b\": \"hello\"}}";
    JSONHandler handler = new JSONHandler("utf-8");

    Event event = handler.getEvent(jsonified);

    assertEquals("123", event.getHeaders().get("a"));
    assertEquals("hello", event.getHeaders().get("b"));
    assertArrayEquals("hello".getBytes("utf-8"), event.getBody());
  }

  @Test
  public void testJsonWithoutEncoding() throws Exception {
    String jsonified = "{\"body\": \"한글\"}";
    JSONHandler handler = new JSONHandler(null);

    Event event = handler.getEvent(jsonified);

    assertArrayEquals("한글".getBytes("utf-8"), event.getBody());
  }
}
