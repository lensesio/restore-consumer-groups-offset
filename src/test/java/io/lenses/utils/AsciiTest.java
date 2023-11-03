/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at: http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package io.lenses.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class AsciiTest {
  @Test
  void handlesNullResourceName() {
    AtomicReference<String> captured = new AtomicReference<>(null);
    Ascii.display(null, captured::set);
    assertNull(captured.get());
  }

  @Test
  void handlesEmptyResourceName() {
    AtomicReference<String> captured = new AtomicReference<>(null);
    Ascii.display("", captured::set);
    assertNull(captured.get());
  }

  @Test
  void handlesNonExistentResourceName() {
    AtomicReference<String> captured = new AtomicReference<>(null);
    Ascii.display("non-existent-resource-name", captured::set);
    assertNull(captured.get());
  }

  @Test
  void returnsTheResourceContent() {
    AtomicReference<String> captured = new AtomicReference<>(null);
    Ascii.display("/ascii.txt", captured::set);
    assertEquals(captured.get(), "test result");
  }
}
