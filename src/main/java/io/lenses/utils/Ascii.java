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

import static io.lenses.utils.Utils.readAll;

import io.lenses.App;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

public class Ascii {
  // java function one argument returns void

  public static void display(String resourceName, Consumer<String> func) {
    if (resourceName == null || resourceName.isEmpty()) {
      return;
    }
    try (InputStream inputStream = App.class.getResourceAsStream(resourceName)) {
      if (inputStream != null) {
        func.accept(readAll(inputStream));
      }
    } catch (IOException e) {
      System.err.println("Failed to read the ascii.txt resource.");
    }
  }
}
