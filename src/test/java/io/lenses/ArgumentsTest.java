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
package io.lenses;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class ArgumentsTest {

  @Test
  void returnsTheArguments() throws IOException {
    // create a temp file which is deleted on process stop
    File configFile = new File("config.txt");
    configFile.deleteOnExit();
    configFile.createNewFile();
    Arguments.from(new String[] {"--config", configFile.getAbsolutePath(), "--preview"})
        .ifRightOrElse(
            arguments -> {
              assertEquals(
                  arguments.getConfigFile().getAbsolutePath(), configFile.getAbsolutePath());
              assertTrue(arguments.isPreview());
            },
            errors -> fail("Should not return errors"));
  }

  @Test
  void returnsMissingConfigFileError() {
    Arguments.from(new String[] {"--preview"})
        .ifRightOrElse(
            arguments -> fail("Should not return arguments"),
            errors -> assertEquals(errors, Arguments.Errors.MISSING_CONFIG_FILE));
  }

  @Test
  void returnsConfigFileDoesNotExistError() {
    Arguments.from(new String[] {"--config", "non-existent-file.txt"})
        .ifRightOrElse(
            arguments -> fail("Should not return arguments"),
            errors -> assertEquals(errors, Arguments.Errors.CONFIG_FILE_DOES_NOT_EXIST));
  }

  @Test
  void returnsConfigFileDoesNotExistErrorWhenConfigFileIsNotSpecified() {
    Arguments.from(new String[] {})
        .ifRightOrElse(
            arguments -> fail("Should not return arguments"),
            errors -> assertEquals(errors, Arguments.Errors.MISSING_CONFIG_FILE));
  }

  @Test
  void returnsFalseForPreviewWhenItsNotSpecified() throws IOException {
    // create a temp file which is deleted on process stop
    File configFile = new File("config.txt");
    configFile.deleteOnExit();
    configFile.createNewFile();
    Arguments.from(new String[] {"--config", configFile.getAbsolutePath()})
        .ifRightOrElse(
            arguments -> assertFalse(arguments.isPreview()),
            errors -> fail("Should not return errors"));
  }

  @Test
  void returnsArgumentsWhenPreviewIsTheFirstOne() throws IOException {
    // create a temp file which is deleted on process stop
    File configFile = new File("config.txt");
    configFile.deleteOnExit();
    configFile.createNewFile();
    Arguments.from(new String[] {"--preview", "--config", configFile.getAbsolutePath()})
        .ifRightOrElse(
            arguments -> {
              assertEquals(
                  arguments.getConfigFile().getAbsolutePath(), configFile.getAbsolutePath());
              assertTrue(arguments.isPreview());
            },
            errors -> fail("Should not return errors"));
  }
}
