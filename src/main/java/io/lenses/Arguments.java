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

import io.lenses.utils.Either;
import java.io.File;

public class Arguments {
  private final File configFile;
  private final boolean preview;

  public Arguments(File configFile, boolean preview) {
    this.configFile = configFile;
    this.preview = preview;
  }

  public File getConfigFile() {
    return configFile;
  }

  public boolean isPreview() {
    return preview;
  }

  public static Either<Errors, Arguments> from(String[] args) {
    String configFilePath = null;
    boolean isPreview = false;

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--config") && i + 1 < args.length) {
        configFilePath = args[i + 1];
        i++; // Skip the next argument, which is the file path
      } else if (args[i].equals("--preview")) {
        isPreview = true;
      }
    }

    if (configFilePath == null) {
      return Either.left(Errors.MISSING_CONFIG_FILE);
    }

    File configFile = new File(configFilePath);

    if (!configFile.exists()) {
      return Either.left(Errors.CONFIG_FILE_DOES_NOT_EXIST);
    }

    return Either.right(new Arguments(configFile, isPreview));
  }

  public static enum Errors {
    MISSING_CONFIG_FILE("Error: Missing --config argument."),
    CONFIG_FILE_DOES_NOT_EXIST("Config file does not exist.");

    private final String message;

    Errors(String message) {
      this.message = message;
    }

    public String getMessage() {
      return message;
    }
  }
}
