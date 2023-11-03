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
package io.lenses.s3;

import java.util.Optional;

public class S3Location {
  private final String bucket;
  //
  private final Optional<String> prefix;

  public S3Location(String bucket, Optional<String> prefix) {
    if (bucket == null) throw new IllegalArgumentException("S3 bucket cannot be null");
    if (bucket.trim().isEmpty()) throw new IllegalArgumentException("S3 bucket cannot be empty");
    this.bucket = bucket;
    this.prefix = prefix;
  }

  public String getBucket() {
    return bucket;
  }

  public Optional<String> getPrefix() {
    return prefix;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (!(obj instanceof S3Location)) return false;
    S3Location other = (S3Location) obj;
    return bucket.equals(other.bucket) && prefix.equals(other.prefix);
  }
}
