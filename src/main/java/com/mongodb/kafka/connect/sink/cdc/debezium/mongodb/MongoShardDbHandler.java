/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */
package com.mongodb.kafka.connect.sink.cdc.debezium.mongodb;

import java.util.HashMap;
import java.util.Map;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.cdc.debezium.OperationType;

public class MongoShardDbHandler extends MongoDbHandler {
  private static final Map<OperationType, CdcOperation> DEFAULT_OPERATIONS = new HashMap<>();

  public MongoShardDbHandler(final MongoSinkTopicConfig config) {
    super(config);
    DEFAULT_OPERATIONS.put(OperationType.READ, new MongoDbShardInsert(config));
    DEFAULT_OPERATIONS.put(OperationType.CREATE, new MongoDbShardInsert(config));
    DEFAULT_OPERATIONS.put(OperationType.UPDATE, new MongoDbShardUpdate(config));
    DEFAULT_OPERATIONS.put(OperationType.DELETE, new MongoDbShardDelete());
    registerOperations(DEFAULT_OPERATIONS);
  }
}
