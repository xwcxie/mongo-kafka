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
package com.mongodb.kafka.connect.sink.cdc.mongodb;

import static java.lang.String.format;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcHandler;
import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.DeleteShard;
import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.ReplaceShard;
import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.UpdateShard;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class ChangeStreamShardHandler extends CdcHandler {
  private final Map<OperationType, CdcOperation> operations = new HashMap<>();

  public ChangeStreamShardHandler(final MongoSinkTopicConfig config) {
    super(config);
    operations.put(OperationType.INSERT, new ReplaceShard(config));
    operations.put(OperationType.REPLACE, new ReplaceShard(config));
    operations.put(OperationType.UPDATE, new UpdateShard(config));
    operations.put(OperationType.DELETE, new DeleteShard(config));
  }

  private static final String OPERATION_TYPE = "operationType";
  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeStreamHandler.class);

  public Optional<WriteModel<BsonDocument>> handle(final SinkDocument doc) {
    BsonDocument changeStreamDocument = doc.getValueDoc().orElseGet(BsonDocument::new);

    if (!changeStreamDocument.containsKey(OPERATION_TYPE)) {
      throw new DataException(
          format(
              "Error: `%s` field is doc is missing. %s",
              OPERATION_TYPE, changeStreamDocument.toJson()));
    } else if (!changeStreamDocument.get(OPERATION_TYPE).isString()) {
      throw new DataException("Error: Unexpected CDC operation type, should be a string");
    }

    OperationType operationType =
        OperationType.fromString(changeStreamDocument.get(OPERATION_TYPE).asString().getValue());

    LOGGER.debug("Creating operation handler for: {}", operationType);
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("ChangeStream document {}", changeStreamDocument.toJson());
    }

    if (operations.containsKey(operationType)) {
      return Optional.of(operations.get(operationType).perform(doc));
    }
    LOGGER.warn("Unsupported change stream operation: {}", operationType.getValue());
    return Optional.empty();
  }
}
