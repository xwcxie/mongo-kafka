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
package com.mongodb.kafka.connect.sink.cdc.mongodb.operations;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class UpdateShard extends Update {
  private static final Logger LOGGER = LoggerFactory.getLogger(Update.class);
  private final MongoSinkTopicConfig config;

  public UpdateShard(final MongoSinkTopicConfig config) {
    this.config = config;
  }

  @Override
  public WriteModel<BsonDocument> perform(final SinkDocument doc) {
    BsonDocument changeStreamDocument =
        doc.getValueDoc()
            .orElseThrow(
                () ->
                    new DataException("Error: value doc must not be missing for update operation"));

    BsonDocument documentKey = OperationHelper.getDocumentKey(changeStreamDocument);
    if (OperationHelper.hasFullDocument(changeStreamDocument)) {
      String keys = config.getString(MongoSinkTopicConfig.SHARD_KEY);
      BsonDocument fullDocument = OperationHelper.getFullDocument(changeStreamDocument);
      if (keys != null) {
        List<String> keyList = Arrays.asList(keys.split(","));
        keyList.forEach(
            e -> {
              BsonValue bsonValue = fullDocument.get(e);
              if (bsonValue != null) {
                documentKey.put(e, bsonValue);
              }
            });
      }
      LOGGER.debug("The full Document available, creating a replace operation.");
      return new ReplaceOneModel<>(documentKey, fullDocument);
    }

    LOGGER.debug("No full document field available, creating update operation.");
    return new UpdateOneModel<>(
        documentKey, OperationHelper.getUpdateDocument(changeStreamDocument));
  }
}
