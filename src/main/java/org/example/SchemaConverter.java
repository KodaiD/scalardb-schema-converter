package org.example;

import com.scalar.db.api.*;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.storage.cosmos.ConcatenationVisitor;
import com.scalar.db.util.ScalarDbUtils;
import java.util.*;

public class SchemaConverter {
  public static final String ARCHIVE_NAMESPACE = "archive";
  public static final String ARCHIVE_TABLE_NAME = "archive_data";
  public static final String ARCHIVE_TABLE_ID = "id";
  public static final String ARCHIVE_TABLE_CONCATENATED_KEY = "concatenated_key";
  public static final String MAPPING_TABLE_ID = "id";
  public static final String MAPPING_TABLE_NAME = "archive_mapping";
  public static final String SEPARATOR = ":";
  private final DistributedStorageAdmin admin;
  private final DistributedTransactionManager manager;

  public SchemaConverter(DistributedStorageAdmin admin, DistributedTransactionManager manager) {
    this.admin = admin;
    this.manager = manager;
  }

  public void store(Scan scan) {
    try {
      if (!scan.forNamespace().isPresent() || !scan.forTable().isPresent()) {
        throw new IllegalArgumentException("Scan must specify a namespace and table");
      }
      String namespaceName = scan.forNamespace().get();
      String tableName = scan.forTable().get();
      TableMetadata metadata = admin.getTableMetadata(namespaceName, tableName);
      if (metadata == null) {
        throw new IllegalArgumentException(
            "Table metadata not found for " + namespaceName + "." + tableName);
      }
      DistributedTransaction transaction = manager.start();
      String id = UUID.randomUUID().toString();
      // Read from the original table
      List<Result> results = transaction.scan(scan);
      for (Result result : results) {
        Key originalTablePartitionKey = ScalarDbUtils.getPartitionKey(result, metadata);
        Key originalTableClusteringKey =
            ScalarDbUtils.getClusteringKey(result, metadata).orElse(null);

        // Insert into the mapping table
        transaction.insert(
            prepareInsertForMappingTable(
                originalTablePartitionKey, originalTableClusteringKey, id));

        // Insert into the archive table
        String concatenatedPartitionKey = getConcatenatedPartitionKey(scan, metadata);
        Optional<String> concatenatedClusteringKey = getConcatenatedClusteringKey(scan, metadata);
        String concatenatedKey =
            concatenatedClusteringKey
                .map(s -> concatenatedPartitionKey + SEPARATOR + s)
                .orElse(concatenatedPartitionKey);
        transaction.insert(
            prepareInsertForArchiveTable(
                Key.ofText(ARCHIVE_TABLE_ID, id),
                Key.ofText(ARCHIVE_TABLE_CONCATENATED_KEY, concatenatedKey),
                result,
                metadata));

        // Delete from the original table
        transaction.delete(
            prepareDeleteForOriginalTable(
                namespaceName, tableName, originalTablePartitionKey, originalTableClusteringKey));

        // Commit the transaction
        try {
          transaction.commit();
        } catch (TransactionException e) {
          transaction.rollback();
          throw e;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to archive data", e);
    }
  }

  private Insert prepareInsertForMappingTable(Key partitionKey, Key clusteringKey, String id) {
    if (clusteringKey != null) {
      return Insert.newBuilder()
          .namespace(ARCHIVE_NAMESPACE)
          .table(MAPPING_TABLE_NAME)
          .partitionKey(partitionKey)
          .clusteringKey(clusteringKey)
          .textValue(MAPPING_TABLE_ID, id)
          .build();
    } else {
      return Insert.newBuilder()
          .namespace(ARCHIVE_NAMESPACE)
          .table(MAPPING_TABLE_NAME)
          .partitionKey(partitionKey)
          .textValue(MAPPING_TABLE_ID, id)
          .build();
    }
  }

  private Insert prepareInsertForArchiveTable(
      Key partitionKey, Key clusteringKey, Result result, TableMetadata metadata) {
    if (clusteringKey == null) {
      throw new IllegalArgumentException(
          "Clustering key must not be null for archive table: " + ARCHIVE_TABLE_NAME);
    }
    InsertBuilder.Buildable buildable =
        Insert.newBuilder()
            .namespace(ARCHIVE_NAMESPACE)
            .table(ARCHIVE_TABLE_NAME)
            .partitionKey(partitionKey)
            .clusteringKey(clusteringKey);
    for (Map.Entry<String, Column<?>> entry : result.getColumns().entrySet()) {
      String columnName = entry.getKey();
      Column<?> column = entry.getValue();
      if (!metadata.getPartitionKeyNames().contains(columnName)
          && !metadata.getClusteringKeyNames().contains(columnName)) {
        // Only include non-key columns
        buildable = buildable.value(column);
      }
    }
    return buildable.build();
  }

  private Delete prepareDeleteForOriginalTable(
      String namespaceName, String tableName, Key partitionKey, Key clusteringKey) {
    if (clusteringKey != null) {
      return Delete.newBuilder()
          .namespace(namespaceName)
          .table(tableName)
          .partitionKey(partitionKey)
          .clusteringKey(clusteringKey)
          .build();
    } else {
      return Delete.newBuilder()
          .namespace(namespaceName)
          .table(tableName)
          .partitionKey(clusteringKey)
          .build();
    }
  }

  private String getConcatenatedPartitionKey(Operation operation, TableMetadata metadata) {
    Map<String, Column<?>> keyMap = new HashMap<>();
    operation.getPartitionKey().getColumns().forEach(c -> keyMap.put(c.getName(), c));
    ConcatenationVisitor visitor = new ConcatenationVisitor();
    metadata.getPartitionKeyNames().forEach(name -> keyMap.get(name).accept(visitor));
    return visitor.build();
  }

  private Optional<String> getConcatenatedClusteringKey(
      Operation operation, TableMetadata metadata) {
    Map<String, Column<?>> keyMap = new HashMap<>();
    if (operation.getClusteringKey().isPresent()) {
      operation.getClusteringKey().get().getColumns().forEach(c -> keyMap.put(c.getName(), c));
      ConcatenationVisitor visitor = new ConcatenationVisitor();
      metadata.getPartitionKeyNames().forEach(name -> keyMap.get(name).accept(visitor));
      return Optional.of(visitor.build());
    } else {
      return Optional.empty();
    }
  }
}
