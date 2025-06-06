package org.example;

import com.scalar.db.api.*;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.storage.cosmos.ConcatenationVisitor;
import com.scalar.db.util.ScalarDbUtils;
import java.util.*;

public class SchemaConverter {
  public static final String ARCHIVE_TABLE_NAME = "archive_data";
  public static final String ARCHIVE_TABLE_ID = "id";
  public static final String ARCHIVE_TABLE_CONCATENATED_KEY = "concatenated_key";
  public static final String MAPPING_TABLE_ID = "id";
  public static final String MAPPING_TABLE_NAME = "archive_mapping";
  public static final String SEPARATOR = ":";

  private final DistributedStorageAdmin admin;
  private final DistributedTransactionManager manager;
  private final String archiveNamespaceName;

  public SchemaConverter(
      DistributedStorageAdmin admin, DistributedTransactionManager manager, String namespaceName) {
    this.admin = admin;
    this.manager = manager;
    this.archiveNamespaceName = namespaceName;
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
            "Table metadata not found for "
                + ScalarDbUtils.getFullTableName(namespaceName, tableName));
      }

      createMappingTableIfNotExists(namespaceName, metadata);
      createArchiveTableIfNotExists(metadata);

      DistributedTransaction transaction = manager.start();
      String id = UUID.randomUUID().toString();
      // Read from the original table
      List<Result> results = transaction.scan(scan);
      List<Insert> insertsForMappingTable = new ArrayList<>();
      List<Insert> insertsForArchiveTable = new ArrayList<>();
      List<Delete> deletesForOriginalTable = new ArrayList<>();
      for (Result result : results) {
        Key originalTablePartitionKey = ScalarDbUtils.getPartitionKey(result, metadata);
        Key originalTableClusteringKey =
            ScalarDbUtils.getClusteringKey(result, metadata).orElse(null);

        // Insert into the mapping table
        InsertBuilder.Buildable builderForMappingTable =
            Insert.newBuilder()
                .namespace(namespaceName)
                .table(MAPPING_TABLE_NAME)
                .partitionKey(originalTablePartitionKey);
        if (originalTableClusteringKey != null) {
          builderForMappingTable = builderForMappingTable.clusteringKey(originalTableClusteringKey);
        }
        Insert insertForMappingTable =
            builderForMappingTable.textValue(MAPPING_TABLE_ID, id).build();
        insertsForMappingTable.add(insertForMappingTable);

        // Insert into the archive table
        String concatenatedPartitionKey = getConcatenatedPartitionKey(scan, metadata);
        Optional<String> concatenatedClusteringKey = getConcatenatedClusteringKey(scan, metadata);
        String concatenatedKey =
            concatenatedClusteringKey
                .map(s -> concatenatedPartitionKey + SEPARATOR + s)
                .orElse(concatenatedPartitionKey);
        InsertBuilder.Buildable builderForArchiveTable =
            Insert.newBuilder()
                .namespace(archiveNamespaceName)
                .table(ARCHIVE_TABLE_NAME)
                .partitionKey(Key.ofText(ARCHIVE_TABLE_ID, id))
                .clusteringKey(Key.ofText(ARCHIVE_TABLE_CONCATENATED_KEY, concatenatedKey));
        for (Map.Entry<String, Column<?>> entry : result.getColumns().entrySet()) {
          String columnName = entry.getKey();
          Column<?> column = entry.getValue();
          if (!metadata.getPartitionKeyNames().contains(columnName)
              && !metadata.getClusteringKeyNames().contains(columnName)) {
            // Only include non-key columns
            builderForArchiveTable = builderForArchiveTable.value(column);
          }
        }
        Insert insertForArchiveTable = builderForArchiveTable.build();
        insertsForArchiveTable.add(insertForArchiveTable);

        // Delete from the original table
        DeleteBuilder.Buildable deleteBuilder =
            Delete.newBuilder()
                .namespace(namespaceName)
                .table(tableName)
                .partitionKey(originalTablePartitionKey);
        if (originalTableClusteringKey != null) {
          deleteBuilder = deleteBuilder.clusteringKey(originalTableClusteringKey);
        }
        Delete deleteForOriginalTable = deleteBuilder.build();
        deletesForOriginalTable.add(deleteForOriginalTable);
      }

      try {
        transaction.mutate(insertsForMappingTable);
        transaction.mutate(insertsForArchiveTable);
        transaction.mutate(deletesForOriginalTable);
        transaction.commit();
      } catch (TransactionException e) {
        transaction.rollback();
        throw e;
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to archive data", e);
    }
  }

  private void createMappingTableIfNotExists(
      String originalNamespace, TableMetadata originalTableMetadata) {
    try {
      TableMetadata.Builder builder = TableMetadata.newBuilder();
      for (String columnName : originalTableMetadata.getPartitionKeyNames()) {
        DataType dataType = originalTableMetadata.getColumnDataType(columnName);
        builder.addColumn(columnName, dataType).addPartitionKey(columnName);
      }
      for (String columnName : originalTableMetadata.getClusteringKeyNames()) {
        DataType dataType = originalTableMetadata.getColumnDataType(columnName);
        builder.addColumn(columnName, dataType).addClusteringKey(columnName);
      }
      builder.addColumn(MAPPING_TABLE_ID, DataType.TEXT).addPartitionKey(MAPPING_TABLE_ID);
      admin.createTable(originalNamespace, MAPPING_TABLE_NAME, builder.build(), true);
      System.out.println(
          "Mapping table created: "
              + ScalarDbUtils.getFullTableName(archiveNamespaceName, MAPPING_TABLE_NAME));
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to create mapping table", e);
    }
  }

  private void createArchiveTableIfNotExists(TableMetadata originalTableMetadata) {
    try {
      TableMetadata.Builder builder =
          TableMetadata.newBuilder()
              .addColumn(ARCHIVE_TABLE_ID, DataType.TEXT)
              .addColumn(ARCHIVE_TABLE_CONCATENATED_KEY, DataType.TEXT)
              .addPartitionKey(ARCHIVE_TABLE_ID)
              .addClusteringKey(ARCHIVE_TABLE_CONCATENATED_KEY);
      for (String columnName : originalTableMetadata.getColumnNames()) {
        if (!originalTableMetadata.getPartitionKeyNames().contains(columnName)
            && !originalTableMetadata.getClusteringKeyNames().contains(columnName)) {
          DataType dataType = originalTableMetadata.getColumnDataType(columnName);
          builder.addColumn(columnName, dataType);
        }
      }
      admin.createTable(archiveNamespaceName, ARCHIVE_TABLE_NAME, builder.build(), true);
      System.out.println(
          "Archive table created: "
              + ScalarDbUtils.getFullTableName(archiveNamespaceName, ARCHIVE_TABLE_NAME));
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to create archive table", e);
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
