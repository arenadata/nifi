/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.services.iceberg.hive;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.hive.MetastoreUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Apache Iceberg Catalog backed by Apache Hive Metastore using the Hive 4 Metastore Client.
 * Iceberg HiveCatalog reads HiveConf.ConfVars fields renamed in Hive 4, so this Catalog extends BaseMetastoreCatalog
 * and reuses HiveTableOperations and HiveClientPool with configuration property names instead.
 * Metastore clients are created with the Thread Context ClassLoader that initialized the Catalog, because the Hive client
 * loads HiveMetaStoreClient and configured hooks through the Thread Context ClassLoader, which belongs to the calling
 * Processor when a Table commit opens a new connection.
 */
class HiveMetastoreCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Configurable {

    static final String METASTORE_URIS_PROPERTY = "hive.metastore.uris";

    static final String METASTORE_WAREHOUSE_PROPERTY = "hive.metastore.warehouse.dir";

    private static final String LOCATION_PROPERTY = "location";

    private static final String COMMENT_PROPERTY = "comment";

    private static final String DATABASE_LOCATION_FORMAT = "%s/%s.db";

    private static final String TABLE_LOCATION_FORMAT = "%s/%s";

    private String name;

    private Configuration conf;

    private Map<String, String> catalogProperties = Map.of();

    private boolean uniqueTableLocation;

    private FileIO fileIO;

    private HiveClientPool clients;

    @Override
    public void initialize(final String inputName, final Map<String, String> properties) {
        name = inputName;
        catalogProperties = Map.copyOf(properties);
        if (conf == null) {
            conf = new Configuration();
        }

        final String uri = properties.get(CatalogProperties.URI);
        if (uri != null) {
            conf.set(METASTORE_URIS_PROPERTY, uri);
        }

        final String warehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
        if (warehouseLocation != null) {
            conf.set(METASTORE_WAREHOUSE_PROPERTY, LocationUtil.stripTrailingSlash(warehouseLocation));
        }

        uniqueTableLocation = PropertyUtil.propertyAsBoolean(properties, CatalogProperties.UNIQUE_TABLE_LOCATION, CatalogProperties.UNIQUE_TABLE_LOCATION_DEFAULT);

        final String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
        fileIO = fileIOImpl == null ? new HadoopFileIO(conf) : CatalogUtil.loadFileIO(fileIOImpl, properties, conf);

        final int poolSize = PropertyUtil.propertyAsInt(properties, CatalogProperties.CLIENT_POOL_SIZE, CatalogProperties.CLIENT_POOL_SIZE_DEFAULT);
        clients = new ContextClassLoaderHiveClientPool(poolSize, conf, Thread.currentThread().getContextClassLoader());
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void setConf(final Configuration configuration) {
        conf = new Configuration(configuration);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    protected Map<String, String> properties() {
        return catalogProperties;
    }

    @Override
    protected boolean isValidIdentifier(final TableIdentifier tableIdentifier) {
        return tableIdentifier.namespace().levels().length == 1;
    }

    @Override
    protected TableOperations newTableOps(final TableIdentifier tableIdentifier) {
        final String database = tableIdentifier.namespace().level(0);
        return new HiveTableOperations(conf, clients, fileIO, null, name, database, tableIdentifier.name()) {
        };
    }

    @Override
    protected String defaultWarehouseLocation(final TableIdentifier tableIdentifier) {
        final String databaseName = tableIdentifier.namespace().level(0);
        final String tableLocation = LocationUtil.tableLocation(tableIdentifier, uniqueTableLocation);

        final Database database;
        try {
            database = run(client -> client.getDatabase(databaseName));
        } catch (final NoSuchObjectException e) {
            throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", databaseName);
        } catch (final TException e) {
            throw new RuntimeException("Get Database failed for %s".formatted(tableIdentifier), e);
        }

        final String databaseLocation = database.getLocationUri() == null
                ? getDefaultDatabaseLocation(databaseName)
                : LocationUtil.stripTrailingSlash(database.getLocationUri());
        return TABLE_LOCATION_FORMAT.formatted(databaseLocation, tableLocation);
    }

    @Override
    public List<TableIdentifier> listTables(final Namespace namespace) {
        final String database = getDatabaseName(namespace);
        try {
            final List<String> tableNames = run(client -> client.getAllTables(database));
            if (tableNames.isEmpty()) {
                getDatabase(namespace);
                return List.of();
            }
            final List<Table> tables = run(client -> client.getTableObjectsByName(database, tableNames));
            return tables.stream()
                    .filter(this::isIcebergTable)
                    .map(table -> TableIdentifier.of(namespace, table.getTableName()))
                    .toList();
        } catch (final NoSuchObjectException | UnknownDBException e) {
            throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);
        } catch (final TException e) {
            throw new RuntimeException("List Tables failed for %s".formatted(namespace), e);
        }
    }

    @Override
    public boolean dropTable(final TableIdentifier identifier, final boolean purge) {
        if (!isValidIdentifier(identifier)) {
            return false;
        }

        final TableOperations operations = newTableOps(identifier);
        TableMetadata lastMetadata = null;
        if (purge) {
            try {
                lastMetadata = operations.current();
            } catch (final NotFoundException e) {
                lastMetadata = null;
            }
        }

        try {
            run(client -> {
                client.dropTable(identifier.namespace().level(0), identifier.name(), false, false);
                return null;
            });
        } catch (final NoSuchObjectException e) {
            return false;
        } catch (final TException e) {
            throw new RuntimeException("Drop Table failed for %s".formatted(identifier), e);
        }

        if (lastMetadata != null) {
            CatalogUtil.dropTableData(operations.io(), lastMetadata);
        }
        return true;
    }

    @Override
    public void renameTable(final TableIdentifier from, final TableIdentifier to) {
        if (!isValidIdentifier(from) || !isValidIdentifier(to)) {
            throw new NoSuchTableException("Invalid table identifier: %s", isValidIdentifier(from) ? to : from);
        }

        final String fromDatabase = from.namespace().level(0);
        final String toDatabase = to.namespace().level(0);
        try {
            final Table table = run(client -> client.getTable(fromDatabase, from.name()));
            if (!isIcebergTable(table)) {
                throw new NoSuchTableException("Table is not an Iceberg table: %s", from);
            }
            table.setDbName(toDatabase);
            table.setTableName(to.name());
            run(client -> {
                MetastoreUtil.alterTable(client, fromDatabase, from.name(), table);
                return null;
            });
        } catch (final NoSuchObjectException e) {
            throw new NoSuchTableException(e, "Table does not exist: %s", from);
        } catch (final TException e) {
            throw new RuntimeException("Rename Table failed for %s".formatted(from), e);
        }
    }

    @Override
    public void createNamespace(final Namespace namespace, final Map<String, String> metadata) {
        final String databaseName = getDatabaseName(namespace);

        final Map<String, String> parameters = new HashMap<>(metadata);
        final String location = parameters.remove(LOCATION_PROPERTY);
        final String comment = parameters.remove(COMMENT_PROPERTY);

        final Database database = new Database();
        database.setName(databaseName);
        database.setLocationUri(location == null ? getDefaultDatabaseLocation(databaseName) : location);
        database.setDescription(comment);
        database.setParameters(parameters);

        try {
            run(client -> {
                client.createDatabase(database);
                return null;
            });
        } catch (final org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
            throw new AlreadyExistsException(e, "Namespace already exists: %s", namespace);
        } catch (final TException e) {
            throw new RuntimeException("Create Namespace failed for %s".formatted(namespace), e);
        }
    }

    @Override
    public List<Namespace> listNamespaces(final Namespace namespace) {
        if (namespace.isEmpty()) {
            try {
                final List<String> databases = run(IMetaStoreClient::getAllDatabases);
                return databases.stream().map(Namespace::of).toList();
            } catch (final TException e) {
                throw new RuntimeException("List Namespaces failed", e);
            }
        }

        loadNamespaceMetadata(namespace);
        return List.of();
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(final Namespace namespace) {
        final Database database = getDatabase(namespace);

        final Map<String, String> metadata = new HashMap<>();
        if (database.getParameters() != null) {
            metadata.putAll(database.getParameters());
        }
        if (database.getLocationUri() != null) {
            metadata.put(LOCATION_PROPERTY, database.getLocationUri());
        }
        if (database.getDescription() != null) {
            metadata.put(COMMENT_PROPERTY, database.getDescription());
        }
        return metadata;
    }

    @Override
    public boolean dropNamespace(final Namespace namespace) {
        if (namespace.levels().length != 1) {
            return false;
        }

        try {
            run(client -> {
                client.dropDatabase(namespace.level(0), false, false, false);
                return null;
            });
            return true;
        } catch (final NoSuchObjectException e) {
            return false;
        } catch (final InvalidOperationException e) {
            throw new NamespaceNotEmptyException(e, "Namespace is not empty: %s", namespace);
        } catch (final TException e) {
            throw new RuntimeException("Drop Namespace failed for %s".formatted(namespace), e);
        }
    }

    @Override
    public boolean setProperties(final Namespace namespace, final Map<String, String> properties) {
        final Database database = getDatabase(namespace);
        final Map<String, String> parameters = database.getParameters() == null ? new HashMap<>() : new HashMap<>(database.getParameters());
        parameters.putAll(properties);
        database.setParameters(parameters);
        alterDatabase(database);
        return true;
    }

    @Override
    public boolean removeProperties(final Namespace namespace, final Set<String> properties) {
        final Database database = getDatabase(namespace);
        final Map<String, String> parameters = database.getParameters() == null ? new HashMap<>() : new HashMap<>(database.getParameters());
        parameters.keySet().removeAll(properties);
        database.setParameters(parameters);
        alterDatabase(database);
        return true;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (clients != null) {
                clients.close();
            }
            if (fileIO != null) {
                fileIO.close();
            }
        }
    }

    @Override
    public String toString() {
        return "%s[name=%s, uri=%s]".formatted(getClass().getSimpleName(), name, conf == null ? null : conf.get(METASTORE_URIS_PROPERTY));
    }

    HiveClientPool getClientPool() {
        return clients;
    }

    FileIO getFileIO() {
        return fileIO;
    }

    private Database getDatabase(final Namespace namespace) {
        final String databaseName = getDatabaseName(namespace);
        try {
            return run(client -> client.getDatabase(databaseName));
        } catch (final NoSuchObjectException e) {
            throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);
        } catch (final TException e) {
            throw new RuntimeException("Get Database failed for %s".formatted(namespace), e);
        }
    }

    private void alterDatabase(final Database database) {
        try {
            run(client -> {
                client.alterDatabase(database.getName(), database);
                return null;
            });
        } catch (final NoSuchObjectException e) {
            throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", database.getName());
        } catch (final TException e) {
            throw new RuntimeException("Alter Database failed for %s".formatted(database.getName()), e);
        }
    }

    private String getDatabaseName(final Namespace namespace) {
        if (namespace.levels().length != 1) {
            throw new NoSuchNamespaceException("Namespace must contain one level: %s", namespace);
        }
        return namespace.level(0);
    }

    private String getDefaultDatabaseLocation(final String databaseName) {
        final String warehouseLocation = conf.get(METASTORE_WAREHOUSE_PROPERTY);
        if (warehouseLocation == null) {
            throw new IllegalStateException("Warehouse location not configured: %s".formatted(METASTORE_WAREHOUSE_PROPERTY));
        }
        return DATABASE_LOCATION_FORMAT.formatted(LocationUtil.stripTrailingSlash(warehouseLocation), databaseName);
    }

    private boolean isIcebergTable(final Table table) {
        final Map<String, String> parameters = table.getParameters() == null ? Collections.emptyMap() : table.getParameters();
        return BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(parameters.get(BaseMetastoreTableOperations.TABLE_TYPE_PROP));
    }

    private <R> R run(final ClientPool.Action<R, IMetaStoreClient, TException> action) throws TException {
        try {
            return clients.run(action);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Hive Metastore operation interrupted", e);
        }
    }

    private static class ContextClassLoaderHiveClientPool extends HiveClientPool {
        private final ClassLoader classLoader;

        private ContextClassLoaderHiveClientPool(final int poolSize, final Configuration configuration, final ClassLoader classLoader) {
            super(poolSize, configuration);
            this.classLoader = classLoader;
        }

        @Override
        protected IMetaStoreClient newClient() {
            return getWithClassLoader(super::newClient);
        }

        @Override
        protected IMetaStoreClient reconnect(final IMetaStoreClient client) {
            return getWithClassLoader(() -> super.reconnect(client));
        }

        private IMetaStoreClient getWithClassLoader(final Supplier<IMetaStoreClient> supplier) {
            final Thread thread = Thread.currentThread();
            final ClassLoader callerClassLoader = thread.getContextClassLoader();
            thread.setContextClassLoader(classLoader);
            try {
                return supplier.get();
            } finally {
                thread.setContextClassLoader(callerClassLoader);
            }
        }
    }
}
