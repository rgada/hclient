/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Wrapper for Thrift HMS interface.
 */
public final class HMSClient implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(HMSClient.class);
  private static final String CONFIG_DIR = "/etc/hive/conf";
  private static final String HIVE_SITE = "hive-site.xml";
  private static final String CORE_SITE = "core-site.xml";
  
  private final String confDir;
  private ThriftHiveMetastore.Iface client;
  private TTransport transport;
  private URI serverURI;

  public URI getServerURI() {
    return serverURI;
  }

  @Override
  public String toString() {
    return serverURI.toString();
  }

  /**
   * Connect to HMS and return a client instance.
   * @param uri Hive Metastore URI.
   */
  public HMSClient(@Nullable URI uri)
      throws TException, IOException, InterruptedException, LoginException, URISyntaxException {
    this(uri, CONFIG_DIR);
  }

  /**
   * Connect to HMS and return a client instance.
   *
   * @param uri Hive Metastore URI.
   * @param confDir Directory with Hive configuration files - normally /etc/hive/conf
   */
  public HMSClient(@Nullable URI uri, @Nullable String confDir)
      throws TException, IOException, InterruptedException, LoginException, URISyntaxException {
    this.confDir = (confDir == null ? CONFIG_DIR : confDir);
    getClient(uri);
  }

  /**
   * Return a new connected client using the same configuration as the original client.
   * @return New connected client instance
   * @throws InterruptedException
   * @throws URISyntaxException
   * @throws TException
   * @throws LoginException
   * @throws IOException
   */
  public HMSClient Clone()
      throws InterruptedException, URISyntaxException, TException, LoginException, IOException {
    return new HMSClient(serverURI, confDir);
  }

  private void addResource(Configuration conf, @NotNull String r) throws MalformedURLException {
    File f = new File(confDir + "/" + r);
    if (f.exists() && !f.isDirectory()) {
      LOG.debug("Adding configuration resource {}", confDir+"/"+r);
      conf.addResource(f.toURI().toURL());
    } else {
      LOG.debug("Configuration {} does not exist", confDir+"/"+r);
    }
  }

  /**
   * Create a client to Hive Metastore.
   * If principal is specified, create kerberised client.
   *
   * @param uri server uri
   * @throws MetaException        if fails to login using kerberos credentials
   * @throws IOException          if fails connecting to metastore
   * @throws InterruptedException if interrupted during kerberos setup
   */
  private void getClient(@Nullable URI uri)
      throws TException, IOException, InterruptedException, URISyntaxException, LoginException {
    Configuration conf = new Configuration(); //use Conf instead of MetadataConf to omit init INFO logs
    addResource(conf, HIVE_SITE);
    if (uri != null) {
      MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, uri.toString());
    }

    // Pick up the first URI from the list of available URIs
    serverURI = uri != null ?
        uri :
        new URI(MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS).split(",")[0]);

    String principal = MetastoreConf.getVar(conf, ConfVars.KERBEROS_PRINCIPAL);

    if (principal == null) {
      LOG.debug("Opening non-kerberos connection to HMS {}", serverURI);
      open(conf, serverURI);
      return;
    }

    LOG.debug("Opening kerberos connection to HMS {}", serverURI);
    addResource(conf, CORE_SITE);

    Configuration hadoopConf = new Configuration();
    addResource(hadoopConf, HIVE_SITE);
    addResource(hadoopConf, CORE_SITE);

    // Kerberos magic
    UserGroupInformation.setConfiguration(hadoopConf);
    UserGroupInformation.getLoginUser()
        .doAs((PrivilegedExceptionAction<TTransport>)
            () -> open(conf, serverURI));
  }

  /**
   * Return True if given Hive database excists
   * @param dbName database name
   * @return True iff dbName exists
   * @throws TException if there is Thrift error
   */
  public boolean dbExists(@NotNull String dbName) throws TException {
    return getAllDatabases(dbName).contains(dbName);
  }

  /**
   * Return True if specified table exists
   *
   * @param dbName Database name
   * @param tableName Table name
   * @return True iff table exists
   * @throws TException if there is Thrift error
   */
  public boolean tableExists(@NotNull String dbName, @NotNull String tableName) throws TException {
    return getAllTables(dbName, tableName).contains(tableName);
  }

  /**
   * Get full Database information.
   *
   * @param dbName Database name.
   * @return
   * @throws TException if there is Thrift error
   */
  public Database getDatabase(@NotNull String dbName) throws TException {
    return client.get_database(dbName);
  }

  /**
   * Return all databases with name matching the filter.
   *
   * @param filter Regexp. Can be null or empty in which case everything matches
   * @return list of database names matching the filter
   * @throws TException when the call fails
   */
  public Set<String> getAllDatabases(@Nullable String filter) throws TException {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.get_all_databases());
    }
    return client.get_all_databases()
        .stream()
        .filter(n -> n.matches(filter))
        .collect(Collectors.toSet());
  }

  /**
   * Get all table names for a database matching the filter.
   * If filter is null or empty, return all table names. This call does not use
   * native HMS table name matching, it uses client-side regexp matching instead.
   *
   * @param dbName Database name
   * @param filter regexp matching table name
   * @return Set of matching table names or set of all table names
   * @throws TException if there is Thrift error
   */
  public Set<String> getAllTables(@NotNull String dbName, @Nullable String filter) throws TException {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.get_all_tables(dbName));
    }
    return client.get_all_tables(dbName)
        .stream()
        .filter(n -> n.matches(filter))
        .collect(Collectors.toSet());
  }

  /**
   * Get list of tables within a database
   * @param dbName Database name
   * @param tableNames All tables of interest
   */
  public List<Table> getTableObjects(@NotNull  String dbName, @NotNull List<String> tableNames)
      throws TException {
    return client.get_table_objects_by_name(dbName, tableNames);
  }

  /**
   * Create database with the given name if it doesn't exist
   *
   * @param name database name
   * @return True if successful, false otherwise
   * @throws TException if the call fails
   */
  public boolean createDatabase(@NotNull String name) throws TException {
    return createDatabase(name, null, null, null);
  }

  /**
   * Create database if it doesn't exist
   * @param name Database name
   * @param description Database description
   * @param location Database location
   * @param params Database params
   * @return True if successful, false otherwise
   * @throws TException if call fails
   */
  public boolean createDatabase(@NotNull String name,
                      @Nullable String description,
                      @Nullable String location,
                      @Nullable Map<String, String> params)
      throws TException {
    Database db = new Database(name, description, location, params);
    client.create_database(db);
    return true;
  }

  public boolean createDatabase(Database db) throws TException {
    client.create_database(db);
    return true;
  }

  public boolean dropDatabase(@NotNull String dbName) throws TException {
    client.drop_database(dbName, true, true);
    return true;
  }

  public boolean createTable(Table table) throws TException {
    client.create_table(table);
    return true;
  }

  public boolean dropTable(@NotNull String dbName, @NotNull String tableName) throws TException {
    client.drop_table(dbName, tableName, true);
    return true;
  }

  public Table getTable(@NotNull String dbName, @NotNull String tableName) throws TException {
    return client.get_table(dbName, tableName);
  }

  public Partition createPartition(@NotNull Table table, @NotNull List<String> values) throws TException {
    return client.add_partition(new Util.PartitionBuilder(table).withValues(values).build());
  }

  public Partition addPartition(@NotNull Partition partition) throws TException {
    return client.add_partition(partition);
  }

  public void addPartitions(List<Partition> partitions) throws TException {
    client.add_partitions(partitions);
  }


  public List<Partition> listPartitions(@NotNull String dbName,
                                 @NotNull String tableName) throws TException {
    return client.get_partitions(dbName, tableName, (short) -1);
  }

  public Long getCurrentNotificationId() throws TException {
    return client.get_current_notificationEventId().getEventId();
  }

  public List<String> getPartitionNames(@NotNull String dbName,
                                 @NotNull String tableName) throws TException {
    return client.get_partition_names(dbName, tableName, (short) -1);
  }

  public boolean dropPartition(@NotNull String dbName, @NotNull String tableName,
                               @NotNull List<String> arguments)
      throws TException {
    return client.drop_partition(dbName, tableName, arguments, true);
  }

  public List<Partition> getPartitions(@NotNull String dbName, @NotNull String tableName) throws TException {
    return client.get_partitions(dbName, tableName, (short)-1);
  }

  public DropPartitionsResult dropPartitions(@NotNull String dbName, @NotNull String tableName,
                                      @Nullable List<String> partNames) throws TException {
    if (partNames == null) {
      return dropPartitions(dbName, tableName, getPartitionNames(dbName, tableName));
    }
    if (partNames.isEmpty()) {
      return null;
    }
    return client.drop_partitions_req(new DropPartitionsRequest(dbName,
        tableName, RequestPartsSpec.names(partNames)));
  }

  public List<Partition> getPartitionsByNames(@NotNull String dbName, @NotNull String tableName,
                                       @Nullable List<String>names) throws TException {
    if (names == null) {
      return client.get_partitions_by_names(dbName, tableName,
          getPartitionNames(dbName, tableName));
    }
    return client.get_partitions_by_names(dbName, tableName, names);
  }

  public boolean alterTable(@NotNull String dbName, @NotNull String tableName, @NotNull Table newTable)
      throws TException {
    client.alter_table(dbName, tableName, newTable);
    return true;
  }

  public void alterPartition(@NotNull String dbName, @NotNull String tableName,
      @NotNull Partition partition) throws TException {
    client.alter_partition(dbName, tableName, partition);
  }

  public void alterPartitions(@NotNull String dbName, @NotNull String tableName,
      @NotNull List<Partition> partitions) throws TException {
    client.alter_partitions(dbName, tableName, partitions);
  }

  public void appendPartition(@NotNull String dbName, @NotNull String tableName,
      @NotNull List<String> partitionValues) throws TException {
    client.append_partition_with_environment_context(dbName, tableName, partitionValues, null);
  }

  private TTransport open(Configuration conf, @NotNull URI uri) throws
      TException, IOException, LoginException, IllegalArgumentException {
    boolean useSSL = MetastoreConf.getBoolVar(conf, ConfVars.USE_SSL);
    boolean useSasl = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_SASL);
    String clientAuthMode = MetastoreConf.getVar(conf, ConfVars.METASTORE_CLIENT_AUTH_MODE);
    boolean useFramedTransport = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_FRAMED_TRANSPORT);
    boolean useCompactProtocol = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_COMPACT_PROTOCOL);
    int clientSocketTimeout = (int) MetastoreConf.getTimeVar(conf,
        ConfVars.CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);

    if (clientAuthMode != null && "PLAIN".equalsIgnoreCase(clientAuthMode)) {
      throw new IllegalArgumentException("HMS Password Auth mode is not supported!");
    }

    LOG.debug("Connecting to {}, framedTransport = {}", uri, useFramedTransport);

    if (useSSL) {
      try {
        String trustStorePath = MetastoreConf.getVar(conf, ConfVars.SSL_TRUSTSTORE_PATH).trim();
        if (trustStorePath.isEmpty()) {
          throw new IllegalArgumentException(ConfVars.SSL_TRUSTSTORE_PATH.toString()
              + " Not configured for SSL connection");
        }
        String trustStorePassword =
            MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.SSL_TRUSTSTORE_PASSWORD);
        String trustStoreType =
            MetastoreConf.getVar(conf, ConfVars.SSL_TRUSTSTORE_TYPE).trim();
        String trustStoreAlgorithm =
            MetastoreConf.getVar(conf, ConfVars.SSL_TRUSTMANAGERFACTORY_ALGORITHM).trim();
        // Create an SSL socket and connect
        transport = SecurityUtils.getSSLSocket(uri.getHost(), uri.getPort(), clientSocketTimeout,
            trustStorePath, trustStorePassword, trustStoreType, trustStoreAlgorithm );
        LOG.debug("Opened an SSL connection to metastore");
      } catch(IOException e) {
        throw new IllegalArgumentException(e);
      } catch(TTransportException e) {
        throw new MetaException(e.toString());
      }
    } else {
      transport = new TSocket(uri.getHost(), uri.getPort(), clientSocketTimeout);
    }

    if (useSasl) {
      LOG.debug("Using SASL authentication");
      HadoopThriftAuthBridge.Client authBridge =
          HadoopThriftAuthBridge.getBridge().createClient();
      // check if we should use delegation tokens to authenticate
      // the call below gets hold of the tokens if they are set up by hadoop
      // this should happen on the map/reduce tasks if the client added the
      // tokens into hadoop's credential store in the front end during job
      // submission.
      String tokenSig = MetastoreConf.getVar(conf, ConfVars.TOKEN_SIGNATURE);
      // tokenSig could be null
      String tokenStrForm = SecurityUtils.getTokenStrForm(tokenSig);
      if (tokenStrForm != null) {
        LOG.debug("Using delegation tokens");
        // authenticate using delegation tokens via the "DIGEST" mechanism
        transport = authBridge.createClientTransport(null, uri.getHost(),
            "DIGEST", tokenStrForm, transport,
            MetaStoreUtils.getMetaStoreSaslProperties(conf, useSSL));
      } else {
        LOG.debug("Using principal");
        String principalConfig =
            MetastoreConf.getVar(conf, ConfVars.KERBEROS_PRINCIPAL);
        LOG.debug("Using principal {}", principalConfig);
        transport = authBridge.createClientTransport(
            principalConfig, uri.getHost(), "KERBEROS", null,
            transport, MetaStoreUtils.getMetaStoreSaslProperties(conf, useSSL));
      }
    } else {
      if (useFramedTransport) {
        transport = new TFramedTransport(transport);
      }
    }

    TProtocol protocol = useCompactProtocol ?
        new TCompactProtocol(transport) :
        new TBinaryProtocol(transport);
    client = new ThriftHiveMetastore.Client(protocol);
    transport.open();
    if (!useSasl && MetastoreConf.getBoolVar(conf, ConfVars.EXECUTE_SET_UGI)) {
      UserGroupInformation ugi = SecurityUtils.getUGI();
      client.set_ugi(ugi.getUserName(), Arrays.asList(ugi.getGroupNames()));
      LOG.debug("Set UGI for {}", ugi.getUserName());
    }
    LOG.debug("Connected to metastore, using compact protocol = {}", useCompactProtocol);
    return transport;
  }

  @Override
  public void close() throws Exception {
    if ((transport != null) && transport.isOpen()) {
      LOG.debug("Closing thrift transport");
      transport.close();
    }
  }
}
