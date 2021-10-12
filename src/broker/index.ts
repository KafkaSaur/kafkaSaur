/** @format */

import Long from '../utils/long.ts';
import Lock from '../utils/lock.ts';
import CompressionObj from '../protocol/message/compression/index.ts';
import { requests, lookup } from '../protocol/requests/index.ts';
import { KafkaJSNonRetriableError } from '../errors.ts';
import apiKeys from '../protocol/requests/apiKeys.ts';
import { SASLAuthenticator } from './saslAuthenticator/index.ts';
import shuffle from '../utils/shuffle.ts';
import process from 'https://deno.land/std@0.110.0/node/process.ts';

const { Types } = CompressionObj;

const PRIVATE = {
  //typed as key of broker in order to index broker class
  SHOULD_REAUTHENTICATE: Symbol(
    'private:Broker:shouldReauthenticate'
  ) as unknown as keyof Broker,
  SEND_REQUEST: Symbol('private:Broker:sendRequest') as unknown as keyof Broker,
};

/** @type {import("../protocol/requests").Lookup} */
const notInitializedLookup = () => {
  throw new Error('Broker not connected');
};

//HARDCODED VERSIONS BELOW
let tempVersions = {
  '0': { minVersion: 0, maxVersion: 5 },
  '1': { minVersion: 0, maxVersion: 7 },
  '2': { minVersion: 0, maxVersion: 2 },
  '3': { minVersion: 0, maxVersion: 5 },
  '4': { minVersion: 0, maxVersion: 1 },
  '5': { minVersion: 0, maxVersion: 0 },
  '6': { minVersion: 0, maxVersion: 4 },
  '7': { minVersion: 0, maxVersion: 1 },
  '8': { minVersion: 0, maxVersion: 3 },
  '9': { minVersion: 0, maxVersion: 3 },
  '10': { minVersion: 0, maxVersion: 1 },
  '11': { minVersion: 0, maxVersion: 2 },
  '12': { minVersion: 0, maxVersion: 1 },
  '13': { minVersion: 0, maxVersion: 1 },
  '14': { minVersion: 0, maxVersion: 1 },
  '15': { minVersion: 0, maxVersion: 1 },
  '16': { minVersion: 0, maxVersion: 1 },
  '17': { minVersion: 0, maxVersion: 1 },
  '18': { minVersion: 0, maxVersion: 1 },
  '19': { minVersion: 0, maxVersion: 2 },
  '20': { minVersion: 0, maxVersion: 1 },
  '21': { minVersion: 0, maxVersion: 0 },
  '22': { minVersion: 0, maxVersion: 0 },
  '23': { minVersion: 0, maxVersion: 0 },
  '24': { minVersion: 0, maxVersion: 0 },
  '25': { minVersion: 0, maxVersion: 0 },
  '26': { minVersion: 0, maxVersion: 0 },
  '27': { minVersion: 0, maxVersion: 0 },
  '28': { minVersion: 0, maxVersion: 0 },
  '29': { minVersion: 0, maxVersion: 0 },
  '30': { minVersion: 0, maxVersion: 0 },
  '31': { minVersion: 0, maxVersion: 0 },
  '32': { minVersion: 0, maxVersion: 1 },
  '33': { minVersion: 0, maxVersion: 0 },
  '34': { minVersion: 0, maxVersion: 0 },
  '35': { minVersion: 0, maxVersion: 0 },
  '36': { minVersion: 0, maxVersion: 0 },
  '37': { minVersion: 0, maxVersion: 0 },
  '38': { minVersion: 0, maxVersion: 0 },
  '39': { minVersion: 0, maxVersion: 0 },
  '40': { minVersion: 0, maxVersion: 0 },
  '41': { minVersion: 0, maxVersion: 0 },
  '42': { minVersion: 0, maxVersion: 0 }
}

/**
 * Each node in a Kafka cluster is called broker. This class contains
 * the high-level operations a node can perform.
 *
 * @type {import("../../types").Broker}
 */

export class Broker {
  allowAutoTopicCreation: any;
  authenticatedAt: any;
  authenticationTimeout: any;
  brokerAddress: any;
  connection: any;
  lock: any;
  logger: any;
  lookupRequest: any;
  nodeId: any;
  reauthenticationThreshold: any;
  rootLogger: any;
  sessionLifetime: any;
  supportAuthenticationProtocol: any;
  versions: any;
  /**
   * @param {Object} options
   * @param {import("../network/connection")} options.connection
   * @param {import("../../types").Logger} options.logger
   * @param {number} [options.nodeId]
   * @param {import("../../types").ApiVersions} [options.versions=null] The object with all available versions and APIs
   *                                 supported by this cluster. The output of broker#apiVersions
   * @param {number} [options.authenticationTimeout=1000]
   * @param {number} [options.reauthenticationThreshold=10000]
   * @param {boolean} [options.allowAutoTopicCreation=true] If this and the broker config 'auto.create.topics.enable'
   *                                                are true, topics that don't exist will be created when
   *                                                fetching metadata.
   * @param {boolean} [options.supportAuthenticationProtocol=null] If the server supports the SASLAuthenticate protocol
   */
  constructor({
    connection,
    logger,
    nodeId = null,
    versions = null,
    authenticationTimeout = 1000,
    reauthenticationThreshold = 10000,
    allowAutoTopicCreation = true,
    supportAuthenticationProtocol = null,
  }: any) {
    this.connection = connection;
    this.nodeId = nodeId;
    this.rootLogger = logger;
    this.logger = logger.namespace('Broker');
    this.versions = null;
    this.authenticationTimeout = authenticationTimeout;
    this.reauthenticationThreshold = reauthenticationThreshold;
    this.allowAutoTopicCreation = allowAutoTopicCreation;
    this.supportAuthenticationProtocol = supportAuthenticationProtocol;
    this.authenticatedAt = null;
    this.sessionLifetime = Long.ZERO;
    // The lock timeout has twice the connectionTimeout because the same timeout is used
    // for the first apiVersions call
    const lockTimeout =
      2 * this.connection.connectionTimeout + this.authenticationTimeout;
    this.brokerAddress = `${this.connection.host}:${this.connection.port}`;
    this.lock = new Lock({
      timeout: lockTimeout,
      description: `connect to broker ${this.brokerAddress}`,
    });
    this.lookupRequest = notInitializedLookup;
  }
  /**
   * @public
   * @returns {boolean}
   */
  isConnected() {
    const { connected, sasl } = this.connection;
    const isAuthenticated =
      this.authenticatedAt != null && !this[PRIVATE.SHOULD_REAUTHENTICATE]();
    return sasl ? connected && isAuthenticated : connected;
  }
  /**
   * @public
   * @returns {Promise}
   */
  async connect() {
    console.log('inside broker/connect - top level')
    try {
      await this.lock.acquire();
      console.log('inside broker/connect - after lock acquire')
      if (this.isConnected()) {
        return;
      }
      this.authenticatedAt = null;
      await this.connection.connect();
      console.log('this.connection, ', this.connection)
      console.log('this.versions', this.versions)
      if (!this.versions) {
        console.log('inside !this.versions')
        this.versions = await this.apiVersions();
        console.log('after this.versions check - this.versions is ', this.versions)
      }
      this.lookupRequest = lookup(this.versions);
      if (this.supportAuthenticationProtocol === null) {
        console.log('supportAuthenticationProtocol is null - inside if block')
        try {
          this.lookupRequest(
            apiKeys.SaslAuthenticate,
            requests.SaslAuthenticate
          );
          this.supportAuthenticationProtocol = true;
        } catch (_) {
          this.supportAuthenticationProtocol = false;
        }
        this.logger.debug(`Verified support for SaslAuthenticate`, {
          broker: this.brokerAddress,
          supportAuthenticationProtocol: this.supportAuthenticationProtocol,
        });
      }
      if (this.authenticatedAt == null && this.connection.sasl) {
        console.log('this.authenticatedAt == null && this.connection.sasl')
        const authenticator = new SASLAuthenticator(
          this.connection,
          this.rootLogger,
          this.versions,
          this.supportAuthenticationProtocol
        );
        console.log('before authenticator.authenticate()')
        await authenticator.authenticate();
        console.log('after authenticator.authenticate()')
        this.authenticatedAt = process.hrtime();
        this.sessionLifetime = Long.fromValue(authenticator.sessionLifetime);
      }
    } finally {
      await this.lock.release();
      console.log('*****lock released*****')
    }
  }
  /**
   * @public
   * @returns {Promise}
   */
  async disconnect() {
    this.authenticatedAt = null;
    await this.connection.disconnect();
  }
  /**
   * @public
   * @returns {Promise<import("../../types").ApiVersions>}
   */
  async apiVersions() {
    let response;
    const availableVersions = requests.ApiVersions.versions
      .map(Number)
      .sort()
      .reverse();
    // Find the best version implemented by the server
    for (const candidateVersion of availableVersions) {
      try {
        const apiVersions = requests.ApiVersions.protocol({
          version: candidateVersion,
        });
        response = await this[PRIVATE.SEND_REQUEST]({
          ...apiVersions(),
          requestTimeout: this.connection.connectionTimeout,
        });
        break;
      } catch (e: any) {
        if (e.type !== 'UNSUPPORTED_VERSION') {
          throw e;
        }
      }
    }
    if (!response) {
      throw new KafkaJSNonRetriableError('API Versions not supported');
    }
    return response.apiVersions.reduce(
      (obj: any, version: any) =>
        Object.assign(obj, {
          [version.apiKey]: {
            minVersion: version.minVersion,
            maxVersion: version.maxVersion,
          },
        }),
      {}
    );
  }
  /**
   * @public
   * @type {import("../../types").Broker['metadata']}
   * @param {string[]} [topics=[]] An array of topics to fetch metadata for.
   *                            If no topics are specified fetch metadata for all topics
   */
  async metadata(topics = []) {
    const metadata = this.lookupRequest(apiKeys.Metadata, requests.Metadata);
    const shuffledTopics = shuffle(topics);
    return await this[PRIVATE.SEND_REQUEST](
      metadata({
        topics: shuffledTopics,
        allowAutoTopicCreation: this.allowAutoTopicCreation,
      })
    );
  }
  /**
   * @public
   * @param {Object} request
   * @param {Array} request.topicData An array of messages per topic and per partition, example:
   *                          [
   *                            {
   *                              topic: 'test-topic-1',
   *                              partitions: [
   *                                {
   *                                  partition: 0,
   *                                  firstSequence: 0,
   *                                  messages: [
   *                                    { key: '1', value: 'A' },
   *                                    { key: '2', value: 'B' },
   *                                  ]
   *                                },
   *                                {
   *                                  partition: 1,
   *                                  firstSequence: 0,
   *                                  messages: [
   *                                    { key: '3', value: 'C' },
   *                                  ]
   *                                }
   *                              ]
   *                            },
   *                            {
   *                              topic: 'test-topic-2',
   *                              partitions: [
   *                                {
   *                                  partition: 4,
   *                                  firstSequence: 0,
   *                                  messages: [
   *                                    { key: '32', value: 'E' },
   *                                  ]
   *                                },
   *                              ]
   *                            },
   *                          ]
   * @param {number} [request.acks=-1] Control the number of required acks.
   *                           -1 = all replicas must acknowledge
   *                            0 = no acknowledgments
   *                            1 = only waits for the leader to acknowledge
   * @param {number} [request.timeout=30000] The time to await a response in ms
   * @param {string} [request.transactionalId=null]
   * @param {number} [request.producerId=-1] Broker assigned producerId
   * @param {number} [request.producerEpoch=0] Broker assigned producerEpoch
   * @param {import("../../types").CompressionTypes} [request.compression=CompressionTypes.None] Compression codec
   * @returns {Promise}
   */
  async produce({
    topicData,
    transactionalId,
    producerId,
    producerEpoch,
    acks = -1,
    timeout = 30000,
    compression = Types.None,
  }: any) {
    const produce = this.lookupRequest(apiKeys.Produce, requests.Produce);
    return await this[PRIVATE.SEND_REQUEST](
      produce({
        acks,
        timeout,
        compression,
        topicData,
        transactionalId,
        producerId,
        producerEpoch,
      })
    );
  }
  /**
   * @public
   * @param {Object} request
   * @param {number} [request.replicaId=-1] Broker id of the follower. For normal consumers, use -1
   * @param {number} [request.isolationLevel=1] This setting controls the visibility of transactional records. Default READ_COMMITTED.
   * @param {number} [request.maxWaitTime=5000] Maximum time in ms to wait for the response
   * @param {number} [request.minBytes=1] Minimum bytes to accumulate in the response
   * @param {number} [request.maxBytes=10485760] Maximum bytes to accumulate in the response. Note that this is
   *                                   not an absolute maximum, if the first message in the first non-empty
   *                                   partition of the fetch is larger than this value, the message will still
   *                                   be returned to ensure that progress can be made. Default 10MB.
   * @param {Array} request.topics Topics to fetch
   *                        [
   *                          {
   *                            topic: 'topic-name',
   *                            partitions: [
   *                              {
   *                                partition: 0,
   *                                fetchOffset: '4124',
   *                                maxBytes: 2048
   *                              }
   *                            ]
   *                          }
   *                        ]
   * @param {string} [request.rackId=''] A rack identifier for this client. This can be any string value which indicates where this
   *                           client is physically located. It corresponds with the broker config `broker.rack`.
   * @returns {Promise}
   */
  async fetch({
    replicaId,
    isolationLevel,
    maxWaitTime = 5000,
    minBytes = 1,
    maxBytes = 10485760,
    topics,
    rackId = '',
  }: any) {
    // TODO: validate topics not null/empty
    const fetch = this.lookupRequest(apiKeys.Fetch, requests.Fetch);
    // Shuffle topic-partitions to ensure fair response allocation across partitions (KIP-74)
    const flattenedTopicPartitions = topics.reduce(
      (topicPartitions: any, { topic, partitions }: any) => {
        partitions.forEach((partition: any) => {
          topicPartitions.push({ topic, partition });
        });
        return topicPartitions;
      },
      []
    );
    const shuffledTopicPartitions = shuffle(flattenedTopicPartitions);
    // Consecutive partitions for the same topic can be combined into a single `topic` entry
    const consolidatedTopicPartitions = shuffledTopicPartitions.reduce(
      (topicPartitions: any, { topic, partition }: any) => {
        const last = topicPartitions[topicPartitions.length - 1];
        if (last != null && last.topic === topic) {
          topicPartitions[topicPartitions.length - 1].partitions.push(
            partition
          );
        } else {
          topicPartitions.push({ topic, partitions: [partition] });
        }
        return topicPartitions;
      },
      []
    );
    return await this[PRIVATE.SEND_REQUEST](
      fetch({
        replicaId,
        isolationLevel,
        maxWaitTime,
        minBytes,
        maxBytes,
        topics: consolidatedTopicPartitions,
        rackId,
      })
    );
  }
  /**
   * @public
   * @param {object} request
   * @param {string} request.groupId The group id
   * @param {number} request.groupGenerationId The generation of the group
   * @param {string} request.memberId The member id assigned by the group coordinator
   * @returns {Promise}
   */
  async heartbeat({ groupId, groupGenerationId, memberId }: any) {
    const heartbeat = this.lookupRequest(apiKeys.Heartbeat, requests.Heartbeat);
    return await this[PRIVATE.SEND_REQUEST](
      heartbeat({ groupId, groupGenerationId, memberId })
    );
  }
  /**
   * @public
   * @param {object} request
   * @param {string} request.groupId The unique group id
   * @param {import("../protocol/coordinatorTypes").CoordinatorType} request.coordinatorType The type of coordinator to find
   * @returns {Promise}
   */
  async findGroupCoordinator({ groupId, coordinatorType }: any) {
    // TODO: validate groupId, mandatory
    const findCoordinator = this.lookupRequest(
      apiKeys.GroupCoordinator,
      requests.GroupCoordinator
    );
    return await this[PRIVATE.SEND_REQUEST](
      findCoordinator({ groupId, coordinatorType })
    );
  }
  /**
   * @public
   * @param {object} request
   * @param {string} request.groupId The unique group id
   * @param {number} request.sessionTimeout The coordinator considers the consumer dead if it receives
   *                                no heartbeat after this timeout in ms
   * @param {number} request.rebalanceTimeout The maximum time that the coordinator will wait for each member
   *                                  to rejoin when rebalancing the group
   * @param {string} [request.memberId=""] The assigned consumer id or an empty string for a new consumer
   * @param {string} [request.protocolType="consumer"] Unique name for class of protocols implemented by group
   * @param {Array} request.groupProtocols List of protocols that the member supports (assignment strategy)
   *                                [{ name: 'AssignerName', metadata: '{"version": 1, "topics": []}' }]
   * @returns {Promise}
   */
  async joinGroup({
    groupId,
    sessionTimeout,
    rebalanceTimeout,
    memberId = '',
    protocolType = 'consumer',
    groupProtocols,
  }: any) {
    const joinGroup = this.lookupRequest(apiKeys.JoinGroup, requests.JoinGroup);
    const makeRequest = (assignedMemberId = memberId) =>
      this[PRIVATE.SEND_REQUEST](
        joinGroup({
          groupId,
          sessionTimeout,
          rebalanceTimeout,
          memberId: assignedMemberId,
          protocolType,
          groupProtocols,
        })
      );
    try {
      return await makeRequest();
    } catch (error: any) {
      if (error.name === 'KafkaJSMemberIdRequired') {
        return makeRequest(error.memberId);
      }
      throw error;
    }
  }
  /**
   * @public
   * @param {object} request
   * @param {string} request.groupId
   * @param {string} request.memberId
   * @returns {Promise}
   */
  async leaveGroup({ groupId, memberId }: any) {
    const leaveGroup = this.lookupRequest(
      apiKeys.LeaveGroup,
      requests.LeaveGroup
    );
    return await this[PRIVATE.SEND_REQUEST](leaveGroup({ groupId, memberId }));
  }
  /**
   * @public
   * @param {object} request
   * @param {string} request.groupId
   * @param {number} request.generationId
   * @param {string} request.memberId
   * @param {object} request.groupAssignment
   * @returns {Promise}
   */
  async syncGroup({ groupId, generationId, memberId, groupAssignment }: any) {
    const syncGroup = this.lookupRequest(apiKeys.SyncGroup, requests.SyncGroup);
    return await this[PRIVATE.SEND_REQUEST](
      syncGroup({
        groupId,
        generationId,
        memberId,
        groupAssignment,
      })
    );
  }
  /**
   * @public
   * @param {object} request
   * @param {number} request.replicaId=-1 Broker id of the follower. For normal consumers, use -1
   * @param {number} request.isolationLevel=1 This setting controls the visibility of transactional records (default READ_COMMITTED, Kafka >0.11 only)
   * @param {TopicPartitionOffset[]} request.topics e.g:
   *
   * @typedef {Object} TopicPartitionOffset
   * @property {string} topic
   * @property {PartitionOffset[]} partitions
   *
   * @typedef {Object} PartitionOffset
   * @property {number} partition
   * @property {number} [timestamp=-1]
   *
   *
   * @returns {Promise}
   */
  async listOffsets({ replicaId, isolationLevel, topics }: any) {
    const listOffsets = this.lookupRequest(
      apiKeys.ListOffsets,
      requests.ListOffsets
    );

    const result = await this[PRIVATE.SEND_REQUEST](
      listOffsets({ replicaId, isolationLevel, topics })
    );
    // ListOffsets >= v1 will return a single `offset` rather than an array of `offsets` (ListOffsets V0).
    // Normalize to just return `offset`.
    for (const response of result.responses) {
      response.partitions = response.partitions.map(
        ({ offsets, ...partitionData }: any) => {
          return offsets
            ? { ...partitionData, offset: offsets.pop() }
            : partitionData;
        }
      );
    }
    return result;
  }
  /**
   * @public
   * @param {object} request
   * @param {string} request.groupId
   * @param {number} request.groupGenerationId
   * @param {string} request.memberId
   * @param {number} [request.retentionTime=-1] -1 signals to the broker that its default configuration
   *                                    should be used.
   * @param {object} request.topics Topics to commit offsets, e.g:
   *                  [
   *                    {
   *                      topic: 'topic-name',
   *                      partitions: [
   *                        { partition: 0, offset: '11' }
   *                      ]
   *                    }
   *                  ]
   * @returns {Promise}
   */
  async offsetCommit({
    groupId,
    groupGenerationId,
    memberId,
    retentionTime,
    topics,
  }: any) {
    const offsetCommit = this.lookupRequest(
      apiKeys.OffsetCommit,
      requests.OffsetCommit
    );

    return await this[PRIVATE.SEND_REQUEST](
      offsetCommit({
        groupId,
        groupGenerationId,
        memberId,
        retentionTime,
        topics,
      })
    );
  }
  /**
   * @public
   * @param {object} request
   * @param {string} request.groupId
   * @param {object} request.topics - If the topic array is null fetch offsets for all topics. e.g:
   *                  [
   *                    {
   *                      topic: 'topic-name',
   *                      partitions: [
   *                        { partition: 0 }
   *                      ]
   *                    }
   *                  ]
   * @returns {Promise}
   */
  async offsetFetch({ groupId, topics }: any) {
    const offsetFetch = this.lookupRequest(
      apiKeys.OffsetFetch,
      requests.OffsetFetch
    );

    return await this[PRIVATE.SEND_REQUEST](offsetFetch({ groupId, topics }));
  }
  /**
   * @public
   * @param {object} request
   * @param {Array} request.groupIds
   * @returns {Promise}
   */
  async describeGroups({ groupIds }: any) {
    const describeGroups = this.lookupRequest(
      apiKeys.DescribeGroups,
      requests.DescribeGroups
    );

    return await this[PRIVATE.SEND_REQUEST](describeGroups({ groupIds }));
  }
  /**
   * @public
   * @param {object} request
   * @param {Array} request.topics e.g:
   *                 [
   *                   {
   *                     topic: 'topic-name',
   *                     numPartitions: 1,
   *                     replicationFactor: 1
   *                   }
   *                 ]
   * @param {boolean} [request.validateOnly=false] If this is true, the request will be validated, but the topic
   *                                       won't be created
   * @param {number} [request.timeout=5000] The time in ms to wait for a topic to be completely created
   *                                on the controller node
   * @returns {Promise}
   */
  async createTopics({ topics, validateOnly = false, timeout = 5000 }: any) {
    const createTopics = this.lookupRequest(
      apiKeys.CreateTopics,
      requests.CreateTopics
    );

    return await this[PRIVATE.SEND_REQUEST](
      createTopics({ topics, validateOnly, timeout })
    );
  }
  /**
   * @public
   * @param {object} request
   * @param {Array} request.topicPartitions e.g:
   *                 [
   *                   {
   *                     topic: 'topic-name',
   *                     count: 3,
   *                     assignments: []
   *                   }
   *                 ]
   * @param {boolean} [request.validateOnly=false] If this is true, the request will be validated, but the topic
   *                                       won't be created
   * @param {number} [request.timeout=5000] The time in ms to wait for a topic to be completely created
   *                                on the controller node
   * @returns {Promise<void>}
   */
  async createPartitions({
    topicPartitions,
    validateOnly = false,
    timeout = 5000,
  }: any) {
    const createPartitions = this.lookupRequest(
      apiKeys.CreatePartitions,
      requests.CreatePartitions
    );
    return await this[PRIVATE.SEND_REQUEST](
      createPartitions({ topicPartitions, validateOnly, timeout })
    );
  }
  /**
   * @public
   * @param {object} request
   * @param {string[]} request.topics An array of topics to be deleted
   * @param {number} [request.timeout=5000] The time in ms to wait for a topic to be completely deleted on the
   *                                controller node. Values <= 0 will trigger topic deletion and return
   *                                immediately
   * @returns {Promise}
   */
  async deleteTopics({ topics, timeout = 5000 }: any) {
    const deleteTopics = this.lookupRequest(
      apiKeys.DeleteTopics,
      requests.DeleteTopics
    );
    return await this[PRIVATE.SEND_REQUEST](deleteTopics({ topics, timeout }));
  }
  /**
   * @public
   * @param {object} request
   * @param {import("../../types").ResourceConfigQuery[]} request.resources
   *                                 [{
   *                                   type: RESOURCE_TYPES.TOPIC,
   *                                   name: 'topic-name',
   *                                   configNames: ['compression.type', 'retention.ms']
   *                                 }]
   * @param {boolean} [request.includeSynonyms=false]
   * @returns {Promise}
   */
  async describeConfigs({ resources, includeSynonyms = false }: any) {
    const describeConfigs = this.lookupRequest(
      apiKeys.DescribeConfigs,
      requests.DescribeConfigs
    );
    return await this[PRIVATE.SEND_REQUEST](
      describeConfigs({ resources, includeSynonyms })
    );
  }
  /**
   * @public
   * @param {object} request
   * @param {import("../../types").IResourceConfig[]} request.resources
   *                                 [{
   *                                  type: RESOURCE_TYPES.TOPIC,
   *                                  name: 'topic-name',
   *                                  configEntries: [
   *                                    {
   *                                      name: 'cleanup.policy',
   *                                      value: 'compact'
   *                                    }
   *                                  ]
   *                                 }]
   * @param {boolean} [request.validateOnly=false]
   * @returns {Promise}
   */
  async alterConfigs({ resources, validateOnly = false }: any) {
    const alterConfigs = this.lookupRequest(
      apiKeys.AlterConfigs,
      requests.AlterConfigs
    );
    return await this[PRIVATE.SEND_REQUEST](
      alterConfigs({ resources, validateOnly })
    );
  }
  /**
   * Send an `InitProducerId` request to fetch a PID and bump the producer epoch.
   *
   * Request should be made to the transaction coordinator.
   * @public
   * @param {object} request
   * @param {number} request.transactionTimeout The time in ms to wait for before aborting idle transactions
   * @param {number} [request.transactionalId] The transactional id or null if the producer is not transactional
   * @returns {Promise}
   */
  async initProducerId({ transactionalId, transactionTimeout }: any) {
    const initProducerId = this.lookupRequest(
      apiKeys.InitProducerId,
      requests.InitProducerId
    );
    return await this[PRIVATE.SEND_REQUEST](
      initProducerId({ transactionalId, transactionTimeout })
    );
  }
  /**
   * Send an `AddPartitionsToTxn` request to mark a TopicPartition as participating in the transaction.
   *
   * Request should be made to the transaction coordinator.
   * @public
   * @param {object} request
   * @param {string} request.transactionalId The transactional id corresponding to the transaction.
   * @param {number} request.producerId Current producer id in use by the transactional id.
   * @param {number} request.producerEpoch Current epoch associated with the producer id.
   * @param {object[]} request.topics e.g:
   *                  [
   *                    {
   *                      topic: 'topic-name',
   *                      partitions: [ 0, 1]
   *                    }
   *                  ]
   * @returns {Promise}
   */
  async addPartitionsToTxn({
    transactionalId,
    producerId,
    producerEpoch,
    topics,
  }: any) {
    const addPartitionsToTxn = this.lookupRequest(
      apiKeys.AddPartitionsToTxn,
      requests.AddPartitionsToTxn
    );
    return await this[PRIVATE.SEND_REQUEST](
      addPartitionsToTxn({ transactionalId, producerId, producerEpoch, topics })
    );
  }
  /**
   * Send an `AddOffsetsToTxn` request.
   *
   * Request should be made to the transaction coordinator.
   * @public
   * @param {object} request
   * @param {string} request.transactionalId The transactional id corresponding to the transaction.
   * @param {number} request.producerId Current producer id in use by the transactional id.
   * @param {number} request.producerEpoch Current epoch associated with the producer id.
   * @param {string} request.groupId The unique group identifier (for the consumer group)
   * @returns {Promise}
   */
  async addOffsetsToTxn({
    transactionalId,
    producerId,
    producerEpoch,
    groupId,
  }: any) {
    const addOffsetsToTxn = this.lookupRequest(
      apiKeys.AddOffsetsToTxn,
      requests.AddOffsetsToTxn
    );
    return await this[PRIVATE.SEND_REQUEST](
      addOffsetsToTxn({ transactionalId, producerId, producerEpoch, groupId })
    );
  }
  /**
   * Send a `TxnOffsetCommit` request to persist the offsets in the `__consumer_offsets` topics.
   *
   * Request should be made to the consumer coordinator.
   * @public
   * @param {object} request
   * @param {OffsetCommitTopic[]} request.topics
   * @param {string} request.transactionalId The transactional id corresponding to the transaction.
   * @param {string} request.groupId The unique group identifier (for the consumer group)
   * @param {number} request.producerId Current producer id in use by the transactional id.
   * @param {number} request.producerEpoch Current epoch associated with the producer id.
   * @param {OffsetCommitTopic[]} request.topics
   *
   * @typedef {Object} OffsetCommitTopic
   * @property {string} topic
   * @property {OffsetCommitTopicPartition[]} partitions
   *
   * @typedef {Object} OffsetCommitTopicPartition
   * @property {number} partition
   * @property {number} offset
   * @property {string} [metadata]
   *
   * @returns {Promise}
   */
  async txnOffsetCommit({
    transactionalId,
    groupId,
    producerId,
    producerEpoch,
    topics,
  }: any) {
    const txnOffsetCommit = this.lookupRequest(
      apiKeys.TxnOffsetCommit,
      requests.TxnOffsetCommit
    );
    return await this[PRIVATE.SEND_REQUEST](
      txnOffsetCommit({
        transactionalId,
        groupId,
        producerId,
        producerEpoch,
        topics,
      })
    );
  }
  /**
   * Send an `EndTxn` request to indicate transaction should be committed or aborted.
   *
   * Request should be made to the transaction coordinator.
   * @public
   * @param {object} request
   * @param {string} request.transactionalId The transactional id corresponding to the transaction.
   * @param {number} request.producerId Current producer id in use by the transactional id.
   * @param {number} request.producerEpoch Current epoch associated with the producer id.
   * @param {boolean} request.transactionResult The result of the transaction (false = ABORT, true = COMMIT)
   * @returns {Promise}
   */
  async endTxn({
    transactionalId,
    producerId,
    producerEpoch,
    transactionResult,
  }: any) {
    const endTxn = this.lookupRequest(apiKeys.EndTxn, requests.EndTxn);
    return await this[PRIVATE.SEND_REQUEST](
      endTxn({ transactionalId, producerId, producerEpoch, transactionResult })
    );
  }
  /**
   * Send request for list of groups
   * @public
   * @returns {Promise}
   */
  async listGroups() {
    const listGroups = this.lookupRequest(
      apiKeys.ListGroups,
      requests.ListGroups
    );
    return await this[PRIVATE.SEND_REQUEST](listGroups());
  }
  /**
   * Send request to delete groups
   * @param {string[]} groupIds
   * @public
   * @returns {Promise}
   */
  async deleteGroups(groupIds: any) {
    const deleteGroups = this.lookupRequest(
      apiKeys.DeleteGroups,
      requests.DeleteGroups
    );
    return await this[PRIVATE.SEND_REQUEST](deleteGroups(groupIds));
  }
  /**
   * Send request to delete records
   * @public
   * @param {object} request
   * @param {TopicPartitionRecords[]} request.topics
   *                          [
   *                            {
   *                              topic: 'my-topic-name',
   *                              partitions: [
   *                                { partition: 0, offset 2 },
   *                                { partition: 1, offset 4 },
   *                              ],
   *                            }
   *                          ]
   * @returns {Promise<Array>} example:
   *                          {
   *                            throttleTime: 0
   *                           [
   *                              {
   *                                topic: 'my-topic-name',
   *                                partitions: [
   *                                 { partition: 0, lowWatermark: '2n', errorCode: 0 },
   *                                 { partition: 1, lowWatermark: '4n', errorCode: 0 },
   *                               ],
   *                             },
   *                           ]
   *                          }
   *
   * @typedef {object} TopicPartitionRecords
   * @property {string} topic
   * @property {PartitionRecord[]} partitions
   *
   * @typedef {object} PartitionRecord
   * @property {number} partition
   * @property {number} offset
   */
  async deleteRecords({ topics }: any) {
    const deleteRecords = this.lookupRequest(
      apiKeys.DeleteRecords,
      requests.DeleteRecords
    );
    return await this[PRIVATE.SEND_REQUEST](deleteRecords({ topics }));
  }
  /**
   * @public
   * @param {object} request
   * @param {import("../../types").AclEntry[]} request.acl e.g:
   *                 [
   *                   {
   *                     resourceType: AclResourceTypes.TOPIC,
   *                     resourceName: 'topic-name',
   *                     resourcePatternType: ResourcePatternTypes.LITERAL,
   *                     principal: 'User:bob',
   *                     host: '*',
   *                     operation: AclOperationTypes.ALL,
   *                     permissionType: AclPermissionTypes.DENY,
   *                   }
   *                 ]
   * @returns {Promise<void>}
   */
  async createAcls({ acl }: any) {
    const createAcls = this.lookupRequest(
      apiKeys.CreateAcls,
      requests.CreateAcls
    );
    return await this[PRIVATE.SEND_REQUEST](createAcls({ creations: acl }));
  }
  /**
   * @public
   * @param {import("../../types").AclEntry} aclEntry
   * @returns {Promise<void>}
   */
  async describeAcls({
    resourceType,
    resourceName,
    resourcePatternType,
    principal,
    host,
    operation,
    permissionType,
  }: any) {
    const describeAcls = this.lookupRequest(
      apiKeys.DescribeAcls,
      requests.DescribeAcls
    );
    return await this[PRIVATE.SEND_REQUEST](
      describeAcls({
        resourceType,
        resourceName,
        resourcePatternType,
        principal,
        host,
        operation,
        permissionType,
      })
    );
  }
  /**
   * @public
   * @param {Object} request
   * @param {import("../../types").AclEntry[]} request.filters
   * @returns {Promise<void>}
   */
  async deleteAcls({ filters }: any) {
    const deleteAcls = this.lookupRequest(
      apiKeys.DeleteAcls,
      requests.DeleteAcls
    );
    return await this[PRIVATE.SEND_REQUEST](deleteAcls({ filters }));
  }
  /***
   * @private
   */
  [(PRIVATE as any).SHOULD_REAUTHENTICATE]() {
    if (this.sessionLifetime.equals(Long.ZERO)) {
      return false;
    }
    if (this.authenticatedAt == null) {
      return true;
    }
    const [secondsSince, remainingNanosSince] = process.hrtime(
      this.authenticatedAt
    );
    const millisSince = Long.fromValue(secondsSince)
      .multiply(1000)
      .add(Long.fromValue(remainingNanosSince).divide(1000000));
    const reauthenticateAt = millisSince.add(this.reauthenticationThreshold);
    return reauthenticateAt.greaterThanOrEqual(this.sessionLifetime);
  }
  /**
   * @private
   */
  async [(PRIVATE as any).SEND_REQUEST](protocolRequest: any) {
    try {
      //return await this.connection.send(protocolRequest);
      console.log('UPCOMING REQUEST IS', protocolRequest)
      return await this.connection.send(protocolRequest);
    } catch (e: any) {
      if (e.name === 'KafkaJSConnectionClosedError') {
        await this.disconnect();
      }
      throw e;
    }
  }
}
