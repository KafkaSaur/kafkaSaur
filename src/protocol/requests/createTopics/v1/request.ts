import {Encoder} from '../../../encoder.ts'
import  apiKeys  from '../../apiKeys.ts'
const apiKey = apiKeys.CreateTopics
/**
 *CreateTopics Request (Version: 1) => [create_topic_requests] timeout validate_only
 *  create_topic_requests => topic num_partitions replication_factor [replica_assignment] [config_entries]
 *    topic => STRING
 *    num_partitions => INT32
 *    replication_factor => INT16
 *    replica_assignment => partition [replicas]
 *      partition => INT32
 *      replicas => INT32
 *    config_entries => config_name config_value
 *      config_name => STRING
 *      config_value => NULLABLE_STRING
 *  timeout => INT32
 *  validate_only => BOOLEAN
 */

export  default ({
  topics,
  validateOnly = false,
  timeout = 5000
}: any) => ({
  apiKey,
  apiVersion: 1,
  apiName: 'CreateTopics',
  //deno-lint-ignore require-await
  encode: async () => {
    return new Encoder()
      .writeArray(topics.map(encodeTopics))
      .writeInt32(timeout)
      .writeBoolean(validateOnly)
  },
})

const encodeTopics = ({
  topic,
  numPartitions = 1,
  replicationFactor = 1,
  replicaAssignment = [],
  configEntries = []
}: any) => {
  return new Encoder()
    .writeString(topic)
    .writeInt32(numPartitions)
    .writeInt16(replicationFactor)
    .writeArray(replicaAssignment.map(encodeReplicaAssignment))
    .writeArray(configEntries.map(encodeConfigEntries))
}

const encodeReplicaAssignment = ({
  partition,
  replicas
}: any) => {
  return new Encoder().writeInt32(partition).writeArray(replicas)
}

const encodeConfigEntries = ({
  name,
  value
}: any) => {
  return new Encoder().writeString(name).writeString(value)
}
