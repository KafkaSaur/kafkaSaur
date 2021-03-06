/** @format */

import { Encoder } from '../../../encoder.ts';
import apiKeys from '../../apiKeys.ts';
const apiKey = apiKeys.OffsetCommit;
/**
 * OffsetCommit Request (Version: 2) => group_id group_generation_id member_id retention_time [topics]
 *   group_id => STRING
 *   group_generation_id => INT32
 *   member_id => STRING
 *   retention_time => INT64
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition offset metadata
 *       partition => INT32
 *       offset => INT64
 *       metadata => NULLABLE_STRING
 */

export default ({
  groupId,
  groupGenerationId,
  memberId,
  retentionTime,
  topics,
}: any) => ({
  apiKey,
  apiVersion: 2,
  apiName: 'OffsetCommit',
  //deno-lint-ignore require-await
  encode: async () => {
    return new Encoder()
      .writeString(groupId)
      .writeInt32(groupGenerationId)
      .writeString(memberId)
      .writeInt64(retentionTime)
      .writeArray(topics.map(encodeTopic));
  },
});

const encodeTopic = ({ topic, partitions }: any) => {
  return new Encoder()
    .writeString(topic)
    .writeArray(partitions.map(encodePartition));
};

const encodePartition = ({ partition, offset, metadata = null }: any) => {
  return new Encoder()
    .writeInt32(partition)
    .writeInt64(offset)
    .writeString(metadata);
};
