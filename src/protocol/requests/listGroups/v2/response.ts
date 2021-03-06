import {Buffer} from 'https://deno.land/std@0.110.0/node/buffer.ts'
import _decode from '../v1/response.ts'
const decodeV1 = _decode.decode
const parse = _decode.parse
/**
 * In version 2 on quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * ListGroups Response (Version: 2) => error_code [groups]
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   groups => group_id protocol_type
 *     group_id => STRING
 *     protocol_type => STRING
 */
const decode = async (rawData: Buffer) => {
  const decoded = await decodeV1(rawData)

  return {
    ...decoded,
    throttleTime: 0,
    clientSideThrottleTime: decoded.throttleTime,
  }
}

export default {
  decode,
  parse,
}
