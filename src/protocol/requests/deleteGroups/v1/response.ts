/** @format */

import response from '../v0/response.ts';

const parse = response.parse;
const decodeV0 = response.decode;

/**
 * Starting in version 1, on quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * DeleteGroups Response (Version: 1) => throttle_time_ms [results]
 *  throttle_time_ms => INT32
 *  results => group_id error_code
 *    group_id => STRING
 *    error_code => INT16
 */

const decode = async (rawData: any) => {
  const decoded: any = await decodeV0(rawData);

  return {
    ...decoded,
    throttleTime: 0,
    clientSideThrottleTime: decoded.throttleTime,
  };
};

export default {
  decode,
  parse,
};
