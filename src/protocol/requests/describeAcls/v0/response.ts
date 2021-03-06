import {Decoder} from '../../../decoder.ts';
import { failure, createErrorFromCode } from '../../../error.ts'

/**
 * DescribeAcls Response (Version: 0) => throttle_time_ms error_code error_message [resources]
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => NULLABLE_STRING
 *   resources => resource_type resource_name [acls]
 *     resource_type => INT8
 *     resource_name => STRING
 *     acls => principal host operation permission_type
 *       principal => STRING
 *       host => STRING
 *       operation => INT8
 *       permission_type => INT8
 */

const decodeAcls = (decoder: any) => ({
  principal: decoder.readString(),
  host: decoder.readString(),
  operation: decoder.readInt8(),
  permissionType: decoder.readInt8()
})

const decodeResources = (decoder: any) => ({
  resourceType: decoder.readInt8(),
  resourceName: decoder.readString(),
  acls: decoder.readArray(decodeAcls)
})

const decode = async (rawData: any) => {
  const decoder = new Decoder(rawData)
  const throttleTime = decoder.readInt32()
  const errorCode = decoder.readInt16()
  const errorMessage = decoder.readString()
  const resources = decoder.readArray(decodeResources)

  return {
    throttleTime,
    errorCode,
    errorMessage,
    resources,
  }
}

const parse = async (data: any) => {
  if (failure(data.errorCode)) {
    throw createErrorFromCode(data.errorCode)
  }

  return data
}

export default {
  decode,
  parse,
}
