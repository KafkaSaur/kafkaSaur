import requestV0 from './v0/request.ts'
import responseV0 from './v0/response.ts'
import requestV1 from './v1/request.ts'
import responseV1 from './v1/response.ts'

const versions: Record<number, any> = {
  0: ({
    transactionalId,
    groupId,
    producerId,
    producerEpoch,
    topics
  }: any) => {
    const request = requestV0
    const response = responseV0
    return {
      request: request({ transactionalId, groupId, producerId, producerEpoch, topics }),
      response,
    }
  },
  1: ({
    transactionalId,
    groupId,
    producerId,
    producerEpoch,
    topics
  }: any) => {
    const request = requestV1
    const response = responseV1
    return {
      request: request({ transactionalId, groupId, producerId, producerEpoch, topics }),
      response,
    }
  },
}

export default {
  versions: Object.keys(versions),
  protocol: ({
    version
  }: any) => versions[version],
}
