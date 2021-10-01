/** @format */

const logResponseError = false;

const versions = {
  0: () => {
    // @ts-expect-error ts-migrate(2580) FIXME: Cannot find name 'require'. Do you need to install... Remove this comment to see the full error message
    //const request = require('./v0/request')
    import * as request from './v0/request';
    // @ts-expect-error ts-migrate(2580) FIXME: Cannot find name 'require'. Do you need to install... Remove this comment to see the full error message
    //const response = require('./v0/response')
    import * as response from './v0/response';
    return { request: request(), response, logResponseError: true };
  },
  1: () => {
    // @ts-expect-error ts-migrate(2580) FIXME: Cannot find name 'require'. Do you need to install... Remove this comment to see the full error message
    // const request = require('./v1/request')
    import * as request from './v1/request';
    // @ts-expect-error ts-migrate(2580) FIXME: Cannot find name 'require'. Do you need to install... Remove this comment to see the full error message
    //const response = require('./v1/response')
    import * as response from './v1/response';
    return { request: request(), response, logResponseError };
  },
  2: () => {
    // @ts-expect-error ts-migrate(2580) FIXME: Cannot find name 'require'. Do you need to install... Remove this comment to see the full error message
    //const request = require('./v2/request')
    import * as request from './v2/request';
    // @ts-expect-error ts-migrate(2580) FIXME: Cannot find name 'require'. Do you need to install... Remove this comment to see the full error message
    //const response = require('./v2/response')
    import * as response from './v2/response';
    return { request: request(), response, logResponseError };
  },
};

export default {
  versions: Object.keys(versions),
  protocol: ({
    version,
  }: // @ts-expect-error ts-migrate(7053) FIXME: Element implicitly has an 'any' type because expre... Remove this comment to see the full error message
  any) => versions[version],
};
