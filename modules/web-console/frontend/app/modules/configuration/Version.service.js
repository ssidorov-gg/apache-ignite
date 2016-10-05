/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Utility service for version parsing and comparing
 */
const VERSION_MATCHER = /(\d+)\.(\d+)\.(\d+)([-.]([^0123456789][^-]+)(-SNAPSHOT)?)?(-(\d+))?(-([\da-f]+))?/i;


const numberCompare = (a, b) => a > b ? 1 : a < b ? -1 : 0;

export default class Version {

    parse(verStr) {
        // Development or built from source ZIP.
        verStr = verStr.replace(/(-DEV|-n\/a)$/i, '');

        let [, major, minor, maintenance, stage, ...chunks] = verStr.match(VERSION_MATCHER);
        let revTs = 0;
        let revHash;

        major = parseInt(major, 10);
        minor = parseInt(minor, 10);
        maintenance = parseInt(maintenance, 10);
        stage = (stage || '').substring(1);

        if (chunks[2])
            revTs = parseInt(chunks[3], 10);

        if (chunks[4])
            revHash = chunks[5];

        return {
            major,
            minor,
            maintenance,
            stage,
            revTs,
            revHash
        };
    }
    /**
     * @param a {String} first compared version
     * @param b {String} second compared version
     * @returns {Integer} 1 if a > b, 0 if versions equals, -1 if a < b
     */
    compare(a, b) {
        const pa = this.parse(a);
        const pb = this.parse(b);

        let res = numberCompare(pa.major, pb.major);

        if (res !== 0)
            return res;

        res = numberCompare(pa.minor, pb.minor);

        if (res !== 0)
            return res;

        res = numberCompare(pa.maintenance, pb.maintenance);

        if (res !== 0)
            return res;

        return numberCompare(pa.revTs, pb.maintenance);
    }

    /**
     * Return current product version
     * @returns {{ignite: string}}
     */
    productVersion() {
        return {
            ignite: '1.7.0'
        };
    }

    since(nodeVer, sinceVer) {
        return this.compare(nodeVer, sinceVer) >= 0;
    }
}
