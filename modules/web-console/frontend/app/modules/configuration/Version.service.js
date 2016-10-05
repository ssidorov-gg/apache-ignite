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
 * Utility service for version compare
 */
const VERSION_MATCHER = /(\d+)\.(\d+)\.(\d+)([-.]([^0123456789][^-]+)(-SNAPSHOT)?)?(-(\d+))?(-([\da-f]+))?/i;

export default class Version {

    parse(verStr) {
        // Development or built from source ZIP.
        verStr = verStr.replace(/-DEV|-n\/a$/, '');

        const [major, minor, maintenance, stage, ...chunks] = verStr.match(VERSION_MATCHER);
        console.log(chunks);

        // if (match.matches()) {
        //     try {
        //
        //         String stage = "";
        //
        //         if (match.group(4) != null)
        //             stage = match.group(4).substring(1);
        //
        //         long revTs = 0;
        //
        //         if (match.group(7) != null)
        //             revTs = Long.parseLong(match.group(8));
        //
        //         byte[] revHash = null;
        //
        //         if (match.group(9) != null)
        //             revHash = U.decodeHex(match.group(10).toCharArray());
        //
        //         return new IgniteProductVersion(major, minor, maintenance, stage, revTs, revHash);
        //     }
        //     catch (IllegalStateException | IndexOutOfBoundsException | NumberFormatException | IgniteCheckedException e) {
        //         throw new IllegalStateException("Failed to parse version: " + verStr, e);
        //     }
        // }
        // else
        //     throw new IllegalStateException("Failed to parse version: " + verStr);
    }
    /**
     * @param a {String} first compared version
     * @param b {String} second compared version
     * @returns {Integer} 1 if a > b, 0 if versions equals, -1 if a < b
     */
    compare(a, b) {
        let pa = a.split('.');
        let pb = b.split('.');

        for (let i = 0; i < 3; i++) {
            let na = Number(pa[i]);
            let nb = Number(pb[i]);
            if (na > nb) return 1;
            if (nb > na) return -1;
            if (!isNaN(na) && isNaN(nb)) return 1;
            if (isNaN(na) && !isNaN(nb)) return -1;
        }

        return 0;
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
