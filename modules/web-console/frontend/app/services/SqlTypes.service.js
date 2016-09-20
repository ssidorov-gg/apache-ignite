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

import _ from 'lodash';

// List of H2 reserved SQL keywords.
import H2_SQL_KEYWORDS from '../data/sql-keywords.json';

// Regular expression to check H2 SQL identifier.
const VALID_IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_$]*$/im;

/**
 * Utility service for various check on SQL types.
 */
export default class SqlTypes {
    /**
     * @param value {String} Value to check.
     * @returns {boolean} 'true' if given text is valid Java class name.
     */
    validIdentifier(value) {
        return !!(value && VALID_IDENTIFIER.test(value));
    }

    /**
     * @param value {String} Value to check.
     * @returns {boolean} 'true' if given text is one of H2 reserved keywords.
     */
    isKeyword(value) {
        return !!(value && _.includes(H2_SQL_KEYWORDS, value.toUpperCase()));
    }
}
