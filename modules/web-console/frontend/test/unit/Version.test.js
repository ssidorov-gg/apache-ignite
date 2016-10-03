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

import VersionService from '../../app/services/Version.service.js';

const INSTANCE = new VersionService();

import { assert } from 'chai';

suite('VersionServiceTestsSuite', () => {
    test('Check patch version', () => {
        assert.equal(INSTANCE.compare('1.7.2', '1.7.1'), 1);
    });

    test('Check minor version', () => {
        assert.equal(INSTANCE.compare('1.8.1', '1.7.1'), 1);
    });

    test('Check major version', () => {
        assert.equal(INSTANCE.compare('2.7.1', '1.7.1'), 1);
    });

    test('Version a > b', () => {
        assert.equal(INSTANCE.compare('1.7.0', '1.5.0'), 1);
    });

    test('Version a = b', () => {
        assert.equal(INSTANCE.compare('1.7.0', '1.7.0'), 0);
    });

    test('Version a < b', () => {
        assert.equal(INSTANCE.compare('1.5.1', '1.5.2'), -1);
    });
});
