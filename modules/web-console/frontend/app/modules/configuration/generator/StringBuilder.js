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

export default class StringBuilder {
    /**
     * @param deep
     * @param indent
     */
    constructor(deep = 0, indent = 4) {
        this.indent = indent;
        this.deep = deep;
        this.lines = [];
    }

    emptyLine() {
        this.lines.push('');

        return this;
    }

    append(...lines) {
        _.forEach(lines, (line) => this.lines.push(_.repeat(' ', this.indent * this.deep) + line));

        return this;
    }

    startBlock(...lines) {
        this.append(...lines);

        this.deep++;

        return this;
    }

    endBlock(line) {
        this.deep--;

        this.append(line);

        return this;
    }

    asString() {
        return this.lines.join('\n');
    }
}