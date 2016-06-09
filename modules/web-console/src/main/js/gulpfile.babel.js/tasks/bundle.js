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

import gulp from 'gulp';
import util from 'gulp-util';
import webpack from 'webpack';
import webpackConfig from '../../webpack.config';
import WebpackDevServer from 'webpack-dev-server';

import {srcDir, destDir, igniteModulesTemp} from '../paths';


// TODO source map
// const options = {
//     minify: true
// };
//
// if (util.env.debug)
//     delete options.minify;
//
// if (util.env.debug || util.env.sourcemaps)
//     options.sourceMaps = true;

gulp.task('bundle', ['bundle:ignite']);

// Package all external dependencies and ignite-console.
gulp.task('bundle:ignite', (cb) => {
    const compiler = webpack(webpackConfig, cb);

    if(process.env.NODE_ENV==='development' && webpackConfig.devServer)
        new WebpackDevServer(compiler, webpackConfig.devServer)
            .listen(webpackConfig.devServer.port || 9000, 'localhost');
});
