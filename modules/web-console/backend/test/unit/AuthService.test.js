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


import {assert} from 'chai';
import injector from '../injector';
import testAccounts from '../data/accounts.json';

let authService;
let mongo;
let errors;

suite('AuthServiceTestsSuite', () => {
    const prepareUserSpaces = () => {
        return mongo.Account.create(testAccounts)
            .then((accounts) => {
                return Promise.all(
                    accounts.map((account) => mongo.Space.create(
                        [
                            {name: 'Personal space', owner: account._id, demo: false},
                            {name: 'Demo space', owner: account._id, demo: true}
                        ]
                    )))
                    .then((spaces) => [accounts, spaces]);
            });
    };

    suiteSetup(() => {
        return Promise.all([injector('services/auth'),
            injector('mongo'),
            injector('errors')])
            .then(([_authService, _mongo, _errors]) => {
                mongo = _mongo;
                authService = _authService;
                errors = _errors;
            });
    });

    setup(() => {
        return Promise.all([
            mongo.Account.remove().exec(),
            mongo.Space.remove().exec()
        ]);
    });

    test('Check token generator', () => {
        const tokenLength = 16;
        const token1 = authService.generateResetToken(tokenLength);
        const token2 = authService.generateResetToken(tokenLength);

        assert.equal(token1.length, tokenLength);
        assert.equal(token2.length, tokenLength);
        assert.notEqual(token1, token2);
    });


    test('Reset password token for non existing user', (done) => {
        authService.resetPasswordToken('non-exisitng@email.ee')
            .catch((err) => {
                assert.instanceOf(err, errors.MissingResourceException);
                done();
            });
    });

    test('Reset password token for existing user', (done) => {
        prepareUserSpaces()
            .then(() => {
                return authService.resetPasswordToken(testAccounts[0].email)
                    .then((account) => {
                        assert.notEqual(account.resetPasswordToken.length, 0);
                        assert.notEqual(account.resetPasswordToken, testAccounts[0].resetPasswordToken);
                    });
            })
            .then(done)
            .catch(done);
    });

    test('Reset password by token for non existing user', (done) => {
        authService.resetPasswordByToken('0')
            .catch((err) => {
                assert.instanceOf(err, errors.MissingResourceException);
                done();
            });
    });

    test('Reset password by token for existing user', (done) => {
        prepareUserSpaces()
            .then(() => {
                return authService.resetPasswordByToken(testAccounts[0].resetPasswordToken, 'NewUniquePassword$1')
                    .then((account) => {
                        assert.isUndefined(account.resetPasswordToken);
                        assert.notEqual(account.hash, 0);
                        assert.notEqual(account.hash, testAccounts[0].hash);
                    });
            })
            .then(done)
            .catch(done);
    });

    test('Validate user for non existing reset token', (done) => {
        authService.validateResetToken('Non existing token')
            .catch((err) => {
                assert.instanceOf(err, errors.IllegalAccessError);
                done();
            });
    });

    test('Validate reset token', (done) => {
        prepareUserSpaces()
            .then(() => {
                return authService.validateResetToken(testAccounts[0].resetPasswordToken)
                    .then(({token, email}) => {
                        assert.equal(email, testAccounts[0].email);
                        assert.equal(token, testAccounts[0].resetPasswordToken);
                    });
            })
            .then(done)
            .catch(done);
    });
});
