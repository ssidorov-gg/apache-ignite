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

package org.apache.ignite.examples.computegrid;

import org.apache.ignite.*;
import org.apache.ignite.examples.*;
import org.apache.ignite.lang.*;

import java.util.*;

/**
 * Demonstrates a simple use of Ignite with reduce closure.
 * <p>
 * This example splits a phrase into collection of words, computes their length on different
 * nodes and then computes total amount of non-whitespaces characters in the phrase.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public class ComputeClosureExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-compute.xml")) {
            System.out.println();
            System.out.println(">>> Compute closure example started.");

            // Execute closure on all cluster nodes.
            Collection<Integer> res = ignite.compute().apply(
                new IgniteClosure<String, Integer>() {
                    @Override public Integer apply(String word) {
                        System.out.println();
                        System.out.println(">>> Printing '" + word + "' on this node from ignite job.");

                        // Return number of letters in the word.
                        return word.length();
                    }
                },

                // Job parameters. Ignite will create as many jobs as there are parameters.
                Arrays.asList("Count characters using closure".split(" "))
            );

            int sum = 0;

            // Add up individual word lengths received from remote nodes
            for (int len : res)
                sum += len;

            System.out.println();
            System.out.println(">>> Total number of characters in the phrase is '" + sum + "'.");
            System.out.println(">>> Check all nodes for output (this node is also part of the cluster).");
        }
    }
}
