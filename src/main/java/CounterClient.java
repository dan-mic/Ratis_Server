/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyFactory;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.SupportedRpcType;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Counter client application, this application sends specific number of
 * INCREMENT command to the Counter cluster and at the end sends a GET command
 * and print the result
 * Value for INCREMENT command is 10
 */
public final class CounterClient {

    private static RaftGroup RAFT_GROUP;

    private CounterClient(){
    }

    //@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public static void main(String[] args)
            throws IOException, InterruptedException {

        if (args.length < 3) {
            System.err.println("Wrong arguments. 3 addresses needed.");
            System.err.println("Optional: true as 4th argument to enable HadroNIO");
            System.exit(1);
        }

        //initialize RAFT_GROUP
        final List<RaftPeer> peers = new ArrayList<>(args.length);
        for (int i = 0; i < args.length; i++) {
            peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress(args[i]).build());
        }
        List<RaftPeer> PEERS = Collections.unmodifiableList(peers);

        final UUID CLUSTER_GROUP_ID = UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");

        RAFT_GROUP = RaftGroup.valueOf(
                RaftGroupId.valueOf(CLUSTER_GROUP_ID), PEERS);

        //enable HadroNIO
        if (args.length >= 4 && args[3].equals("true"))
            System.setProperty("java.nio.channels.spi.SelectorProvider", "de.hhu.bsinfo.hadronio.HadronioProvider");

        //indicate the number of INCREMENT command, set 10 if no parameter passed
        int increment = 10;

        //build the counter cluster client
        RaftClient raftClient = buildClient();

        //use a executor service with 10 thread to send INCREMENT commands
        // concurrently
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        //send INCREMENT commands concurrently
        System.out.printf("Sending %d increment command...%n", increment);
        for (int i = 0; i < increment; i++) {
            executorService.submit(() ->
                    raftClient.io().send(Message.valueOf("INCREMENT")));
        }

        //shutdown the executor service and wait until they finish their work
        executorService.shutdown();
        executorService.awaitTermination(increment * 500L, TimeUnit.MILLISECONDS);

        //send GET command and print the response
        RaftClientReply count = raftClient.io().sendReadOnly(Message.valueOf("GET"));
        String response = count.getMessage().getContent().toString(Charset.defaultCharset());
        System.out.println(response);
    }

    /**
     * build the RaftClient instance which is used to communicate to
     * Counter cluster
     *
     * @return the created client of Counter cluster
     */
    private static RaftClient buildClient() {
        RaftProperties raftProperties = new RaftProperties();

        //set rpc type to netty
        RaftConfigKeys.Rpc.setType(raftProperties, SupportedRpcType.NETTY);

        RaftClient.Builder builder = RaftClient.newBuilder()
                .setProperties(raftProperties)
                .setRaftGroup(CounterClient.RAFT_GROUP)
                .setClientRpc(
                        new NettyFactory(new Parameters())
                                .newRaftClientRpc(ClientId.randomId(), raftProperties));
        return builder.build();
    }
}