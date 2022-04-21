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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.netty.server.NettyRpcService;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Simplest Ratis server, use a simple state machine {@link CounterStateMachine}
 * which maintain a counter across multi server.
 * This server application designed to run several times with different
 * parameters (1,2 or 3).
 * <p>
 * Run this application three times with three different parameter set-up a
 * ratis cluster which maintain a counter value replicated in each server memory
 */
public final class CounterServer implements Closeable {
    private final RaftServer server;
    private static RaftGroup RAFT_GROUP;


    public CounterServer(RaftPeer peer, File storageDir) throws IOException {
        //create a property object
        RaftProperties properties = new RaftProperties();

        //set rpc type to netty
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);

        //set the storage directory (different for each peer) in RaftProperty object
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

        //set the port which server listen to in RaftProperty object
        final int port = NetUtils.createSocketAddr(peer.getAddress()).getPort();
        NettyConfigKeys.Server.setPort(properties, port);

        //create the counter state machine which hold the counter value
        CounterStateMachine counterStateMachine = new CounterStateMachine();

        //create and start the Raft server
        this.server = RaftServer.newBuilder()
                .setGroup(CounterServer.RAFT_GROUP)
                .setProperties(properties)
                .setServerId(peer.getId())
                .setStateMachine(counterStateMachine)
                .build();
    }

    public void start() throws IOException {
        server.start();
    }

    @Override
    public void close() throws IOException {
        server.close();
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 4) {
            System.err.println("Wrong arguments. First serverIndex (1, 2 or 3), followed by 3 addresses");
            System.err.println("Optional: true as 5th argument to enable HadroNIO.");
            System.exit(1);
        }

        //enable HadroNIO
        if (args.length >= 5 && args[4].equals("true"))
            System.setProperty("java.nio.channels.spi.SelectorProvider", "de.hhu.bsinfo.hadronio.HadronioProvider");

        //initialize RAFT_GROUP
        final List<RaftPeer> peers = new ArrayList<>(args.length - 1);
        for (int i = 1; i < args.length; i++) {
            peers.add(RaftPeer.newBuilder().setId("n" + (i - 1)).setAddress(args[i]).build());
        }
        List<RaftPeer> PEERS = Collections.unmodifiableList(peers);

        final UUID CLUSTER_GROUP_ID = UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");

        RAFT_GROUP = RaftGroup.valueOf(
                RaftGroupId.valueOf(CLUSTER_GROUP_ID), PEERS);

        //find current peer object based on application parameter
        final RaftPeer currentPeer = PEERS.get(Integer.parseInt(args[0]) - 1);

        //start a counter server
        final File storageDir = new File("./" + currentPeer.getId());
        final CounterServer counterServer = new CounterServer(currentPeer, storageDir);
        System.out.println("Start server...");
        counterServer.start();
        System.out.println("Server started");

        //exit when any input entered
        Scanner scanner = new Scanner(System.in, UTF_8.name());
        scanner.nextLine();
        System.out.println("Close server...");
        counterServer.close();
        System.out.println("Server closed");
        System.exit(0);
    }
}