import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*; // Added this import
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.layered.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;
import org.apache.zookeeper.data.Stat;

import org.apache.log4j.*;

public class StorageNode {
    static Logger log;

    public static void main(String [] args) throws Exception {
        BasicConfigurator.configure();
        log = Logger.getLogger(StorageNode.class.getName());

        if (args.length != 4) {
            System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
            System.exit(-1);
        }

        CuratorFramework curClient =
                CuratorFrameworkFactory.builder()
                        .connectString(args[2])
                        .retryPolicy(new RetryNTimes(10, 1000))
                        .connectionTimeoutMs(1000)
                        .sessionTimeoutMs(10000)
                        .build();

        curClient.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                curClient.close();
            }
        });

        // --- THIS IS THE FIX ---
        // 1. Create the handler and store it in a variable
        KeyValueHandler handler = new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]);
        // 2. Pass that variable to the processor
        KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(handler);
        // --- END FIX ---

        TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
        TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
        sargs.protocolFactory(new TBinaryProtocol.Factory());
        sargs.transportFactory(new TFramedTransport.Factory());
        sargs.processorFactory(new TProcessorFactory(processor));
        sargs.maxWorkerThreads(64);
        TServer server = new TThreadPoolServer(sargs);
        log.info("Launching server on port " + args[1] + "...");

        new Thread(new Runnable() {
            public void run() {
                server.serve();
            }
        }).start();

        // --- Step 4: Create an ephemeral, sequential node in ZooKeeper ---

        String hostPort = args[0] + ":" + args[1];
        String nodePathPrefix = args[3] + "/node-"; // Using a prefix

        try {
            String createdNodePath = curClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(nodePathPrefix, hostPort.getBytes());

            log.info("Successfully registered node at " + createdNodePath);

            // Now you can call the methods on the 'handler' variable
            handler.setMyZnodePath(createdNodePath);
            handler.initialize(); // Start the primary/backup logic

        } catch (Exception e) {
            log.error("Failed to create znode in ZooKeeper", e);
            System.exit(1);
        }
    }
}