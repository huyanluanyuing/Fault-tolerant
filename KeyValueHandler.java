import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock; // (1) 仅用于控制平面的锁

import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.layered.*;

import org.apache.zookeeper.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.CuratorWatcher;

import org.apache.log4j.*; // (如您所愿，注释掉日志以提高速度)

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher {

    static Logger log = Logger.getLogger(KeyValueHandler.class.getName());

    // (2) 性能基石: 线程安全的 ConcurrentHashMap
    private Map<String, String> myMap;

    // (3) 优化: 此锁 *仅* 用于控制平面 (ZK事件, 状态转移, 备节点写入)
    // Get 和 Put (主节点) 的热路径是 *无锁* 的
    private final Lock controlLock = new ReentrantLock();

    // --- ZK & Server Info ---
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private String myZnodePath;

    // --- Role & State Variables ---
    private volatile boolean isPrimary = false; // (4) Volatile 保证可见性
    private volatile boolean dataInitialized = false;

    // --- (5) Volatile 保证连接对 Put 线程可见 ---
    private volatile String backupAddress = null;
    private volatile KeyValueService.Client backupClient = null;
    private TTransport backupTransport = null;
    private Socket backupSocket = null; // (6) 使用高级 Socket 调优


    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        // (7) 优化: 设置初始容量和并发级别 (基于您的代码)
        this.myMap = new ConcurrentHashMap<>(1 << 15, 0.75f,
                Math.max(2, Runtime.getRuntime().availableProcessors()));
    }

    public void setMyZnodePath(String path) {
        this.myZnodePath = path;
    }

    public void initialize() {
        log.info("Handler initialized. My znode is: " + myZnodePath);
        determineRole();
    }

    /**
     * 核心逻辑：确定角色 (Primary/Backup) 并设置 ZK Watch.
     * 这是 "控制平面", 所以我们使用 'controlLock'.
     */
    private void determineRole() {
        controlLock.lock(); // (8) 锁定控制平面
        try {
            List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
            Collections.sort(children);

            // 检查我们自己的 znode 是否仍然存在
            String myChild = null;
            if (myZnodePath != null && myZnodePath.startsWith(zkNode + "/")) {
                myChild = myZnodePath.substring(zkNode.length() + 1);
            }

            if (children.isEmpty() || myChild == null || !children.contains(myChild)) {
                // log.warn("My znode is not in children list or list is empty.");
                isPrimary = false;
                closeBackupConnection();
                return;
            }

            String primaryZnodeName = children.get(0);
            String primaryFullPath = zkNode + "/" + primaryZnodeName;

            if (myZnodePath.equals(primaryFullPath)) {
                // --- 我是 Primary ---
                isPrimary = true;

                String newBackupAddress = null;
                if (children.size() > 1) {
                    String backupZnodeName = children.get(1);
                    String backupFullPath = zkNode + "/" + backupZnodeName;
                    try {
                        byte[] data = curClient.getData().forPath(backupFullPath);
                        newBackupAddress = new String(data);
                    } catch (Exception e) {
                        newBackupAddress = null;
                    }
                }

                // (9) 管理持久连接
                if (!Objects.equals(newBackupAddress, this.backupAddress)) {
                    // log.info("Backup changed. Old: " + this.backupAddress + ", New: " + newBackupAddress);
                    closeBackupConnection();
                    if (newBackupAddress != null) {
                        createBackupConnection(newBackupAddress);
                    }
                }

            } else {
                // --- 我是 Backup ---
                isPrimary = false;
                closeBackupConnection(); // Backup 节点不需要出站连接

                // (10) 状态转移
                if (!dataInitialized) {
                    try {
                        byte[] data = curClient.getData().forPath(primaryFullPath);
                        log.info("Begin state transfer from primary at " );
                        String primaryAddress = new String(data);
                        log.info("Finishing Performing state transfer from primary at " + primaryAddress);

                        // performStateTransfer 会在内部被 'determineRole' 的锁保护
                        performStateTransfer(primaryAddress);
                        dataInitialized = true;
                    } catch (Exception e) {
                        log.error("Failed to perform initial state transfer", e);
                        // 保持 uninitialized, 下次 ZK 事件时重试
                    }
                }
            }
        } catch (Exception e) {
            // log.error("Failed to determine role", e);
            isPrimary = false;
            closeBackupConnection();
        } finally {
            controlLock.unlock(); // (8) 释放控制平面锁
        }
    }

    /**
     * ZK Watcher 的回调方法 (Step 6 逻辑)
     */
    @Override
    public void process(WatchedEvent event) throws Exception {
        // log.warn("ZooKeeper watch triggered: " + event.getType());
        // ZK 事件是串行的; 它会调用 determineRole,
        // determineRole 会获取 'controlLock'，从而安全地更新状态。
        determineRole();
    }

    // ========== KeyValueService.Iface (Thrift RPC) ==========

    /**
     * 读操作 (热路径)
     * (11) 完全无锁
     */
    @Override
    public String get(String key) throws org.apache.thrift.TException {
        if (!isPrimary) { // 检查 volatile 变量
            // log.warn("Received GET request, but I am not primary. Throwing exception.");
            throw new TException("Not the primary. Cannot serve get requests.");
        }
        try {
            // controlLock.lock();
            return myMap.getOrDefault(key, "");
        }finally {
            // controlLock.unlock();
        }
        // (12) CHM 的 getOrDefault 是线程安全的，无需加锁

    }

    /**
     * 写操作 (热路径)
     * (13) 主路径无锁，但会阻塞等待同步复制
     */
    @Override
    public void put(String key, String value) throws org.apache.thrift.TException {
        controlLock.lock();
        if (!isPrimary) { // 检查 volatile 变量
            // log.warn("Received PUT request, but I am not primary. Throwing exception.");
            throw new TException("Not the primary. Cannot serve put requests.");
        }


        //controlLock.lock();

        //controlLock.unlock();
        // (14) 读取 volatile 变量以获取当前客户端
        KeyValueService.Client c = this.backupClient;

        if (c != null) {
            try {
                // (15) 正确性: 先进行同步复制
                c.backupPut(key, value);

            } catch (TException e) {
                log.warn("Replication to backup failed.", e);
                closeBackupConnection();
            }
        }

        // (17) 复制成功 (或没有备节点, 或备节点失败) 后, 在本地写入
        // CHM 的 put 是线程安全的，无需加锁
        myMap.put(key, value);
        controlLock.unlock();
        // (18) 此时向 A3Client 返回 "OK".
        // 这满足了“同步复制”：数据要么在备节点上，要么备节点已被标记为死亡。
    }

    /**
     * 备节点写入 (控制平面)
     * (19) 必须加锁，以防止与 'performStateTransfer' 冲突
     */
    @Override
    public void backupPut(String key, String value) throws TException {
        controlLock.lock(); // (20) 锁定
        try {
            // log.info("Backup: Replicating PUT " + key + "=" + value);
            myMap.put(key, value);
        } finally {
            controlLock.unlock(); // (20) 解锁
        }
    }

    /**
     * 主节点提供快照 (热路径，但不频繁)
     * (21) 完全无锁
     */
    @Override
    public Map<String, String> getFullDataSet() throws TException {
        if (!isPrimary) {
            // log.warn("Received getFullDataSet request, but I am not primary.");
            throw new TException("Not the primary. Cannot serve full data set.");
        }

        // (22) 正确性: 必须返回一个 *副本* (Snapshot).
        // CHM 的构造函数是线程安全的，无需加锁。
        return new ConcurrentHashMap<>(myMap);

    }

    // ========== 辅助方法 ==========

    /**
     * 备节点拉取数据 (控制平面)
     * (23) 此方法由 'determineRole' 调用，因此已持有 'controlLock'
     */
    private void performStateTransfer(String primaryAddress) throws TException {
        KeyValueService.Client primaryClient = null;
        TTransport primaryTransport = null;
        Socket socket = null;
        try {
            String[] parts = primaryAddress.split(":");
            String h = parts[0];
            int p = Integer.parseInt(parts[1]);

            // (24) 建立一个 *一次性* 连接 (使用您的高级 Socket 调优)
            socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            socket.connect(new InetSocketAddress(h, p), 1000); // 1s connect timeout
            socket.setSoTimeout(5000); // 5s read timeout (状态转移可能需要时间)
            int MAX_FRAME_SIZE = 50 * 1024 * 1024;
            primaryTransport = new TFramedTransport(new TSocket(socket), MAX_FRAME_SIZE);
            TProtocol protocol = new TBinaryProtocol(primaryTransport);
            primaryClient = new KeyValueService.Client(protocol);

            // 调用 getFullDataSet (无锁)
            Map<String, String> data = primaryClient.getFullDataSet();

            // (25) 正确性: 必须复制到 *现有的* CHM 中
            // 我们已持有 'controlLock'，因此此操作是原子的 (不会与 backupPut 冲突)
            log.info("State transfer: Received " + data.size() + " items from primary.");
            myMap.clear();
            myMap.putAll(data);
            // log.info("State transfer complete.");

        } catch (IOException e) {
            throw new TException(e); // 转换为 TException
        } finally {
            if (primaryTransport != null) {
                primaryTransport.close();
            }
            if (socket != null) {
                try { socket.close(); } catch (IOException ignore) {}
            }
        }
    }

    /**
     * (控制平面) 创建持久连接
     * (26) 此方法由 'determineRole' 调用，因此已持有 'controlLock'
     */
    private void createBackupConnection(String hostPort) {
        try {
            String[] parts = hostPort.split(":");
            String h = parts[0];
            int p = Integer.parseInt(parts[1]);

            // (27) 使用您的高级 Socket 调优
            this.backupSocket = new Socket();
            this.backupSocket.setTcpNoDelay(true); // 禁用 Nagle 算法 (!! 性能关键)
            this.backupSocket.setKeepAlive(true); // 保持 TCP 连接

            // (28) 缓冲区大小 (可能需要调优)
            // this.backupSocket.setSendBufferSize(1 << 16);
            // this.backupSocket.setReceiveBufferSize(1 << 16);

            // (29) 设置短连接和读超时，以便快速失败
            this.backupSocket.connect(new InetSocketAddress(h, p), 300); // 300ms 连接超时
            this.backupSocket.setSoTimeout(300); // 300ms 读超时

            this.backupTransport = new TFramedTransport(new TSocket(this.backupSocket));
            TProtocol protocol = new TBinaryProtocol(this.backupTransport);
            this.backupClient = new KeyValueService.Client(protocol);
            this.backupAddress = hostPort; // 标记连接为活动
            // log.info("Created persistent connection to backup at " + hostPort);
        } catch (Exception e) {
            // log.warn("Failed to create persistent connection to backup: " + e.getMessage());
            closeBackupConnection(); // 确保清理
        }
    }

    /**
     * (控制平面) 关闭持久连接
     * (30) 此方法由 'determineRole' 或 'put' (在 catch 块中) 调用
     */
    private void closeBackupConnection() {
        // (31) 检查 backupClient 而不是 transport，以避免在 'put' 中重复关闭
        if (this.backupClient == null) {
            return;
        }

        // log.info("Closing persistent connection to backup at " + this.backupAddress);

        if (this.backupTransport != null) {
            this.backupTransport.close();
        }
        if (this.backupSocket != null) {
            try { this.backupSocket.close(); } catch (IOException ignore) {}
        }
        this.backupClient = null;
        this.backupTransport = null;
        this.backupSocket = null;
        this.backupAddress = null;
    }
}