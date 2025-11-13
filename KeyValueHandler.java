import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock; // 导入 Java 锁

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.layered.*; // 导入 TFrameTransport
import org.apache.thrift.protocol.*;
import org.apache.zookeeper.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.CuratorWatcher; // 导入 Watcher

import org.apache.log4j.*;

/**
 * 实现了 Step 5 & 6 逻辑的 KeyValueHandler.
 * * !!! 重要 !!!
 * 编译此文件前, 您必须:
 * 1. 打开 'a3.thrift'
 * 2. 在 'service KeyValueService' 中添加:
 * void backupPut(1:string key, 2:string value)
 * map<string, string> getFullDataSet()
 * 3. 运行 'thrift --gen java a3.thrift' 来重新生成 gen-java
 */
public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher {

    static Logger log = Logger.getLogger(KeyValueHandler.class.getName());

    // --- Member Variables ---
    private Map<String, String> myMap;
    private final ReentrantLock dataLock = new ReentrantLock(); // Step 5: Java Lock

    // --- ZK & Server Info ---
    private CuratorFramework curClient;
    private String zkNode; // The parent znode
    private String host;
    private int port;
    private String myZnodePath; // This server's full ZK path

    // --- Role & State Variables ---
    private volatile boolean isPrimary = false;
    private volatile boolean dataInitialized = false; // 用于 Scenario #1 状态转移

    // --- (FIX) 持久化的备节点连接 ---
    private volatile String backupAddress = null; // "host:port" of backup
    private KeyValueService.Client backupClient = null; // 持久化的 Thrift 客户端
    private TTransport backupTransport = null;       // 对应的 Transport



    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        myMap = new ConcurrentHashMap<String, String>();
    }

    /**
     * 由 StorageNode 调用，设置此服务器的 znode 路径
     */
    public void setMyZnodePath(String path) {
        this.myZnodePath = path;
    }

    /**
     * 由 StorageNode 在 znode 创建后调用
     */
    public void initialize() {
        // log.info("Handler initialized. My znode is: " + myZnodePath);
        // 第一次确定角色并设置 watch
        determineRole();
    }

    /**
     * 核心逻辑：确定角色 (Primary/Backup) 并设置 ZK Watch.
     * 也在 process() 中被调用.
     */
    private void determineRole() {
        dataLock.lock(); // 锁定以安全地更改角色
        try {
            // 关键: 每次都必须重新设置 watch!
            List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
            Collections.sort(children); // 按字典序排序

            if (children.isEmpty() || myZnodePath == null || !children.contains(myZnodePath.substring(zkNode.length() + 1))) {
                // log.warn("My znode is not in children list or list is empty.");
                isPrimary = false;
                closeBackupConnection(); // (FIX) 清理旧的备节点连接
                return;
            }

            // 检查我是否是 Primary (列表中的第一个)
            String primaryZnodeName = children.get(0);
            String primaryFullPath = zkNode + "/" + primaryZnodeName;
            boolean wasPrimary = isPrimary; // 跟踪角色变化

            if (myZnodePath.equals(primaryFullPath)) {
                // --- 我是 Primary ---
                isPrimary = true;
                // if (!wasPrimary) {
                //     // log.info("====== Transitioned to PRIMARY role ======");
                // }

                // (FIX) 查找并管理到 Backup 的持久连接
                String newBackupAddress = null;
                if (children.size() > 1) {
                    String backupZnodeName = children.get(1);
                    String backupFullPath = zkNode + "/" + backupZnodeName;
                    try {
                        byte[] data = curClient.getData().forPath(backupFullPath);
                        newBackupAddress = new String(data);
                    } catch (Exception e) {
                        // log.warn("Failed to get backup address, assuming no backup.", e);
                        newBackupAddress = null;
                    }
                }

                // 检查备节点是否已更改
                if (newBackupAddress == null || !newBackupAddress.equals(this.backupAddress)) {
                    // log.info("Backup changed (or removed). Old: " + this.backupAddress + ", New: " + newBackupAddress);
                    closeBackupConnection(); // 清理旧连接
                    if (newBackupAddress != null) {
                        // 创建并保存新连接
                        createBackupConnection(newBackupAddress);
                    }
                }

            } else {
                // --- 我是 Backup ---
                isPrimary = false;
                closeBackupConnection(); // (FIX) Backup 不需要备节点连接

                if (wasPrimary) {
                    // log.info("====== Transitioned to BACKUP role ======");
                }

                // Step 6 / Scenario #1: 如果我是 Backup 且未初始化数据, 执行状态转移
                if (!dataInitialized) {
                    try {
                        byte[] data = curClient.getData().forPath(primaryFullPath);
                        String primaryAddress = new String(data);
                        // log.info("Performing state transfer from primary at " + primaryAddress);
                        performStateTransfer(primaryAddress);
                        dataInitialized = true; // 标记为已初始化
                    } catch (Exception e) {
                        // log.error("Failed to perform initial state transfer", e);
                        // 将保持未初始化状态，下次 watch 触发时重试
                    }
                }
            }

        } catch (Exception e) {
            // log.error("Failed to determine role", e);
            isPrimary = false; // 出错时, 假定为 backup
            closeBackupConnection();
        } finally {
            dataLock.unlock();
        }
    }

    /**
     * ZK Watcher 的回调方法 (Step 6 逻辑)
     */
    @Override
    public void process(WatchedEvent event) throws Exception {
        // log.warn("ZooKeeper watch triggered: " + event.getType());

        // Znode 列表发生变化 (服务器崩溃或加入)
        // 重新运行角色确定逻辑
        determineRole();
    }

    // ========== KeyValueService.Iface (Thrift RPC) ==========

    @Override
    public String get(String key) throws org.apache.thrift.TException {
        // Step 5: 检查角色 (Scenario #3) 读操作不应该上锁
        if (!isPrimary) {
            // log.warn("Received GET request, but I am not primary. Throwing exception.");
            throw new TException("Not the primary. Cannot serve get requests.");
        }
        return myMap.getOrDefault(key, "");
    }

    @Override
    public void put(String key, String value) throws org.apache.thrift.TException {
        // Step 5: 检查角色 (Scenario #3)
        if (!isPrimary) {
            // log.warn("Received PUT request, but I am not primary. Throwing exception.");
            throw new TException("Not the primary. Cannot serve put requests.");
        }
        // Step 5: Concurrency Control
        dataLock.lock();
        try {
            // log.info("Primary: PUT " + key + "=" + value);

            // (FIX) Step 5: 使用持久连接同步复制到 Backup
            if (this.backupClient != null) {
                // log.debug("Replicating PUT to backup: " + this.backupAddress);
                try {
                    // (FIX) 重用持久化的客户端
                    this.backupClient.backupPut(key, value);
                    // log.debug("Replication successful.");

                } catch (TException e) {
                    // log.warn("Replication to backup failed. Assuming backup is down.", e);
                    // 复制失败. 假定 backup 已崩溃.
                    // 清理坏的连接. 下次 ZK watch 触发时 'determineRole' 会找到新备节点.
                    closeBackupConnection();
                }
            }
            // 先更新本地还是远程？！
            // 复制完成 (或失败) 后, 更新本地 map
            myMap.put(key, value);

        } finally {
            dataLock.unlock();
        }
    }

    /**
     * 新的 Thrift RPC (在 a3.thrift 中定义)
     * 由 Primary 调用, 在 Backup 上执行.
     */
    @Override
    public void backupPut(String key, String value) throws TException {
        // 这个方法 *不* 检查 isPrimary
        //dataLock.lock();
        //try {
        // log.info("Backup: Replicating PUT " + key + "=" + value);
        myMap.put(key, value);
        //} finally {
        //    dataLock.unlock();
        //}
    }

    /**
     * 新的 Thrift RPC (在 a3.thrift 中定义)
     * 由 Backup 调用, 在 Primary 上执行 (Scenario #1)
     */
    @Override
    public Map<String, String> getFullDataSet() throws TException {
        if (!isPrimary) {
            // log.warn("Received getFullDataSet request, but I am not primary.");
            // throw new TException("Not the primary. Cannot serve full data set.");
        }

        dataLock.lock();
        try {
            // log.info("Primary: Serving full data set to new backup.");
            // 返回一个 *副本* 以确保线程安全
            // 返回新的map是否会增加开销？！
            // return new HashMap<>(myMap);
            return myMap;
        } finally {
            dataLock.unlock();
        }
    }

    // ========== 辅助方法 ==========

    /**
     * (Scenario #1) Backup 从 Primary 拉取所有数据
     */
    private void performStateTransfer(String primaryAddress) throws TException {
        KeyValueService.Client primaryClient = null;
        TTransport primaryTransport = null;
        try {
            // (FIX) 使用局部变量，因为这个连接只在状态转移时使用一次
            String[] parts = primaryAddress.split(":");
            primaryTransport = new TFramedTransport(new TSocket(parts[0], Integer.parseInt(parts[1])));
            primaryTransport.open();
            TProtocol protocol = new TBinaryProtocol(primaryTransport);
            primaryClient = new KeyValueService.Client(protocol);

            // 调用在 a3.thrift 中新定义的 'getFullDataSet'
            myMap = primaryClient.getFullDataSet(); // FIX: 声明局部变量 'data'

            // 已持有 dataLock (在 determineRole 中)
            // log.info("State transfer: Received " + data.size() + " items from primary.");

            // FIX: 覆盖 myMap 的内容以完成状态转移
            //myMap.clear();
            //myMap.putAll(data);

            // log.info("State transfer complete.");

        } finally {
            if (primaryTransport != null) {
                primaryTransport.close();
            }
        }
    }

    /**
     * (FIX) 创建一个到备节点的持久 Thrift 客户端连接
     */
    private void createBackupConnection(String hostPort) {
        try {
            String[] parts = hostPort.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            this.backupTransport = new TFramedTransport(new TSocket(host, port));
            this.backupTransport.open();
            TProtocol protocol = new TBinaryProtocol(this.backupTransport);
            this.backupClient = new KeyValueService.Client(protocol);
            this.backupAddress = hostPort; // 存储当前连接的地址
            // log.info("Created persistent connection to backup at " + hostPort);
        } catch (TException e) {
            // log.warn("Failed to create persistent connection to backup: " + e.getMessage());
            this.backupClient = null;
            this.backupTransport = null;
            this.backupAddress = null;
        }
    }

    /**
     * (FIX) 关闭并清理到备节点的持久连接
     */
    private void closeBackupConnection() {
        if (this.backupTransport != null) {
            // log.info("Closing persistent connection to backup at " + this.backupAddress);
            this.backupTransport.close();
        }
        this.backupClient = null;
        this.backupTransport = null;
        this.backupAddress = null;
    }
}