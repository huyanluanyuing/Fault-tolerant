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
    private volatile boolean isPrimary = false; // if the node is primary
    private volatile boolean dataInitialized = false; // 用于 Scenario #1 状态转移
    private volatile String backupAddress = null; // "host:port" of backup


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
        log.info("Handler initialized. My znode is: " + myZnodePath);
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
                log.warn("My znode is not in children list or list is empty.");
                isPrimary = false;
                backupAddress = null;
                return;
            }

            // 检查我是否是 Primary (列表中的第一个)
            String primaryZnodeName = children.get(0);
            String primaryFullPath = zkNode + "/" + primaryZnodeName;
            boolean wasPrimary = isPrimary; // 跟踪角色变化

            if (myZnodePath.equals(primaryFullPath)) {
                // --- 我是 Primary ---
                isPrimary = true;
                if (!wasPrimary) {
                    log.info("====== Transitioned to PRIMARY role ======");
                }

                // 查找 Backup (列表中的第二个)
                if (children.size() > 1) {
                    String backupZnodeName = children.get(1);
                    String backupFullPath = zkNode + "/" + backupZnodeName;
                    try {
                        byte[] data = curClient.getData().forPath(backupFullPath);
                        backupAddress = new String(data);
                        log.info("Found backup at: " + backupAddress);
                    } catch (Exception e) {
                        log.warn("Failed to get backup address, assuming no backup.", e);
                        backupAddress = null;
                    }
                } else {
                    backupAddress = null; // 没有 backup
                }

            } else {
                // --- 我是 Backup ---
                isPrimary = false;
                backupAddress = null;
                if (wasPrimary) {
                    log.info("====== Transitioned to BACKUP role ======");
                }

                // Step 6 / Scenario #1: 如果我是 Backup 且未初始化数据, 执行状态转移
                if (!dataInitialized) {
                    try {
                        byte[] data = curClient.getData().forPath(primaryFullPath);
                        String primaryAddress = new String(data);
                        log.info("Performing state transfer from primary at " + primaryAddress);
                        performStateTransfer(primaryAddress);// get all data from primary
                        dataInitialized = true; // 标记为已初始化
                    } catch (Exception e) {
                        log.error("Failed to perform initial state transfer", e);
                        // 将保持未初始化状态，下次 watch 触发时重试
                    }
                }
            }

        } catch (Exception e) {
            log.error("Failed to determine role", e);
            isPrimary = false; // 出错时, 假定为 backup
            backupAddress = null;
        } finally {
            dataLock.unlock();
        }
    }

    /**
     * ZK Watcher 的回调方法 (Step 6 逻辑)
     */
    @Override
    public void process(WatchedEvent event) throws Exception {
        log.warn("ZooKeeper watch triggered: " + event.getType());

        // Znode 列表发生变化 (服务器崩溃或加入)
        // 重新运行角色确定逻辑
        determineRole();
    }

    // ========== KeyValueService.Iface (Thrift RPC) ==========

    @Override
    public String get(String key) throws org.apache.thrift.TException {
        // Step 5: 检查角色 (Scenario #3)
        if (!isPrimary) {
            log.warn("Received GET request, but I am not primary. Throwing exception.");
            throw new TException("Not the primary. Cannot serve get requests.");
        }

        // Step 5: Concurrency Control
        dataLock.lock();
        try {
            log.info("Primary: GET " + key);
            String ret = myMap.get(key);
            if (ret == null)
                return "";
            else
                return ret;
        } finally {
            dataLock.unlock();
        }
    }

    @Override
    public void put(String key, String value) throws org.apache.thrift.TException {
        // Step 5: 检查角色 (Scenario #3)
        if (!isPrimary) {
            log.warn("Received PUT request, but I am not primary. Throwing exception.");
            throw new TException("Not the primary. Cannot serve put requests.");
        }

        // Step 5: Concurrency Control
        dataLock.lock();
        try {
            log.info("Primary: PUT " + key + "=" + value);

            // Step 5: 同步复制到 Backup
            if (backupAddress != null) {
                log.debug("Replicating PUT to backup: " + backupAddress);
                try {
                    // 每一次新建一个连接并关闭连接可能会降低吞吐量?!
                    // (此辅助方法在下面定义)
                    KeyValueService.Client backupClient = createThriftClient(backupAddress);

                    // 调用在 a3.thrift 中新定义的 'backupPut'
                    backupClient.backupPut(key, value);

                    log.debug("Replication successful.");

                    closeThriftClient(backupClient);
                } catch (TException e) {
                    log.warn("Replication to backup failed. Assuming backup is down.", e);
                    // 复制失败. 暂时假定 backup 已崩溃.
                    // 下次 ZK watch 触发时 'determineRole' 会更新 backupAddress.
                    backupAddress = null;
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
        // 这个方法 不检查 isPrimary
        dataLock.lock();
        try {
            log.info("Backup: Replicating PUT " + key + "=" + value);
            myMap.put(key, value);
        } finally {
            dataLock.unlock();
        }
    }

    /**
     * 新的 Thrift RPC (在 a3.thrift 中定义)
     * 由 Backup 调用, 在 Primary 上执行 (Scenario #1)
     */
    @Override
    public Map<String, String> getFullDataSet() throws TException {
        if (!isPrimary) {
            log.warn("Received getFullDataSet request, but I am not primary.");
            throw new TException("Not the primary. Cannot serve full data set.");
        }

        dataLock.lock();
        try {
            log.info("Primary: Serving full data set to new backup.");
            // 返回一个 *副本* 以确保线程安全
            // 返回新的map是否会增加开销？！
            return new HashMap<>(myMap);
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
        try {
            primaryClient = createThriftClient(primaryAddress);

            // 调用在 a3.thrift 中新定义的 'getFullDataSet'
            Map<String, String> data = primaryClient.getFullDataSet();

            // 已持有 dataLock (在 determineRole 中)
            //myMap.clear();myMap.putAll(data);是否会增加开销？
            log.info("State transfer: Received " + data.size() + " items from primary.");
            myMap.clear();
            myMap.putAll(data);
            log.info("State transfer complete.");

        } finally {
            if (primaryClient != null) {
                closeThriftClient(primaryClient);
            }
        }
    }

    /**
     * 创建一个到另一台服务器的 Thrift 客户端连接
     */
    private KeyValueService.Client createThriftClient(String hostPort) throws TException {
        String[] parts = hostPort.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        TSocket sock = new TSocket(host, port);
        TTransport transport = new TFramedTransport(sock);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new KeyValueService.Client(protocol);
    }

    /**
     * 关闭 Thrift 客户端连接
     */
    private void closeThriftClient(KeyValueService.Client client) {
        try {
            client.getInputProtocol().getTransport().close();
        } catch (Exception e) {
            // ignore
        }
        try {
            client.getOutputProtocol().getTransport().close();
        } catch (Exception e) {
            // ignore
        }
    }
}