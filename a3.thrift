service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  // 1. 用于主节点复制数据到备节点
  void backupPut(1:string key, 2:string value)
  // 2. 用于备节点从主节点复制所有数据 (Scenario #1)
  map<string, string> getFullDataSet()
}
