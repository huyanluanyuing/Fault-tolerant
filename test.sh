#!/bin/bash

# --- 配置 ---
INTERVAL=${1:-5}
SERVER_SCRIPT="./runserver.sh"
PROCESS_PATTERN="java StorageNode"

echo "--- 🚀 自动化测试场景已启动 (v3 - 可中断) ---"

# --- 健壮的清理函数 (添加了 exit 0) ---
cleanup() {
    echo
    echo "--- 🧹 正在清理所有 '$PROCESS_PATTERN' 进程... ---"

    # 额外步骤：杀死所有我们启动的 sleep 进程
    # pgrep -P $$ 会查找这个脚本($$)的所有子进程
    SLEEP_PIDS=$(pgrep -P $$ sleep)
    if [ -n "$SLEEP_PIDS" ]; then
        kill -9 $SLEEP_PIDS 2>/dev/null
    fi

    # 杀死所有 Java 服务器进程
    pkill -9 -f "$PROCESS_PATTERN"

    if [ $? -eq 0 ]; then
        echo "清理完毕。"
    else
        echo "未找到正在运行的进程。"
    fi

    echo "Watchdog 退出。"
    # --- 改进 #2: 强制退出脚本 ---
    exit 0
}

# --- 启动时清理 ---
echo "[Watchdog] 正在执行“启动时清理”，确保环境干净..."
# 在 trap 之前运行 cleanup，否则 trap 会捕获 exit 信号
cleanup_output=$(cleanup)
echo "$cleanup_output"
# 确保 cleanup 真的退出了，如果是，我们就不用继续了
# (这是一个安全检查)
if [[ "$cleanup_output" == *"Watchdog 退出。"* ]]; then
    # cleanup 内部的 exit 被触发了
    # 这不应该发生，但作为安全检查
    echo "清理函数意外退出。"
fi
echo "-----------------------------------"


# "trap" 会捕获 Ctrl+C (SIGINT) 或终止 (SIGTERM)
# 现在 cleanup() 会处理退出
trap cleanup SIGINT SIGTERM

# 启动第一个服务器实例
echo "[Watchdog] 正在启动 Server 1..."
$SERVER_SCRIPT &
PID1=$!
echo "[Watchdog] Server 1 已启动 (PID $PID1)"

# 启动第二个服务器实例
echo "[Watchdog] 正在启动 Server 2..."
$SERVER_SCRIPT &
PID2=$!
echo "[Watchdog] Server 2 已启动 (PID $PID2)"

# 模拟“崩溃并重启”的无限循环
while true; do
    echo
    echo "[Watchdog] 两个服务器正在运行 (PIDs: $PID1, $PID2)。"

    # --- 改进 #1: 使用 'wait' 使 sleep 可中断 ---
    echo "[Watchdog] 等待 $INTERVAL 秒 (按 Ctrl+C 可中断)..."
    sleep $INTERVAL &
    wait $!
    # --- End of improvement ---

    # 随机选择一个进程来“崩溃”
    if (( RANDOM % 2 == 0 )); then
        # === 崩溃并重启 Server 1 ===
        echo "[Watchdog] === 💥 模拟 CRASH (kill -9) 在 Server 1 (PID $PID1) ==="
        kill -9 $PID1 2>/dev/null

        echo "[Watchdog] === 🔄 模拟 RESTART (作为新备节点) ==="
        $SERVER_SCRIPT &
        PID1=$!
        echo "[Watchdog] 新的 Server 1 已启动 (PID $PID1)"
    else
        # === 崩溃并重启 Server 2 ===
        echo "[Watchdog] === 💥 模拟 CRASH (kill -9) 在 Server 2 (PID $PID2) ==="
        kill -9 $PID2 2>/dev/null

        echo "[Watchdog] === 🔄 模拟 RESTART (作为新备节点) ==="
        $SERVER_SCRIPT &
        PID2=$!
        echo "[Watchdog] 新的 Server 2 已启动 (PID $PID2)"
    fi
done