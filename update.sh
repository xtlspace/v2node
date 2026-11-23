#!/bin/sh
set -e

# 确保目标目录存在
mkdir -p /node

TARGET_FILE="/node/node"
TEMP_FILE="/node/node.tmp"

# 使用 GitHub 官方 latest/download 固定链接自动获取最新正式版[cite: 1]
BASE_URL="https://github.com/xtlspace/v2node/releases/latest/download"
URL_V3="${BASE_URL}/v2node-linux-amd64-v3"
URL_V2="${BASE_URL}/v2node-linux-amd64-v2"
URL_V1="${BASE_URL}/v2node-linux-amd64-v1"

# 校验系统是否为 x86_64 架构[cite: 1]
ARCH=$(uname -m)
if [ "$ARCH" != "x86_64" ]; then
    echo "[ERROR] 当前系统架构为 $ARCH，仅支持 x86_64。" >&2
    exit 1
fi

# 检查是否支持 x86-64-v3 (关键指令集: avx2, bmi1, bmi2, fma)[cite: 1]
is_v3() {
    grep -q -m1 'avx2' /proc/cpuinfo && \
    grep -q -m1 'bmi1' /proc/cpuinfo && \
    grep -q -m1 'bmi2' /proc/cpuinfo && \
    grep -q -m1 'fma'  /proc/cpuinfo
}

# 检查是否支持 x86-64-v2 (关键指令集: sse4_2, popcnt)[cite: 1]
is_v2() {
    grep -q -m1 'sse4_2' /proc/cpuinfo && \
    grep -q -m1 'popcnt' /proc/cpuinfo
}

# 判断微架构并选择对应下载地址 (v3 -> v2 -> v1 依次降级)[cite: 1]
if is_v3; then
    echo "[INFO] CPU 架构支持: x86-64-v3"
    DOWNLOAD_URL="$URL_V3"
elif is_v2; then
    echo "[INFO] CPU 架构支持: x86-64-v2"
    DOWNLOAD_URL="$URL_V2"
else
    echo "[INFO] CPU 架构支持: x86-64-v1 (基础兼容模式)"
    DOWNLOAD_URL="$URL_V1"
fi

# 下载到临时文件，避免直接写入运行中的文件导致 "Text file busy"[cite: 1]
# 注：curl 的 -L 和 wget 默认行为均支持自动跟踪 GitHub latest 的 302 重定向
echo "[INFO] 正在下载最新版本 ($DOWNLOAD_URL)..."
if command -v curl >/dev/null 2>&1; then
    curl -fL "$DOWNLOAD_URL" -o "$TEMP_FILE"
elif command -v wget >/dev/null 2>&1; then
    wget -O "$TEMP_FILE" "$DOWNLOAD_URL"
else
    echo "[ERROR] 未检测到 curl 或 wget，请先安装下载工具。" >&2
    exit 1
fi

# 赋予执行权限[cite: 1]
chmod +x "$TEMP_FILE"

# 通过 mv 进行原子替换覆盖（解决 Text file busy 问题）[cite: 1]
echo "[INFO] 正在覆盖 $TARGET_FILE..."
mv -f "$TEMP_FILE" "$TARGET_FILE"

# 判断是否为 Alpine 系统[cite: 1]
is_alpine() {
    if [ -f /etc/alpine-release ]; then
        return 0
    elif [ -f /etc/os-release ] && grep -qi '^ID=alpine' /proc/os-release; then
        return 0
    fi
    return 1
}

if is_alpine; then
    echo "[INFO] 检测到 Alpine 系统，正在更新 /etc/init.d/node..."
    cat << 'EOF' > /etc/init.d/node
#!/sbin/openrc-run

#开启quic日志
#export QUIC_GO_LOG_LEVEL=debug

name="node"
command="/node/node"
command_args="server -c /node/config.xtls.json"
command_user="root:root"

supervisor="supervise-daemon"
rc_ulimit="-n 65535"
retry="SIGTERM/5/SIGKILL/5"
respawn_delay="5"
respawn_max="0"

start_pre() {
    pkill -9 -f "${command}.*${command_args}" >/dev/null 2>&1 || true
}
stop_post() {
    pkill -9 -f "${command}.*${command_args}" >/dev/null 2>&1 || true
}

depend() {
    after net dns
    use net
}
EOF
    chmod +x /etc/init.d/node

    # 重启服务[cite: 1]
    echo "[INFO] 正在重启 node 服务..."
    rc-service node restart
    echo "[SUCCESS] 二进制更新及 Alpine 服务重启完成！"
else
    echo "[INFO] 非 Alpine 系统（如 Debian），跳过 OpenRC 配置与服务重启。"
    echo "[SUCCESS] 二进制文件更新完成！"
fi