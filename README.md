# v2node
A v2board backend base on moddified xray-core.
一个基于修改版xray内核的V2board节点服务端。

**注意： 本项目需要搭配[修改版V2board](https://github.com/wyx2685/v2board)**

## 软件安装

### 一键安装

```
wget -N https://raw.githubusercontent.com/wyx2685/v2node/master/script/install.sh && bash install.sh
```

## 会话策略

可在配置文件中增加 `Policy` 块，单位：超时为秒，`BufferSize` 为 KB：

```json
"Policy": {
  "Handshake": 4,
  "ConnIdle": 120,
  "UplinkOnly": 2,
  "DownlinkOnly": 4,
  "BufferSize": 128
}
```

- 不写 `Policy` 块时使用以上默认值
- `BufferSize` 设为 `-1` 表示不限制

## 构建
``` bash
GOEXPERIMENT=jsonv2 go build -v -o build_assets/v2node -trimpath -ldflags "-X 'github.com/wyx2685/v2node/cmd.version=$version' -s -w -buildid="
```

## Stars 增长记录

[![Stargazers over time](https://starchart.cc/wyx2685/v2node.svg?variant=adaptive)](https://starchart.cc/wyx2685/v2node)
