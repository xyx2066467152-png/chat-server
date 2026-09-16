# Chat Server 三端聊天系统

C 语言实现的实时聊天系统，打通 **ARM 开发板 GUI 客户端 ↔ Linux 服务器 ↔ 终端客户端** 三端。

## 功能

- 多客户端通过 TCP 连接，服务器广播群发消息
- 私聊（`to.名字 内容` 按名字路由）
- 上线 / 下线广播通知
- 聊天记录落盘
- ARM 开发板 LVGL 图形客户端（软键盘输入、中文显示、可滚动聊天框）

## 架构

- **服务器**：`epoll` 边缘触发 + 线程池，I/O 与业务分离；`outbox 队列 + eventfd` 把「计算」和「发送」解耦
- **协议**：自定义「4 字节长度头 + JSON」应用层协议，抽成独立接缝 `protocol.c`，三端共用同一份代码
- **GUI 客户端**：LVGL 单线程事件循环 + 非阻塞 socket + 定时轮询

## 目录结构

| 文件 | 职责 |
|---|---|
| `src/server.c` | 服务器主体（epoll 主循环 + 线程池 + 广播 / 私聊） |
| `src/client.c` | 终端客户端 |
| `src/gui_client.c` | ARM 板 LVGL 图形客户端 |
| `src/protocol.c` / `protocol.h` | 协议接缝（JSON ↔ Message 互转） |
| `src/thread_pool.c` / `thread_pool.h` | 通用线程池 |
| `lvgl/` | LVGL v9 GUI 库源码 |
| `lv_font_chinese_16.c` | 预生成中文字库 |
| `Makefile` | x86 构建 + `gui_client_arm` 交叉编译目标 |

## 构建

```bash
make                 # 编译服务器 + 终端客户端（x86）
make gui_client_arm  # 交叉编译 ARM 板 GUI 客户端（需 arm-linux-gcc）
make load_client     # 只编译高并发连接测试客户端
```

## 运行

```bash
./server              # 先起服务器
./client 127.0.0.1    # 终端客户端
```

### 高并发连接测试

`load_client` 使用 epoll 管理连接，不会为每个连接创建线程，适合在 VM 中测试服务器的连接承载能力：

```bash
./load_client -h 127.0.0.1 -p 8888 -c 1000 -d 30
```

消息广播压测（每个连接每秒发送 1 条消息）：

```bash
./load_client -h 127.0.0.1 -p 8888 -c 1000 -d 60 -m 1
```

此模式会统计压测端实际发送的消息数和收到的 `msg` 广播数。群聊广播会排除发送者，因此理想接收数约为
`发送消息数 × (在线连接数 - 1)`；同时结合服务端日志、CPU、内存和 `ss -s` 记录测试条件。

参数说明：

- `-c`：并发连接数（默认 100）
- `-d`：保持连接时间，单位秒（默认 10）
- `-r`：建连速率，单位连接/秒；不指定时立即发起全部连接
- `-m`：每个连接发送的消息数/秒；不指定时只测试连接，不发送消息
- `-h` / `-p`：服务器地址和端口

测试前请在 VM 中确认文件描述符上限（例如 `ulimit -n 20000`），并观察服务器的 CPU、内存、`ss -s` 和日志。测试结束后客户端会输出 TCP 成功数、join 发送数、收到的数据包数和失败数。

## 技术栈

`C` · `Linux` · `epoll` · `pthread 线程池` · `TCP/IP` · `cJSON` · `LVGL` · `framebuffer / evdev` · `交叉编译`

> 项目规格详见 [SPEC.md](SPEC.md)
