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
```

## 运行

```bash
./server              # 先起服务器
./client 127.0.0.1    # 终端客户端
```

## 技术栈

`C` · `Linux` · `epoll` · `pthread 线程池` · `TCP/IP` · `cJSON` · `LVGL` · `framebuffer / evdev` · `交叉编译`

> 项目规格详见 [SPEC.md](SPEC.md)
