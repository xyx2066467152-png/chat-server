/**
 * ======================================================================
 *  聊天室服务器（epoll 边缘触发 + 长度前缀协议）
 * 
 *  协议说明：
 *    每个消息包由 4 字节长度头（网络字节序） + 实际数据组成。
 *    长度头指示后续数据的字节数（不包括头本身）。
 *    这能彻底解决 TCP 粘包/半包问题。
 * 
 *  功能：
 *    - 客户端连接后首先发送用户名（作为登录包）
 *    - 服务器广播所有聊天消息，并显示发送者
 *    - 客户端断开时广播下线通知
 * ======================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <pthread.h>
#include "protocol.h"
#include "thread_pool.h"
#include <time.h>          // time(NULL)
#include <sys/eventfd.h>


// ========================== 宏定义 ==========================
#define MAX_EVENTS   1024    // epoll 可监控的最大事件数（同时在线人数上限）
#define BUFFER_SIZE  1024    // 临时接收缓冲区大小
#define NAME_LEN     64      // 用户名最大长度
#define READ_BUF_SIZE 40960  // 每个客户端连接的读缓冲区大小（存放粘包数据）

  
static volatile sig_atomic_t running = 1;
// ========================== 用户结构体 ==========================
/**
 * 每个在线客户端的信息
 * 注意：read_buf 和 read_len 用于累积接收数据，解决粘包/半包
 */
struct user {
    int fd;                          // 客户端 socket 文件描述符
    char name[NAME_LEN];             // 用户名
    int online;                      // 1=已登录，0=未登录（等待用户名）
    char read_buf[READ_BUF_SIZE];    // 该连接的读缓冲区（累积未处理的数据）
    int read_len;                    // 当前缓冲区中有效数据的字节数
} users[MAX_EVENTS];

// ---- 任务：主线程 → worker ----
typedef struct {
    int     sender_fd;   // 发送者 fd（广播时排除自己）
    int     slot;        // 发送者槽位（worker 靠它找 users[slot]）
    Message msg;         // 已解析的消息（整体赋值 = 深拷贝）
} Task;

// ---- 下行结果：worker → 主线程 ----
typedef struct OutMsg{
    int   sender_fd;
    char  to[NAME_LEN];  // 私聊目标用户名（空 = 群发所有人）
    char *json;          // 已序列化报文（malloc，主线程负责 free）
    struct OutMsg *next;
} OutMsg;

static void handle_shutdown(int sig) {
    (void)sig;
    running = 0;                              // 信号处理里只置标志，其它什么都不做
}

// ========================== 全局变量 ==========================
// ---- outbox（待发送队列）+ 它的锁 ----
static OutMsg *outbox_head = NULL;
static OutMsg *outbox_tail = NULL;
static pthread_mutex_t outbox_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t users_lock = PTHREAD_MUTEX_INITIALIZER;

// ---- 线程池指针（main 里 create）----
static threadpool_t *pool = NULL;
static int evfd = -1; 

// ========================== 函数声明 ==========================
void init_clients(void);
int  find_empty_slot(void);
int  process_message(struct user *u, const Message *in, Message *out);
void broadcast_to_all(int sender_fd, const char *msg);
void send_to_user(const char *to, const char *msg);
int  set_nonblocking(int fd);
void drain_outbox(void);
void business_worker(void *arg);
void outbox_push(int sender_fd, const char *to, char *json);
int  parse_packet(int slot);
void close_client(int slot, int epoll_fd);
void log_message(const char *line);

static int log_fd = -1;    // 日志文件描述符，main 里 open 一次


// ========================== 初始化函数 ==========================
/**
 * 初始化所有用户结构体，清空数据
 */
void init_clients(void) {
    for (int i = 0; i < MAX_EVENTS; i++) {
        users[i].fd = -1;
        users[i].online = 0;
        memset(users[i].name, 0, NAME_LEN);
        users[i].read_len = 0;   // 清空读缓冲区
    }
}

/**
 * 查找一个空闲槽位（fd == -1 表示空闲）
 * 返回槽位索引，若满则返回 -1
 */
int find_empty_slot(void) {
    for (int i = 0; i < MAX_EVENTS; i++) {
        if (users[i].fd == -1)
            return i;
    }
    return -1;
}
// 业务接缝：输入「发送者 u + 收到的消息 in」，输出「要广播的下行消息 out」
// 返回值：
//    1  = 要广播，*out 已填好（type/name/content/time 齐全）
//    0  = 不广播（合法但无需广播，比如未知 type）
//   -1  = 非法消息（应关闭连接）
// 约束：不碰 send/recv/fd/epoll/日志文件 —— 只算「发什么」，所以能脱离网络单测
int process_message(struct user *u, const Message *in, Message *out) {
    *out = *in;                       // 先整体复制，再改 name/time

    if (u->online == 0) {
        // ---- 登录包：type 必须是 "join" ----
        if (strcmp(in->type, "join") != 0)
            return -1;

        // 保存用户名 + 标记在线（这是「改内存状态」，不是 I/O，所以算业务）
        strncpy(u->name, in->name, NAME_LEN - 1);
        u->name[NAME_LEN - 1] = '\0';
        u->online = 1;

        // 欢迎消息：name 用客户端自己报的，补个时间戳
        out->time = time(NULL);
        return 1;

    } else {
        // ---- 已登录：只处理 "msg" ----
        if (strcmp(in->type, "msg") == 0) {
            strncpy(out->name, u->name, NAME_LEN - 1);  // 名字用服务器存的真实名
            out->name[NAME_LEN - 1] = '\0';
            out->time = time(NULL);
            return 1;
        }
        return 0;   // 其它 type：忽略，不广播也不关连接
    }
}
// ========================== 广播函数 ==========================
/**
 * 向所有在线客户端（除发送者本人外）广播一条消息
 * 发送格式：4字节长度头（网络序） + 消息数据
 */
void broadcast_to_all(int sender_fd, const char *msg) {
    uint32_t msg_len = strlen(msg);
    uint32_t net_len = htonl(msg_len);

    pthread_mutex_lock(&users_lock);
    for (int i = 0; i < MAX_EVENTS; i++) {
        if (users[i].fd != -1 && users[i].fd != sender_fd && users[i].online) {
            send(users[i].fd, &net_len, 4, MSG_NOSIGNAL);
            send(users[i].fd, msg, msg_len, MSG_NOSIGNAL);
        }
    }
    pthread_mutex_unlock(&users_lock);
}

// 私聊：只发给指定用户名的那个用户（按名字查 fd）
void send_to_user(const char *to, const char *msg) {
    uint32_t msg_len = strlen(msg);
    uint32_t net_len = htonl(msg_len);

    pthread_mutex_lock(&users_lock);
    for (int i = 0; i < MAX_EVENTS; i++) {
        if (users[i].fd != -1 && users[i].online && strcmp(users[i].name, to) == 0) {
            send(users[i].fd, &net_len, 4, MSG_NOSIGNAL);
            send(users[i].fd, msg, msg_len, MSG_NOSIGNAL);
        }
    }
    pthread_mutex_unlock(&users_lock);
}


void log_message(const char *line) {
    if (log_fd < 0) return;                        // 打开失败就跳过，不影响聊天
    size_t len = strlen(line);
    if (write(log_fd, line, len) == -1 || write(log_fd, "\n", 1) == -1)
        perror("write chat.log");                  // 打日志，但不崩
}

// ========================== 工具函数 ==========================
/**
 * 将套接字设置为非阻塞模式（epoll 边缘触发必须）
 */
int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1)
        return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}
// ========================== 线程函数 ==========================
// 入队：worker 塞结果
void outbox_push(int sender_fd, const char *to, char *json) {
    OutMsg *m = malloc(sizeof(OutMsg));
    m->sender_fd = sender_fd;
    strncpy(m->to, to, NAME_LEN - 1);
    m->to[NAME_LEN - 1] = '\0';
    m->json = json;
    m->next = NULL;
    pthread_mutex_lock(&outbox_lock);
    if (outbox_tail == NULL) outbox_head = outbox_tail = m;
    else { outbox_tail->next = m; outbox_tail = m; }
    pthread_mutex_unlock(&outbox_lock);

    uint64_t one = 1;
    if (write(evfd, &one, sizeof(one)) == -1)   // 唤醒主线程
        perror("write eventfd");                 // 只在退出时 evfd 被关才失败，可忽略
}
// 出队：主线程一次性取走整条链，锁外再发
void drain_outbox(void) {
    pthread_mutex_lock(&outbox_lock);
    OutMsg *list = outbox_head;
    outbox_head = outbox_tail = NULL;
    pthread_mutex_unlock(&outbox_lock);

    for (OutMsg *m = list; m; ) {
        if (m->to[0] != '\0')
            send_to_user(m->to, m->json);             // 私聊：只发给目标用户
        else
            broadcast_to_all(m->sender_fd, m->json);  // 群发：广播给所有人（排除发送者）
        log_message(m->json);
        OutMsg *next = m->next;
        free(m->json);
        free(m);
        m = next;
    }
}
// worker 干的活：业务 + 序列化，产出塞 outbox
void business_worker(void *arg) {
    Task *t = (Task *)arg;
    struct user *u = &users[t->slot];

    Message out = {0};
    pthread_mutex_lock(&users_lock);              // ★ 锁
    int r = process_message(u, &t->msg, &out);    // 里面读/写 online、name
    pthread_mutex_unlock(&users_lock);            // ★ 解锁
    // 之后的 serialize / outbox_push 不再碰 users[]，锁外做

    if (r == 1) {
        char *json = message_serialize(&out);
        outbox_push(t->sender_fd, out.to, json);
    }
    free(t);
}



// ========================== 包解析器 ==========================
/**
 * 从指定的用户槽位中解析出一个完整的协议包。
 *
 * 协议格式：
 *   [4字节长度头（网络字节序）] + [数据]
 *
 * 返回值：
 *    1  : 成功解析并处理了一个包
 *    0  : 数据不足，需要等待更多数据
 *   -1  : 协议错误（包长度异常），应关闭连接
 *
 * 处理过程：
 *   1. 检查缓冲区中是否有至少 4 字节（长度头）
 *   2. 读取长度头，转为主机序
 *   3. 校验长度合法性
 *   4. 检查缓冲区是否已有完整的包（4 + 包体长度）
 *   5. 提取数据，根据登录状态处理（登录包或聊天包）
 *   6. 将未处理的数据移到缓冲区开头，更新 read_len
 */
int parse_packet(int slot) {
    struct user *u = &users[slot];
    char *buf = u->read_buf;
    int len = u->read_len;

    if (len < 4)
        return 0;

    uint32_t pkt_len;
    memcpy(&pkt_len, buf, sizeof(pkt_len));
    pkt_len = ntohl(pkt_len);
    // memcpy 保证任意对齐安全（避免 *(uint32_t*)buf 的未对齐 UB）
    if (pkt_len > READ_BUF_SIZE - 5)             // ← 顺手把 off-by-one 也修了
        return -1;
    if (len < (int)(4 + pkt_len))
        return 0;

    char *data = buf + 4;
    data[pkt_len] = '\0';

    // ---- 先解析 JSON（登录、聊天都走这里，只调一次） ----
        // ---- 解析 JSON ----
    Message msg = {0};
    if (message_parse(data, &msg) != 0)
        return -1;

    // ---- 业务接缝：只算「发什么」 ----
    // 分帧 + parse 后，业务交给 worker，主线程只负责提交
    Task *t = malloc(sizeof(Task));
    t->sender_fd = u->fd;
    t->slot = slot;
    t->msg = msg;                         // 整体赋值 = 深拷贝，read_buf 可放心复用
    threadpool_submit(pool, business_worker, t);

    // 消费掉这个包（memmove 照旧）
    int consumed = 4 + pkt_len;
    if (len > consumed) { memmove(buf, buf+consumed, len-consumed); u->read_len = len-consumed; }
    else u->read_len = 0;
    return 1;



}



// ========================== 关闭客户端函数 ==========================
/**
 * 统一清理一个客户端连接：
 *   - 如果已登录，广播下线消息
 *   - 从 epoll 中移除 fd
 *   - 关闭 socket
 *   - 重置结构体
 */
void close_client(int slot, int epoll_fd) {
    if (slot < 0 || slot >= MAX_EVENTS) return;
    struct user *u = &users[slot];

    // ① 锁内：读状态 + 立刻把槽位「摘掉」（fd=-1），原子完成，worker 读不到半吊子状态
    pthread_mutex_lock(&users_lock);
    if (u->fd == -1) {                    // 已经关过了，防御
        pthread_mutex_unlock(&users_lock);
        return;
    }
    int fd         = u->fd;
    int was_online = u->online;
    char name[NAME_LEN];
    strncpy(name, u->name, NAME_LEN - 1);
    name[NAME_LEN - 1] = '\0';
    u->fd      = -1;                       // 先摘掉，broadcast 就再也发不到它
    u->online  = 0;
    memset(u->name, 0, NAME_LEN);
    u->read_len = 0;
    pthread_mutex_unlock(&users_lock);

    // ② 锁外：广播下线（broadcast 内部自己加锁，这里绝不能还持着 users_lock）
    if (was_online) {
        Message m = {0};
        strcpy(m.type, "leave");
        strncpy(m.name, name, NAME_LEN - 1);
        m.time = time(NULL);
        char *out = message_serialize(&m);
        broadcast_to_all(fd, out);         // 用局部变量 fd，不是 u->fd
        log_message(out);
        free(out);
        printf("用户 [%s] 已断开 (fd=%d)\n", name, fd);
    }

    // ③ 关 fd、移出 epoll（不碰 users[]，锁外做）
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
    close(fd);
}


// ========================== 主函数 ==========================
int main(void) {

    
    // 忽略 SIGPIPE，防止 send 到已关闭的连接时进程崩溃
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_shutdown;
    sigemptyset(&sa.sa_mask);                     // 不设 SA_RESTART → epoll_wait 会被信号打断返回 EINTR
    sigaction(SIGINT,  &sa, NULL);                // Ctrl-C
    sigaction(SIGTERM, &sa, NULL);                // kill
    // 1. 创建监听套接字
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

          
    // 建线程池（4 个 worker 开始空转等待）

    // 2. 设置端口重用（便于快速重启）
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        perror("setsockopt");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    // 3. 绑定地址
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;   // 监听所有网卡
    server_addr.sin_port = htons(8888);         // 端口 8888

    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        perror("bind");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    // 4. 开始监听
    if (listen(server_fd, 128) == -1) {
        perror("listen");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    // 5. 将监听套接字设为非阻塞（epoll 边缘触发必须）
    if (set_nonblocking(server_fd) == -1) {
        perror("set_nonblocking");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    // 6. 初始化客户端数组
    init_clients();

    // 7. 创建 epoll 实例
    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        perror("epoll_create1");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    pool = threadpool_create();  
    evfd = eventfd(0, EFD_NONBLOCK);
    if (evfd == -1) { perror("eventfd"); close(epoll_fd); exit(EXIT_FAILURE); }

    struct epoll_event ev_evfd;
    ev_evfd.events = EPOLLIN;              // 注意：不加 EPOLLET，唤醒信号用「电平触发」更稳
    ev_evfd.data.fd = evfd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, evfd, &ev_evfd) == -1) {
    perror("epoll_ctl: eventfd"); close(epoll_fd); exit(EXIT_FAILURE);
}

    // 8. 注册监听套接字到 epoll（关注可读事件，边缘触发）
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = server_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) == -1) {
        perror("epoll_ctl: server_fd");
        close(server_fd);
        close(epoll_fd);
        exit(EXIT_FAILURE);
    }

    printf("聊天服务器启动，监听端口 8888（epoll 边缘触发）\n");

    log_fd = open("chat.log", O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (log_fd == -1)
    perror("open chat.log");     // 只告警，不退出

    // 9. 事件循环
    struct epoll_event events[MAX_EVENTS];
    char buffer[BUFFER_SIZE];

    while (running) {                                          // ① while(1) → while(running)
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);   // ② 100 → -1
        if (nfds == -1) {
            if (errno == EINTR) {
                if (!running) break;                           // ③ 信号让退出就 break
                continue;                                      //    其它信号继续等
            }
            perror("epoll_wait");
            break;
        }


        // 处理每个就绪事件
        for (int i = 0; i < nfds; i++) {
            int fd = events[i].data.fd;

            // ④ eventfd 可读 → 读空计数（它只是唤醒信号，实际数据在 outbox）
            if (fd == evfd) {
                uint64_t val;
                while (read(evfd, &val, sizeof(val)) == (ssize_t)sizeof(val)) { /* 读空 */ }
                continue;
            }

            // ----- 新连接到达（监听套接字可读） -----
            if (fd == server_fd) {

                while (1) {
                    struct sockaddr_in client_addr;
                    socklen_t client_len = sizeof(client_addr);
                    int conn_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
                    if (conn_fd == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            // 所有新连接已处理完
                            break;
                        } else if (errno == EINTR) {
                            continue;   // 被信号中断，重试
                        } else {
                            perror("accept");
                            break;
                        }
                    }

                    // 打印客户端信息
                    char ip[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));
                    printf("新连接: %s:%d (fd=%d)\n", ip, ntohs(client_addr.sin_port), conn_fd);

                    // 查找空闲槽位
                    int slot = find_empty_slot();
                    if (slot == -1) {
                        printf("服务器已满，拒绝连接 fd=%d\n", conn_fd);
                        close(conn_fd);
                        continue;
                    }

                    // 初始化该槽位
                    users[slot].fd = conn_fd;
                    users[slot].online = 0;
                    memset(users[slot].name, 0, NAME_LEN);
                    users[slot].read_len = 0;

                    // 设为非阻塞
                    if (set_nonblocking(conn_fd) == -1) {
                        perror("set_nonblocking");
                        close(conn_fd);
                        users[slot].fd = -1;
                        continue;
                    }

                    // 注册到 epoll（边缘触发）
                    ev.events = EPOLLIN | EPOLLET;
                    ev.data.fd = conn_fd;
                    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn_fd, &ev) == -1) {
                        perror("epoll_ctl: conn_fd");
                        close(conn_fd);
                        users[slot].fd = -1;
                    }
                }
                continue;   // 处理完新连接，继续下一个事件
            }

            // ----- 已连接客户端的数据 -----
            // 根据 fd 查找对应的槽位
            int slot = -1;
            for (int j = 0; j < MAX_EVENTS; j++) {
                if (users[j].fd == fd) {
                    slot = j;
                    break;
                }
            }
            if (slot == -1) {
                // 找不到对应槽位（可能已被清理），直接关闭
                close(fd);
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
                continue;
            }

            // ---- 循环读取数据（边缘触发必须读到 EAGAIN） ----
            while (1) {
                ssize_t n = recv(fd, buffer, sizeof(buffer) - 1, 0);
                if (n > 0) {
                    // 检查缓冲区是否溢出
                    if (users[slot].read_len + n > READ_BUF_SIZE) {
                        // 缓冲区已满，视为攻击或异常，关闭连接
                        close_client(slot, epoll_fd);
                        break;
                    }
                    // 追加数据到该连接的读缓冲区
                    memcpy(users[slot].read_buf + users[slot].read_len, buffer, n);
                    users[slot].read_len += n;

                    // 循环解析缓冲区中的所有完整包
                    while (1) {
                        int ret = parse_packet(slot);
                        if (ret == 0) {
                            // 数据不够，跳出解析循环，继续收数据
                            break;
                        } else if (ret == -1) {
                            // 协议错误，关闭连接
                            close_client(slot, epoll_fd);
                            break;
                        }
                        // ret == 1 继续解析（可能缓冲区中还有粘包）
                    }
                } else if (n == 0) {
                    // 客户端主动关闭连接
                    close_client(slot, epoll_fd);
                    break;
                } else { // n == -1
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        // 数据已读完，正常退出 recv 循环
                        break;
                    } else if (errno == EINTR) {
                        // 被信号中断，重试
                        continue;
                    } else {
                        // 真正的错误
                        perror("recv");
                        close_client(slot, epoll_fd);
                        break;
                    }
                }
            }
        }
        drain_outbox();   // ⑤ 每轮事件处理完，统一把 outbox 里的结果发出去
    }

    // 10. 清理资源（正常退出时执行）
    threadpool_destroy(pool);   // stop=1 + broadcast + join 所有 worker，处理完剩余任务再退
    drain_outbox();             // 把最后残留的结果发掉 + free，避免泄漏
    close(evfd);
    close(log_fd);
    close(server_fd);
    close(epoll_fd);

    return 0;
}