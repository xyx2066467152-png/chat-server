/* ============================================================
 *  gui_client.c —— LVGL 图形界面聊天客户端（跑在 ARM 开发板上）
 *
 *  运行环境：板子 Linux + framebuffer(/dev/fb0) + 触摸(/dev/input/eventX)
 *  复用协议：protocol.c 里的 Message 结构 + 序列化/反序列化
 *           （与服务器端是同一套协议，这就是之前抽出来的"接缝"）
 *
 *  界面：标题栏 / 聊天框(可滚动) / 输入框 / 软键盘(数字+字母+空格+退格+发送)
 *  发送：软键盘「发送」按钮 或 输入框右侧「发送」按钮
 *        —— 对应旧终端客户端里的 //SEND 标志，现在用按钮取代了它
 *
 *  用法：./gui_client <服务器IP> [端口=8888] [触摸设备=/dev/input/event0] [显示设备=/dev/fb0]
 *  例如：./gui_client 192.168.1.100 8888 /dev/input/event0 /dev/fb0
 * ============================================================ */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <poll.h>
#include <arpa/inet.h>      /* htonl / ntohl / inet_pton */
#include <netinet/in.h>
#include <sys/socket.h>

#include "lvgl.h"           /* LVGL 本体 + fbdev/evdev 驱动（见 lv_drivers.h） */
#include "protocol.h"       /* 复用：Message / message_parse / message_serialize */

/* 中文库定义在 lv_font_chinese_16.c 里，这里声明一下就能用 */
LV_FONT_DECLARE(lv_font_chinese_16);

/* ============================================================
 *  全局变量（GUI 是单线程的，所以不用加锁）
 * ============================================================ */
#define DEFAULT_PORT      8888
#define MAX_MSG_LINES     100          /* 聊天框最多保留多少条消息 */
#define RECV_BUF_SIZE     65536        /* 接收缓冲区大小 */

static int  sockfd = -1;               /* 服务器 socket（非阻塞） */
static char nickname[NAME_LEN] = "";   /* 昵称，登录时由用户输入 */

static lv_obj_t *chat_cont;            /* 聊天框（可滚动容器） */
static lv_obj_t *ta;                   /* 输入框（登录时输昵称，登录后输消息） */
static lv_obj_t *title_label;          /* 标题栏文字（登录后更新成昵称） */
static int msg_count = 0;              /* 当前消息条数（用于限流删除） */

static int logged_in = 0;              /* 0 = 还没起名字，1 = 已进入聊天室 */
static const char *server_ip = NULL;   /* 服务器 IP（main 里从 argv 取） */
static int  server_port = DEFAULT_PORT;

/* 非阻塞读可能一次只收到半条消息，需要缓存拼接 */
static uint8_t recv_buf[RECV_BUF_SIZE];
static size_t  recv_len = 0;

/* ============================================================
 *  小工具
 * ============================================================ */

/* 给控件设置中文字体（聊天内容、按钮、标题都可能是中文） */
static void set_font(lv_obj_t *obj)
{
    lv_obj_set_style_text_font(obj, &lv_font_chinese_16, 0);
}

/* 创建一个"透明、无边框、带 flex 布局"的容器，用来做排版 */
static lv_obj_t *make_flex(lv_obj_t *parent, lv_flex_flow_t flow)
{
    lv_obj_t *c = lv_obj_create(parent);
    lv_obj_remove_style_all(c);        /* 去掉默认的背景/边框/内边距 */
    lv_obj_set_flex_flow(c, flow);
    return c;
}

/* epoch 秒 -> "HH:MM:SS" 字符串 */
static void format_time(long epoch, char *buf, size_t n)
{
    time_t t = (time_t)epoch;
    struct tm tmv;
    localtime_r(&t, &tmv);
    strftime(buf, n, "%H:%M:%S", &tmv);
}

/* ============================================================
 *  聊天框：显示一条消息
 * ============================================================ */

static void add_chat_line(const char *text)
{
    lv_obj_t *label = lv_label_create(chat_cont);
    set_font(label);
    lv_label_set_text(label, text);
    lv_label_set_long_mode(label, LV_LABEL_LONG_WRAP);
    lv_obj_set_width(label, LV_PCT(100));

    /* 消息太多时删掉最旧的，防止内存一直涨 */
    msg_count++;
    if (msg_count > MAX_MSG_LINES) {
        lv_obj_t *first = lv_obj_get_child(chat_cont, 0);
        if (first) lv_obj_delete(first);
        msg_count--;
    }

    /* 滚动到底部，显示最新消息 */
    lv_obj_scroll_to_y(chat_cont, LV_COORD_MAX, LV_ANIM_OFF);
}

/* 把一条 Message 显示成一行聊天记录 */
static void display_message(const Message *m)
{
    char t[16] = "--:--:--";
    if (m->time > 0) format_time(m->time, t, sizeof t);

    char line[CONTENT_LEN + NAME_LEN + 64];
    if (strcmp(m->type, "msg") == 0) {
        if (m->to[0] != '\0')   /* 私聊（服务器只发给目标，收到即是对我说的） */
            snprintf(line, sizeof line, "[%s] %s 私聊: %s", t, m->name, m->content);
        else
            snprintf(line, sizeof line, "[%s] %s: %s", t, m->name, m->content);
    } else if (strcmp(m->type, "join") == 0) {
        snprintf(line, sizeof line, "[%s] %s 加入了聊天室", t, m->name);
    } else if (strcmp(m->type, "leave") == 0) {
        snprintf(line, sizeof line, "[%s] %s 离开了聊天室", t, m->name);
    } else {
        return;                 /* 未知类型，忽略 */
    }
    add_chat_line(line);
}

/* ============================================================
 *  网络：非阻塞 socket + 长度前缀分帧
 * ============================================================ */

/* 把整个 buf 发完，处理了部分发送和 EAGAIN（非阻塞下的常见情况） */
static int send_all(int fd, const void *data, size_t len)
{
    const char *p = (const char *)data;
    while (len > 0) {
        ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(2000);           /* 缓冲区满，稍等再试 */
                continue;
            }
            return -1;                  /* 真错误 */
        }
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

/* 长度前缀分帧：4 字节网络序长度 + 数据（和服务器端协议一致） */
static int packet_send(int fd, const char *data, uint32_t len)
{
    uint32_t be = htonl(len);
    if (send_all(fd, &be, 4) < 0) return -1;
    return send_all(fd, data, len);
}

/* 从 recv_buf 里取出完整帧，逐条解析显示（可能攒了好几条） */
static void drain_frames(void)
{
    size_t off = 0;
    while (recv_len - off >= 4) {
        uint32_t body_len;
        memcpy(&body_len, recv_buf + off, 4);
        body_len = ntohl(body_len);

        /* 合理性检查：一条正常 JSON 不会超过 4KB */
        if (body_len == 0 || body_len > 4096) {
            recv_len = 0;               /* 数据错位，整包丢弃等下一次 */
            return;
        }

        if (recv_len - off < 4 + body_len) break;   /* 还没收齐，等下次 */

        char *json = malloc(body_len + 1);
        if (json == NULL) { recv_len = 0; return; }
        memcpy(json, recv_buf + off + 4, body_len);
        json[body_len] = '\0';
        off += 4 + body_len;

        Message m;
        if (message_parse(json, &m) == 0)
            display_message(&m);
        free(json);
    }

    /* 把没消费完的剩余数据挪到缓冲区开头 */
    if (off > 0) {
        memmove(recv_buf, recv_buf + off, recv_len - off);
        recv_len -= off;
    }
}

/* 定时器回调：轮询 socket 收数据（跑在主线程里，和界面不冲突） */
static void poll_socket(lv_timer_t *t)
{
    (void)t;
    if (sockfd < 0) return;

    /* 尽量读，直到 EAGAIN（没有更多数据） */
    while (1) {
        ssize_t n = recv(sockfd, recv_buf + recv_len, RECV_BUF_SIZE - recv_len, 0);
        if (n > 0) {
            recv_len += (size_t)n;
            if (recv_len >= RECV_BUF_SIZE) {    /* 缓冲区满了（异常） */
                recv_len = 0;
                break;
            }
        } else if (n == 0) {
            /* 服务器主动断开 */
            add_chat_line("--- 服务器连接断开 ---");
            close(sockfd);
            sockfd = -1;
            return;
        } else { /* n < 0 */
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;  /* 读完了 */
            if (errno == EINTR) continue;
            add_chat_line("--- 连接出错 ---");
            close(sockfd);
            sockfd = -1;
            return;
        }
    }

    drain_frames();
}

/* 非阻塞 connect，带 5 秒超时，避免服务器不通时卡死 */
static int connect_to_server(const char *ip, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "无效的 IP 地址: %s\n", ip);
        close(fd);
        return -1;
    }

    int rc = connect(fd, (struct sockaddr *)&addr, sizeof addr);
    if (rc < 0 && errno != EINPROGRESS) {
        perror("connect");
        close(fd);
        return -1;
    }

    /* 等待连接建立（最多 5 秒） */
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLOUT;
    rc = poll(&pfd, 1, 5000);
    if (rc <= 0) {
        fprintf(stderr, "连接服务器超时\n");
        close(fd);
        return -1;
    }

    /* 检查连接是否真的成功 */
    int err = 0;
    socklen_t elen = sizeof err;
    getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &elen);
    if (err != 0) {
        errno = err;
        perror("connect");
        close(fd);
        return -1;
    }

    return fd;      /* 保持非阻塞，交给 poll_socket 定时读取 */
}

/* ============================================================
 *  发送：把输入框内容作为 msg 发出去
 * ============================================================ */

static void do_send(void)
{
    const char *text = lv_textarea_get_text(ta);
    if (text == NULL || text[0] == '\0') return;   /* 空内容不处理 */

    /* ---- 还没起名字：第一次点发送 = 用输入内容当昵称，连服务器进聊天室 ---- */
    if (!logged_in) {
        strncpy(nickname, text, NAME_LEN - 1);
        nickname[NAME_LEN - 1] = '\0';

        sockfd = connect_to_server(server_ip, server_port);
        if (sockfd < 0) {
            add_chat_line("--- 连接服务器失败 ---");
            lv_textarea_set_text(ta, "");
            return;
        }

        Message join;
        memset(&join, 0, sizeof join);
        strncpy(join.type, "join", TYPE_LEN - 1);
        strncpy(join.name, nickname, NAME_LEN - 1);
        char *json = message_serialize(&join);
        if (json) {
            packet_send(sockfd, json, (uint32_t)strlen(json));
            free(json);
        }

        lv_timer_create(poll_socket, 50, NULL);   /* 开始轮询收消息 */

        logged_in = 1;
        char title[128];
        snprintf(title, sizeof title, "轻聊 LiteChat  -  %s", nickname);
        lv_label_set_text(title_label, title);
        lv_textarea_set_placeholder_text(ta, "输入消息...");
        add_chat_line("--- 已连接服务器 ---");

        lv_textarea_set_text(ta, "");
        return;
    }

    /* ---- 已进入：解析私聊 "to.名字 内容"，没有 to. 就是群发 ---- */
    Message m;
    memset(&m, 0, sizeof m);
    strncpy(m.type, "msg", TYPE_LEN - 1);
    strncpy(m.name, nickname, NAME_LEN - 1);

    const char *content = text;                     /* 默认整段都是正文 */
    if (strncmp(text, "to.", 3) == 0) {             /* 以 to. 开头 = 私聊 */
        const char *name_start = text + 3;
        const char *space = strchr(name_start, ' ');
        if (space != NULL && space > name_start) {  /* "to.名字 内容" */
            size_t nlen = (size_t)(space - name_start);
            if (nlen >= NAME_LEN) nlen = NAME_LEN - 1;
            memcpy(m.to, name_start, nlen);
            m.to[nlen] = '\0';
            content = space + 1;                    /* 正文 = 空格后面 */
        }
        /* 没有空格说明格式不对（只有 "to.名字"），当普通群发处理 */
    }
    strncpy(m.content, content, CONTENT_LEN - 1);
    /* m.time = 0，时间戳由服务器统一填 */

    char *json = message_serialize(&m);
    int ok = 0;
    if (json != NULL) {
        ok = (packet_send(sockfd, json, (uint32_t)strlen(json)) == 0);
        free(json);
    }

    /* 本地回显：服务器不把消息回发给发送者，自己补一条显示在聊天框 */
    if (ok) {
        /* 最坏情况：前缀 9 + to 63 + content 511 + 结尾 0 = 584，缓冲区要够大 */
        char echo[CONTENT_LEN + NAME_LEN + 16];
        if (m.to[0] != '\0')
            snprintf(echo, sizeof echo, "我 -> %s: %s", m.to, m.content);
        else
            snprintf(echo, sizeof echo, "我: %s", m.content);
        add_chat_line(echo);
    } else {
        add_chat_line("--- 发送失败 ---");
    }

    lv_textarea_set_text(ta, "");   /* 发完清空输入框 */
}

/* ============================================================
 *  软键盘
 * ============================================================ */

/* 软键盘 / 发送按钮 共用的点击回调 */
static void kb_key_event(lv_event_t *e)
{
    const char *key = (const char *)lv_event_get_user_data(e);
    if (key == NULL) return;

    if (strcmp(key, "退格") == 0) {
        lv_textarea_delete_char(ta);
    } else if (strcmp(key, "空格") == 0) {
        lv_textarea_add_char(ta, ' ');
    } else if (strcmp(key, "发送") == 0) {
        do_send();
    } else {
        lv_textarea_add_text(ta, key);   /* 字母 / 数字 / 符号直接追加 */
    }
}

static void build_keyboard(lv_obj_t *parent)
{
    static const char *row0[] = {"1","2","3","4","5","6","7","8","9","0", NULL};
    static const char *row1[] = {"q","w","e","r","t","y","u","i","o","p", NULL};
    static const char *row2[] = {"a","s","d","f","g","h","j","k","l", NULL};
    static const char *row3[] = {"z","x","c","v","b","n","m",",",".","退格", NULL};
    static const char *row4[] = {"空格", "发送", NULL};
    static const char **rows[] = { row0, row1, row2, row3, row4, NULL };

    lv_obj_t *kb = make_flex(parent, LV_FLEX_FLOW_COLUMN);
    lv_obj_set_width(kb, LV_PCT(100));
    lv_obj_set_height(kb, 260);
    lv_obj_set_style_bg_color(kb, lv_color_hex(0x181825), 0);
    lv_obj_set_style_pad_all(kb, 6, 0);
    lv_obj_set_style_pad_gap(kb, 6, 0);

    for (int r = 0; rows[r]; r++) {
        lv_obj_t *row = make_flex(kb, LV_FLEX_FLOW_ROW);
        lv_obj_set_flex_grow(row, 1);
        lv_obj_set_width(row, LV_PCT(100));
        lv_obj_set_style_pad_gap(row, 6, 0);

        for (int k = 0; rows[r][k]; k++) {
            const char *key = rows[r][k];

            lv_obj_t *btn = lv_button_create(row);
            lv_obj_set_flex_grow(btn, 1);      /* 每个键均匀撑满一行 */

            lv_obj_t *lbl = lv_label_create(btn);
            set_font(lbl);
            lv_label_set_text(lbl, key);
            lv_obj_center(lbl);

            lv_obj_add_event_cb(btn, kb_key_event, LV_EVENT_CLICKED, (void *)key);
        }
    }
}

/* ============================================================
 *  界面搭建
 * ============================================================ */

static void build_ui(void)
{
    lv_obj_t *scr = lv_screen_active();

    /* 根容器：竖向 flex，填满整个屏幕 */
    lv_obj_t *root = make_flex(scr, LV_FLEX_FLOW_COLUMN);
    lv_obj_set_size(root, LV_PCT(100), LV_PCT(100));
    lv_obj_set_style_pad_gap(root, 0, 0);

    /* 1. 标题栏 */
    lv_obj_t *title_bar = lv_obj_create(root);
    lv_obj_remove_style_all(title_bar);
    lv_obj_set_width(title_bar, LV_PCT(100));
    lv_obj_set_height(title_bar, LV_SIZE_CONTENT);
    lv_obj_set_style_bg_color(title_bar, lv_color_hex(0x1E1E2E), 0);
    lv_obj_set_style_pad_ver(title_bar, 8, 0);
    lv_obj_set_style_pad_hor(title_bar, 12, 0);

    title_label = lv_label_create(title_bar);
    set_font(title_label);
    lv_label_set_text(title_label, "轻聊 LiteChat  -  请输入昵称");

    /* 2. 聊天框（可滚动） */
    chat_cont = make_flex(root, LV_FLEX_FLOW_COLUMN);
    lv_obj_set_flex_grow(chat_cont, 1);
    lv_obj_set_width(chat_cont, LV_PCT(100));
    lv_obj_set_style_pad_all(chat_cont, 8, 0);
    lv_obj_set_style_pad_gap(chat_cont, 4, 0);
    lv_obj_set_scroll_dir(chat_cont, LV_DIR_VER);
    lv_obj_set_scrollbar_mode(chat_cont, LV_SCROLLBAR_MODE_AUTO);

    /* 3. 输入行：输入框 + 发送按钮 */
    lv_obj_t *input_row = make_flex(root, LV_FLEX_FLOW_ROW);
    lv_obj_set_width(input_row, LV_PCT(100));
    lv_obj_set_height(input_row, LV_SIZE_CONTENT);
    lv_obj_set_style_pad_all(input_row, 8, 0);
    lv_obj_set_style_pad_gap(input_row, 8, 0);

    ta = lv_textarea_create(input_row);
    set_font(ta);
    lv_obj_set_flex_grow(ta, 1);
    lv_textarea_set_one_line(ta, true);
    lv_textarea_set_max_length(ta, CONTENT_LEN - 1);
    lv_textarea_set_placeholder_text(ta, "输入昵称，点发送进入");

    lv_obj_t *send_btn = lv_button_create(input_row);
    lv_obj_set_width(send_btn, 120);
    lv_obj_t *send_lbl = lv_label_create(send_btn);
    set_font(send_lbl);
    lv_label_set_text(send_lbl, "发送");
    lv_obj_center(send_lbl);
    lv_obj_add_event_cb(send_btn, kb_key_event, LV_EVENT_CLICKED, "发送");

    /* 4. 软键盘 */
    build_keyboard(root);
}

/* ============================================================
 *  main
 * ============================================================ */

static void usage(const char *prog)
{
    printf("用法: %s <服务器IP> [端口=%d] [触摸设备=%s] [显示设备=%s]\n",
           prog, DEFAULT_PORT, "/dev/input/event6", "/dev/fb0");
    printf("例如: %s 192.168.1.100 8888 /dev/input/event0 /dev/fb0\n", prog);
}

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    server_ip   = argv[1];
    server_port = (argc > 2) ? atoi(argv[2]) : DEFAULT_PORT;
    const char *touch_dev = (argc > 3) ? argv[3] : "/dev/input/event6";
    const char *fb_dev    = (argc > 4) ? argv[4] : "/dev/fb0";

    /* 1. 初始化 LVGL */
    lv_init();

    /* 2. 显示驱动：framebuffer（分辨率/颜色位数自动从 fb 设备读取） */
    lv_display_t *disp = lv_linux_fbdev_create();
    lv_linux_fbdev_set_file(disp, fb_dev);

    /* 3. 触摸驱动：evdev（POINTER 表示触摸屏/鼠标） */
    lv_indev_t *indev = lv_evdev_create(LV_INDEV_TYPE_POINTER, touch_dev);
    if (indev == NULL) {
        fprintf(stderr, "无法打开触摸设备 %s（用 ls /dev/input/ 查看实际设备名）\n", touch_dev);
        return 1;
    }

    /* 4. 搭界面（先显示登录界面，点「发送」时才真正连服务器） */
    build_ui();

    /* 5. 主循环：不停地跑 LVGL */
    while (1) {
        lv_timer_handler();
        usleep(5000);
    }

    return 0;   /* 实际不会走到这里 */
}
