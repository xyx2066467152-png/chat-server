#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <signal.h>
#include "protocol.h"

#define SERVER_PORT 8888
#define BUFFER_SIZE 4096
#define MAX_MSG_SIZE 512
#define SEND_FLAG "//SEND"   // 发送结束标志

struct root {
    int fd;
    char name[256];
} user;

int send_packet(int fd, const char *data, size_t len) {
    uint32_t net_len = htonl((uint32_t)len);
    if (send(fd, &net_len, 4, MSG_NOSIGNAL) != 4)
        return -1;
    if (send(fd, data, len, MSG_NOSIGNAL) != (ssize_t)len)
        return -1;
    return 0;
}

int recv_packet(int fd, char *buf, size_t buf_size) {
    uint32_t net_len;
    size_t head_received = 0;
    char *head_ptr = (char*)&net_len;          // 将 net_len 视为字节数组

    while (head_received < 4) {
        ssize_t n = recv(fd, head_ptr + head_received, 4 - head_received, 0);
        if (n <= 0) {
            // n == 0：对端关闭；n < 0：出错
            return -1;
        }
        head_received += n;
    }
    uint32_t data_len = ntohl(net_len);
    if (data_len >= buf_size - 1)
        return -1;
    size_t received = 0;
    while (received < data_len) {
        ssize_t ret = recv(fd, buf + received, data_len - received, 0);
        if (ret <= 0) return -1;
        received += ret;
    }
    buf[data_len] = '\0';
    return (int)data_len;
}

int main(int argc,char *argv[]) {
    if(argc!=2)
    {
        fprintf(stderr, "用法: %s <服务器IP>\n", argv[0]);
        return 1;
    }
    signal(SIGPIPE, SIG_IGN);

    user.fd = socket(AF_INET, SOCK_STREAM, 0);
    if (user.fd < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);

    printf("请输入用户名: ");
    fgets(user.name, sizeof(user.name), stdin);
    user.name[strcspn(user.name, "\n")] = '\0';

   if (inet_pton(AF_INET, argv[1], &server_addr.sin_addr) != 1) {
    fprintf(stderr, "无效的 IP 地址: %s\n", argv[1]);
    close(user.fd);
    exit(EXIT_FAILURE);
    }

    if (connect(user.fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        close(user.fd);
        exit(EXIT_FAILURE);
    }

   Message m = {0};
    strcpy(m.type, "join");
    strncpy(m.name, user.name, sizeof(m.name) - 1);

    char *out = message_serialize(&m);
    send_packet(user.fd, out, strlen(out));
    free(out);


    printf("已连接服务器。输入多行消息，结束行输入 `//SEND` 即可一次性发送。\n");
    printf("提示：可以粘贴任意包含换行符的文本，最后单独一行输入 //SEND\n");
    printf("私聊：输入 \"to.对方名字 内容\" 再 //SEND，只发给对方一个人\n");

    char buf[BUFFER_SIZE];
    char msg_buf[MAX_MSG_SIZE];
    int msg_len = 0;

    fd_set readfds;
    int max_fd = (user.fd > STDIN_FILENO) ? user.fd : STDIN_FILENO;

    for (;;) {
        FD_ZERO(&readfds);
        FD_SET(STDIN_FILENO, &readfds);
        FD_SET(user.fd, &readfds);

        int nfds = select(max_fd + 1, &readfds, NULL, NULL, NULL);
        if (nfds < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }

        // 键盘输入
        if (FD_ISSET(STDIN_FILENO, &readfds)) {
            if (fgets(buf, sizeof(buf), stdin) == NULL) {
                break;  // Ctrl+D 退出
            }

            // 检查是否遇到结束标志（去除换行符后比较）
            buf[strcspn(buf, "\n")] = '\0';  // 去掉换行符
            if (strcmp(buf, SEND_FLAG) == 0) {
                // 触发发送
                msg_buf[msg_len] = '\0';
                if (msg_len > 0) {
                    Message m = {0};
strcpy(m.type, "msg");
                    /* 私聊：以 "to.名字 内容" 开头（和开发板客户端同一套规则） */
                    if (strncmp(msg_buf, "to.", 3) == 0) {
                        char *sp = strchr(msg_buf + 3, ' ');
                        if (sp != NULL && sp > msg_buf + 3) {
                            *sp = '\0';
                            strncpy(m.to, msg_buf + 3, NAME_LEN - 1);
                            strncpy(m.content, sp + 1, sizeof(m.content) - 1);
                        } else {
                            strncpy(m.content, msg_buf, sizeof(m.content) - 1);
                        }
                    } else {
                        strncpy(m.content, msg_buf, sizeof(m.content) - 1);  // 换行会被 cJSON 自动转义
                    }

char *out = message_serialize(&m);
                    if (send_packet(user.fd, out, strlen(out))< 0) {
                        perror("send 消息失败");
                        break;
                    }
                    msg_len = 0;  // 清空缓冲区
                    printf("消息已发送。继续输入下一条（结束行 //SEND）：\n");
                } else {
                    printf("没有输入内容，忽略。\n");
                }
                continue;  // 结束标志行不加入消息
            }

            // 非结束标志行：加入累积缓冲区（恢复换行符，因为刚才去掉了）
            int line_len = strlen(buf);
            if (msg_len + line_len + 1 < MAX_MSG_SIZE) {
                // 手动添加换行符（原来 fgets 会读入换行，但我们去掉了，需要补回）
                strcpy(msg_buf + msg_len, buf);
                msg_len += line_len;
                msg_buf[msg_len] = '\n';
                msg_len++;
            } else {
                printf("消息过长，已截断\n");
            }
        }

        // 服务器消息
    if (FD_ISSET(user.fd, &readfds)) {
    int data_len = recv_packet(user.fd, buf, sizeof(buf));
    if (data_len < 0) {
        printf("\n[服务器已关闭连接或网络错误]\n");
        break;
    }
    Message m;
    if (message_parse(buf, &m) == 0) {
        if (strcmp(m.type, "join") == 0)
            printf("*** %s 加入了聊天室 ***\n", m.name);
        else if (strcmp(m.type, "leave") == 0)
            printf("*** %s 离开了聊天室 ***\n", m.name);
        else if (strcmp(m.type, "msg") == 0) {
            if (m.to[0] != '\0')
                printf("[%s] 私聊你: %s\n", m.name, m.content);
            else
                printf("[%s]: %s\n", m.name, m.content);
        }
    }
}

    }

    close(user.fd);
    return 0;
}