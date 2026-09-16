/*
 * High-concurrency connection test client.
 *
 * This client is intentionally separate from the interactive client. It uses
 * one epoll loop instead of one thread per connection, so the VM can exercise
 * the server's connection capacity without the test client becoming the
 * bottleneck.
 */

#define _POSIX_C_SOURCE 200112L

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "protocol.h"

#define DEFAULT_PORT 8888
#define DEFAULT_DURATION 10
#define MAX_EVENTS 1024
#define MAX_CONNECTIONS 100000
#define FRAME_HEADER_SIZE 4
#define RX_BUFFER_SIZE 65536

enum client_state {
    CLIENT_UNUSED = 0,
    CLIENT_CONNECTING,
    CLIENT_SENDING_JOIN,
    CLIENT_READY,
    CLIENT_CLOSED
};

struct load_client {
    int fd;
    enum client_state state;
    size_t tx_offset;
    size_t tx_length;
    unsigned char tx[FRAME_HEADER_SIZE + 1024];
    unsigned char rx[RX_BUFFER_SIZE];
    size_t rx_length;
    double next_message_at;
};

struct counters {
    unsigned long long attempted;
    unsigned long long connected;
    unsigned long long active;
    unsigned long long peak_active;
    unsigned long long join_sent;
    unsigned long long messages_sent;
    unsigned long long packets_received;
    unsigned long long messages_received;
    unsigned long long bytes_received;
    unsigned long long failed;
    unsigned long long closed;
    double first_connected_at;
    double last_connected_at;
};

static void usage(const char *program) {
    printf("用法: %s [选项]\n"
           "\n"
           "选项:\n"
           "  -h <host>       服务器地址，默认 127.0.0.1\n"
           "  -p <port>       服务器端口，默认 8888\n"
           "  -c <count>      并发连接数，默认 100\n"
           "  -d <seconds>    保持连接时间，默认 10\n"
           "  -r <rate>       建连速率（连接/秒），默认 0（立即发起）\n"
           "  -m <rate>       每个连接发送消息数/秒，默认 0（只测连接）\n"
           "  --help          显示帮助\n"
           "\n"
           "示例:\n"
           "  %s -h 192.168.56.101 -p 8888 -c 1000 -d 30\n"
           "  %s -c 5000 -r 200 -d 60\n",
           program, program, program);
}

static int parse_positive(const char *value, unsigned long *result) {
    char *end = NULL;
    unsigned long parsed;

    errno = 0;
    parsed = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed == 0)
        return -1;
    *result = parsed;
    return 0;
}

static double now_seconds(void) {
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
        return 0.0;
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
}

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
        return -1;
    return 0;
}

static int update_events(int epoll_fd, struct load_client *client) {
    struct epoll_event event;

    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN | EPOLLERR | EPOLLHUP;
    if (client->state == CLIENT_CONNECTING ||
        client->state == CLIENT_SENDING_JOIN ||
        client->tx_offset < client->tx_length)
        event.events |= EPOLLOUT;
    event.data.ptr = client;
    return epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client->fd, &event);
}

static void close_client(int epoll_fd, struct load_client *client,
                         struct counters *counters) {
    if (client->state == CLIENT_CLOSED || client->state == CLIENT_UNUSED)
        return;
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client->fd, NULL);
    close(client->fd);
    if (client->state == CLIENT_SENDING_JOIN ||
        client->state == CLIENT_READY) {
        if (counters->active > 0)
            counters->active--;
        counters->closed++;
    } else if (client->state == CLIENT_CONNECTING) {
        counters->closed++;
    }
    client->fd = -1;
    client->state = CLIENT_CLOSED;
}

static int prepare_join(struct load_client *client, unsigned long id) {
    Message message;
    char name[NAME_LEN];
    char *json;
    uint32_t network_length;
    size_t json_length;

    memset(&message, 0, sizeof(message));
    snprintf(name, sizeof(name), "load-%lu", id);
    strncpy(message.type, "join", sizeof(message.type) - 1);
    strncpy(message.name, name, sizeof(message.name) - 1);

    json = message_serialize(&message);
    if (json == NULL)
        return -1;
    json_length = strlen(json);
    if (json_length > sizeof(client->tx) - FRAME_HEADER_SIZE) {
        free(json);
        return -1;
    }

    network_length = htonl((uint32_t)json_length);
    memcpy(client->tx, &network_length, FRAME_HEADER_SIZE);
    memcpy(client->tx + FRAME_HEADER_SIZE, json, json_length);
    client->tx_length = FRAME_HEADER_SIZE + json_length;
    client->tx_offset = 0;
    free(json);
    return 0;
}

static int prepare_message(struct load_client *client, unsigned long id,
                           unsigned long sequence) {
    Message message;
    char *json;
    uint32_t network_length;
    size_t json_length;

    memset(&message, 0, sizeof(message));
    strncpy(message.type, "msg", sizeof(message.type) - 1);
    snprintf(message.content, sizeof(message.content), "load message %lu-%lu",
             id, sequence);
    json = message_serialize(&message);
    if (json == NULL)
        return -1;
    json_length = strlen(json);
    if (json_length > sizeof(client->tx) - FRAME_HEADER_SIZE) {
        free(json);
        return -1;
    }
    network_length = htonl((uint32_t)json_length);
    memcpy(client->tx, &network_length, FRAME_HEADER_SIZE);
    memcpy(client->tx + FRAME_HEADER_SIZE, json, json_length);
    client->tx_length = FRAME_HEADER_SIZE + json_length;
    client->tx_offset = 0;
    free(json);
    return 0;
}

static void mark_connected(struct load_client *client, struct counters *counters,
                           double connected_at) {
    client->state = CLIENT_SENDING_JOIN;
    counters->connected++;
    counters->active++;
    if (counters->active > counters->peak_active)
        counters->peak_active = counters->active;
    if (counters->connected == 1)
        counters->first_connected_at = connected_at;
    counters->last_connected_at = connected_at;
}

static int finish_connect(struct load_client *client,
                          struct counters *counters, double connected_at) {
    int error = 0;
    socklen_t error_length = sizeof(error);

    if (getsockopt(client->fd, SOL_SOCKET, SO_ERROR, &error, &error_length) < 0)
        return -1;
    if (error != 0) {
        errno = error;
        return -1;
    }
    mark_connected(client, counters, connected_at);
    return 0;
}

static void send_join(int epoll_fd, struct load_client *client,
                      struct counters *counters) {
    while (client->tx_offset < client->tx_length) {
        ssize_t sent = send(client->fd, client->tx + client->tx_offset,
                            client->tx_length - client->tx_offset,
                            MSG_NOSIGNAL);
        if (sent > 0) {
            client->tx_offset += (size_t)sent;
            continue;
        }

        if (sent < 0 && (errno == EINTR))
            continue;
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            return;
        return;
    }

    if (client->state == CLIENT_SENDING_JOIN) {
        client->state = CLIENT_READY;
        counters->join_sent++;
        if (update_events(epoll_fd, client) < 0)
            perror("epoll_ctl");
    }
}

static void send_message(int epoll_fd, struct load_client *client,
                         struct counters *counters) {
    while (client->tx_offset < client->tx_length) {
        ssize_t sent = send(client->fd, client->tx + client->tx_offset,
                            client->tx_length - client->tx_offset,
                            MSG_NOSIGNAL);
        if (sent > 0) {
            client->tx_offset += (size_t)sent;
            continue;
        }
        if (sent < 0 && errno == EINTR)
            continue;
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            update_events(epoll_fd, client);
            return;
        }
        return;
    }
    counters->messages_sent++;
    update_events(epoll_fd, client);
}

static void receive_data(int epoll_fd, struct load_client *client,
                         struct counters *counters) {
    unsigned char buffer[4096];

    for (;;) {
        ssize_t received = recv(client->fd, buffer, sizeof(buffer), 0);
        if (received > 0) {
            counters->bytes_received += (unsigned long long)received;
            if (client->rx_length + (size_t)received > sizeof(client->rx)) {
                close_client(epoll_fd, client, counters);
                return;
            }
            memcpy(client->rx + client->rx_length, buffer, (size_t)received);
            client->rx_length += (size_t)received;

            while (client->rx_length >= FRAME_HEADER_SIZE) {
                uint32_t network_length;
                size_t packet_length;
                memcpy(&network_length, client->rx, FRAME_HEADER_SIZE);
                packet_length = (size_t)ntohl(network_length);
                if (packet_length > sizeof(client->rx) - FRAME_HEADER_SIZE) {
                    close_client(epoll_fd, client, counters);
                    return;
                }
                if (client->rx_length < FRAME_HEADER_SIZE + packet_length)
                    break;
                {
                    char packet[1025];
                    Message message;
                    if (packet_length < sizeof(packet)) {
                        memcpy(packet, client->rx + FRAME_HEADER_SIZE,
                               packet_length);
                        packet[packet_length] = '\0';
                    }
                    if (packet_length < sizeof(packet) &&
                        message_parse(packet, &message) == 0) {
                        if (strcmp(message.type, "msg") == 0)
                            counters->messages_received++;
                    }
                    counters->packets_received++;
                }
                memmove(client->rx,
                        client->rx + FRAME_HEADER_SIZE + packet_length,
                        client->rx_length - FRAME_HEADER_SIZE - packet_length);
                client->rx_length -= FRAME_HEADER_SIZE + packet_length;
            }
            continue;
        }
        if (received == 0) {
            close_client(epoll_fd, client, counters);
            return;
        }
        if (errno == EINTR)
            continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return;
        close_client(epoll_fd, client, counters);
        return;
    }
}

static int create_connection(int epoll_fd, const struct sockaddr *address,
                             socklen_t address_length, unsigned long id,
                             struct load_client *client,
                             struct counters *counters) {
    struct epoll_event event;
    int fd;
    int result;

    fd = socket(address->sa_family, SOCK_STREAM, 0);
    if (fd < 0 || set_nonblocking(fd) < 0) {
        if (fd >= 0)
            close(fd);
        counters->failed++;
        return -1;
    }
    if (prepare_join(client, id) < 0) {
        close(fd);
        counters->failed++;
        return -1;
    }

    client->fd = fd;
    client->state = CLIENT_CONNECTING;
    client->rx_length = 0;
    result = connect(fd, address, address_length);
    if (result == 0) {
        mark_connected(client, counters, now_seconds());
    } else if (errno != EINPROGRESS) {
        close(fd);
        client->fd = -1;
        client->state = CLIENT_CLOSED;
        counters->failed++;
        return -1;
    }

    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP;
    event.data.ptr = client;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) < 0) {
        close(fd);
        client->fd = -1;
        client->state = CLIENT_CLOSED;
        counters->failed++;
        return -1;
    }
    counters->attempted++;
    if (result == 0)
        send_join(epoll_fd, client, counters);
    return 0;
}

static void print_summary(const struct counters *counters, double elapsed) {
    double connection_window = counters->connected > 0
                                    ? counters->last_connected_at -
                                          counters->first_connected_at
                                    : 0.0;
    double connection_rate = connection_window > 0.0
                                 ? (double)counters->connected /
                                       connection_window
                                 : (counters->connected > 0 ? 0.0 : 0.0);
    double success_rate = counters->attempted > 0
                              ? 100.0 * (double)counters->connected /
                                    (double)counters->attempted
                              : 0.0;

    printf("\n压测结束（%.2f 秒，客户端将在此时统一关闭连接）\n"
           "  发起连接: %llu\n"
           "  TCP 成功: %llu (%.2f%%)\n"
           "  峰值并发: %llu\n"
           "  实际建连窗口: %.3f 秒 (%.1f 连接/秒)\n"
           "  join 已发送: %llu\n"
           "  压测消息发送: %llu\n"
           "  收到广播消息: %llu\n"
           "  收到协议包: %llu\n"
           "  收到字节: %llu\n"
           "  建连失败: %llu\n"
           "  已关闭: %llu\n",
           elapsed, counters->attempted, counters->connected, success_rate,
           counters->peak_active, connection_window, connection_rate,
           counters->join_sent, counters->messages_sent,
           counters->messages_received, counters->packets_received,
           counters->bytes_received, counters->failed, counters->closed);
}

int main(int argc, char **argv) {
    const char *host = "127.0.0.1";
    unsigned long port = DEFAULT_PORT;
    unsigned long connection_count = 100;
    unsigned long duration = DEFAULT_DURATION;
    unsigned long rate = 0;
    unsigned long message_rate = 0;
    struct addrinfo hints;
    struct addrinfo *address = NULL;
    struct load_client *clients = NULL;
    struct counters counters;
    int epoll_fd = -1;
    unsigned long created = 0;
    double start;
    int option;

    if (argc == 2 && strcmp(argv[1], "--help") == 0) {
        usage(argv[0]);
        return EXIT_SUCCESS;
    }

    memset(&counters, 0, sizeof(counters));
    while ((option = getopt(argc, argv, "h:p:c:d:r:m:")) != -1) {
        unsigned long value;
        switch (option) {
        case 'h':
            host = optarg;
            break;
        case 'p':
            if (parse_positive(optarg, &port) < 0 || port > 65535) {
                fprintf(stderr, "无效端口: %s\n", optarg);
                return EXIT_FAILURE;
            }
            break;
        case 'c':
            if (parse_positive(optarg, &connection_count) < 0 ||
                connection_count > MAX_CONNECTIONS) {
                fprintf(stderr, "连接数应为 1-%d\n", MAX_CONNECTIONS);
                return EXIT_FAILURE;
            }
            break;
        case 'd':
            if (parse_positive(optarg, &duration) < 0) {
                fprintf(stderr, "无效持续时间: %s\n", optarg);
                return EXIT_FAILURE;
            }
            break;
        case 'r':
            if (parse_positive(optarg, &value) < 0) {
                fprintf(stderr, "无效建连速率: %s\n", optarg);
                return EXIT_FAILURE;
            }
            rate = value;
            break;
        case 'm':
            if (parse_positive(optarg, &message_rate) < 0) {
                fprintf(stderr, "无效消息速率: %s\n", optarg);
                return EXIT_FAILURE;
            }
            break;
        default:
            usage(argv[0]);
            return option == 'h' ? EXIT_FAILURE : EXIT_SUCCESS;
        }
    }

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    {
        char port_text[16];
        snprintf(port_text, sizeof(port_text), "%lu", port);
        if (getaddrinfo(host, port_text, &hints, &address) != 0) {
            fprintf(stderr, "无法解析服务器地址: %s\n", host);
            return EXIT_FAILURE;
        }
    }

    clients = calloc(connection_count, sizeof(*clients));
    if (clients == NULL) {
        perror("calloc");
        freeaddrinfo(address);
        return EXIT_FAILURE;
    }
    for (unsigned long i = 0; i < connection_count; i++) {
        clients[i].fd = -1;
        clients[i].state = CLIENT_UNUSED;
    }

    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("epoll_create1");
        free(clients);
        freeaddrinfo(address);
        return EXIT_FAILURE;
    }

    signal(SIGPIPE, SIG_IGN);
    start = now_seconds();
    printf("开始压测 %s:%lu，并发 %lu，持续 %lu 秒%s%s\n", host, port,
           connection_count, duration,
           rate == 0 ? "" : "（按速率逐步建立）",
           message_rate == 0 ? "" : "（每连接发送消息）");

    while (now_seconds() - start < (double)duration) {
        double elapsed = now_seconds() - start;
        unsigned long target = connection_count;
        struct epoll_event events[MAX_EVENTS];
        int event_count;

        if (rate > 0) {
            double scheduled = elapsed * (double)rate;
            target = scheduled >= (double)connection_count
                         ? connection_count
                         : (unsigned long)scheduled;
        }
        while (created < target) {
            clients[created].state = CLIENT_UNUSED;
            create_connection(epoll_fd, address->ai_addr, address->ai_addrlen,
                              created + 1, &clients[created], &counters);
            created++;
        }

        event_count = epoll_wait(epoll_fd, events, MAX_EVENTS, 100);
        if (event_count < 0) {
            if (errno == EINTR)
                continue;
            perror("epoll_wait");
            break;
        }
        for (int i = 0; i < event_count; i++) {
            struct load_client *client = events[i].data.ptr;
            uint32_t event_flags = events[i].events;

            if (client->state == CLIENT_CLOSED ||
                client->state == CLIENT_UNUSED)
                continue;
            if (client->state == CLIENT_CONNECTING &&
                (event_flags & (EPOLLOUT | EPOLLERR | EPOLLHUP))) {
                if (finish_connect(client, &counters, now_seconds()) < 0) {
                    counters.failed++;
                    close_client(epoll_fd, client, &counters);
                    continue;
                }
            }
            if (client->state == CLIENT_SENDING_JOIN &&
                (event_flags & EPOLLOUT))
                send_join(epoll_fd, client, &counters);
            if (client->state == CLIENT_READY &&
                (event_flags & EPOLLOUT))
                send_message(epoll_fd, client, &counters);
            if (client->state != CLIENT_CLOSED &&
                (event_flags & (EPOLLIN | EPOLLERR | EPOLLHUP)))
                receive_data(epoll_fd, client, &counters);
        }
        if (message_rate > 0) {
            for (unsigned long i = 0; i < created; i++) {
                unsigned long sequence;
                double interval = 1.0 / (double)message_rate;
                if (clients[i].state != CLIENT_READY ||
                    clients[i].tx_offset < clients[i].tx_length ||
                    now_seconds() < clients[i].next_message_at)
                    continue;
                sequence = counters.messages_sent + 1;
                if (prepare_message(&clients[i], i + 1, sequence) == 0) {
                    clients[i].next_message_at = now_seconds() + interval;
                    send_message(epoll_fd, &clients[i], &counters);
                }
            }
        }
    }

    for (unsigned long i = 0; i < created; i++)
        close_client(epoll_fd, &clients[i], &counters);
    print_summary(&counters, now_seconds() - start);

    close(epoll_fd);
    free(clients);
    freeaddrinfo(address);
    return EXIT_SUCCESS;
}
