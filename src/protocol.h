#ifndef PROTOCOL_H
#define PROTOCOL_H

#define TYPE_LEN    16
#define NAME_LEN    64
#define CONTENT_LEN 512

typedef struct {
    char type[TYPE_LEN];        // "join" / "msg" / "leave"
    char name[NAME_LEN];        // 昵称（发送者）
    char to[NAME_LEN];          // 私聊目标昵称（空 = 群发所有人）
    char content[CONTENT_LEN];  // 消息正文
    long time;                    // 时间戳（epoch 秒），0 表示未设置
} Message;

// JSON 字符串 -> Message（反序列化），成功返回 0，失败返回 -1
int  message_parse(const char *json, Message *out);

// Message -> JSON 字符串（序列化），返回 malloc 出来的字符串，调用者负责 free
char *message_serialize(const Message *m);

#endif
