#include "protocol.h"
#include "cJSON.h"
#include <string.h>
#include <stdlib.h>

// 反序列化：JSON 字符串 -> Message
int message_parse(const char *json, Message *out) {
    cJSON *root = cJSON_Parse(json);
    if (root == NULL) {
        // 非法 JSON。可选：用 cJSON_GetErrorPtr() 打印出错位置
        return -1;
    }

cJSON *t = cJSON_GetObjectItem(root, "type");
cJSON *n = cJSON_GetObjectItem(root, "name");
cJSON *to_item = cJSON_GetObjectItem(root, "to");
cJSON *c = cJSON_GetObjectItem(root, "content");
cJSON *tm = cJSON_GetObjectItem(root, "time");

out->time = cJSON_IsNumber(tm) ? (long)tm->valuedouble : 0;   // 缺省 0

if (!cJSON_IsString(t)) { cJSON_Delete(root); return -1; }   // type 必填

strncpy(out->type, t->valuestring, sizeof(out->type) - 1);
out->type[sizeof(out->type) - 1] = '\0';

strncpy(out->name, cJSON_IsString(n) ? n->valuestring : "", sizeof(out->name) - 1);
out->name[sizeof(out->name) - 1] = '\0';

strncpy(out->to, cJSON_IsString(to_item) ? to_item->valuestring : "", sizeof(out->to) - 1);
out->to[sizeof(out->to) - 1] = '\0';

strncpy(out->content, cJSON_IsString(c) ? c->valuestring : "", sizeof(out->content) - 1);
out->content[sizeof(out->content) - 1] = '\0';

cJSON_Delete(root);
return 0;

}

// 序列化：Message -> JSON 字符串（紧凑单行）
char *message_serialize(const Message *m) {
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "type",    m->type);
    cJSON_AddStringToObject(root, "name",    m->name);
    cJSON_AddStringToObject(root, "to",      m->to);
    cJSON_AddStringToObject(root, "content", m->content);
    cJSON_AddNumberToObject(root, "time", (double)m->time);


    char *s = cJSON_PrintUnformatted(root);  // 紧凑单行，malloc 出来
    cJSON_Delete(root);
    return s;                                 // 调用者 free(s)
}
