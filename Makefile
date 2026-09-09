CC        = gcc
ARM_CC    = arm-linux-gcc 
CFLAGS    = -Wall -Wextra -g
LDFLAGS   = -pthread         

SERVER    = server 
CLIENT    = client
ARM_CLIENT = client_arm

COMMON = src/cJSON.c src/protocol.c

all: $(SERVER) $(CLIENT) $(ARM_CLIENT)

$(SERVER): src/server.c src/thread_pool.c $(COMMON)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $(SERVER) src/server.c src/thread_pool.c $(COMMON)

$(CLIENT): src/client.c $(COMMON)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $(CLIENT) src/client.c $(COMMON)

client_arm: src/client.c $(COMMON)
	$(ARM_CC) $(CFLAGS) $(LDFLAGS) -o $(ARM_CLIENT) src/client.c $(COMMON)

# ---- LVGL 图形界面客户端（跑在开发板上）----
# 注意：LVGL_SRCS 用 find 收集 lvgl/src 下所有 .c，需要在 Linux 下执行 make
LVGL_DIR  = lvgl
LVGL_SRCS = $(shell find $(LVGL_DIR)/src -name '*.c')
LVGL_INC  = -I$(LVGL_DIR)

GUI_CLIENT     = gui_client
GUI_CLIENT_ARM = gui_client_arm
GUI_APP        = src/gui_client.c
GUI_FONT       = lv_font_chinese_16.c

# 本地编译（可在 Linux 桌面用 framebuffer 调试）
gui_client: $(GUI_APP) $(GUI_FONT) $(COMMON)
	$(CC) $(CFLAGS) $(LVGL_INC) $(LDFLAGS) -lm -o $(GUI_CLIENT) $(GUI_APP) $(GUI_FONT) $(COMMON) $(LVGL_SRCS)

# 交叉编译到 ARM 开发板
gui_client_arm: $(GUI_APP) $(GUI_FONT) $(COMMON)
	$(ARM_CC) $(CFLAGS) $(LVGL_INC) $(LDFLAGS) -lm -o $(GUI_CLIENT_ARM) $(GUI_APP) $(GUI_FONT) $(COMMON) $(LVGL_SRCS)

clean:
	rm -f $(SERVER) $(CLIENT) $(ARM_CLIENT) $(GUI_CLIENT) $(GUI_CLIENT_ARM)

.PHONY: all clean client_arm gui_client gui_client_arm