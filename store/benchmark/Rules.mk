d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), benchClient.cc, retwisClient.cc)

OBJS-all-clients := $(OBJS-tapir-client)

$(d)benchClient: $(OBJS-all-clients) $(o)benchClient.o

$(d)benchClient: $(OBJS-all-clients) $(o)retwisClient.o

BINS += $(d)benchClient $(d)retwisClient
