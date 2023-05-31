d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), benchClient.cc retwisClient.cc)

OBJS-all-clients := $(OBJS-tapir-client)

$(d)benchClient: $(OBJS-all-clients) $(o)benchClient.o

$(d)retwisClient: $(OBJS-all-clients) $(o)retwisClient.o

BINS += $(d)retwisClient
