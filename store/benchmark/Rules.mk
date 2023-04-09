d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), benchClient.cc)

OBJS-all-clients := $(OBJS-tapir-client)

$(d)benchClient: $(OBJS-all-clients) $(o)benchClient.o

BINS += $(d)benchClient
