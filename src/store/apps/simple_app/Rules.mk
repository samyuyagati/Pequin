d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), simple_app.cc)

OBJS-all-store-clients := $(OBJS-strong-client) $(OBJS-weak-client) \
		$(LIB-tapir-client)  \
		$(LIB-indicus-client) 

LIB-simple-app := $(o)simple_app.o 

OBJS-simple-app := $(LIB-simple-app) 

$(d)simpleapp: $(LIB-key-selector) $(LIB-latency) $(LIB-tcptransport) $(LIB-udptransport) $(OBJS-all-store-clients) $(OBJS-simple-app) $(LIB-store-common)

BINS +=  $(d)simpleapp
