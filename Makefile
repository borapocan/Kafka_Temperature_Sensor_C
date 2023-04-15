EXAMPLES ?= rdkafka_example rdkafka_performance  \
	rdkafka_complex_consumer_example  \
	producer temperature_sensor_producer \
	consumer temperature_sensor_consumer \
	idempotent_producer transactions \
	delete_records \
	list_consumer_groups \
	describe_consumer_groups \
	list_consumer_group_offsets \
	alter_consumer_group_offsets \
	misc

all: $(EXAMPLES)

include ../mklove/Makefile.base

CFLAGS += -I../src
CXXFLAGS += -I../src-cpp

# librdkafka must be compiled with -gstrict-dwarf, but rdkafka_example must not,
# due to some clang bug on OSX 10.9
CPPFLAGS := $(subst strict-dwarf,,$(CPPFLAGS))

rdkafka_example: ../src/librdkafka.a rdkafka_example.c
	$(CC) $(CPPFLAGS) $(CFLAGS) rdkafka_example.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)
	@echo "# $@ is ready"
	@echo "#"
	@echo "# Run producer (write messages on stdin)"
	@echo "./$@ -P -t <topic> -p <partition>"
	@echo ""
	@echo "# or consumer"
	@echo "./$@ -C -t <topic> -p <partition>"
	@echo ""
	@echo "#"
	@echo "# More usage options:"
	@echo "./$@ -h"

producer: ../src/librdkafka.a producer.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $@.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)

temperature_sensor_producer: ../src/librdkafka.a temperature_sensor_producer.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $@.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)

consumer: ../src/librdkafka.a consumer.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $@.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)

temperature_sensor_consumer: ../src/librdkafka.a temperature_sensor_consumer.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $@.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)

idempotent_producer: ../src/librdkafka.a idempotent_producer.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $@.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)

transactions: ../src/librdkafka.a transactions.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $@.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)

transactions-older-broker.c: ../src/librdkafka.a transactions-older-broker.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $@.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)

delete_records: ../src/librdkafka.a delete_records.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $@.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)

list_consumer_groups: ../src/librdkafka.a list_consumer_groups.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $@.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)

describe_consumer_groups: ../src/librdkafka.a describe_consumer_groups.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $@.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)

list_consumer_group_offsets: ../src/librdkafka.a list_consumer_group_offsets.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $@.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)

alter_consumer_group_offsets: ../src/librdkafka.a alter_consumer_group_offsets.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $@.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)

rdkafka_complex_consumer_example: ../src/librdkafka.a rdkafka_complex_consumer_example.c
	$(CC) $(CPPFLAGS) $(CFLAGS) rdkafka_complex_consumer_example.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)
	@echo "# $@ is ready"
	@echo "#"
	@echo "./$@ <topic[:part]> <topic2[:part]> .."
	@echo ""
	@echo "#"
	@echo "# More usage options:"
	@echo "./$@ -h"

rdkafka_performance: ../src/librdkafka.a rdkafka_performance.c
	$(CC) $(CPPFLAGS) $(CFLAGS) rdkafka_performance.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)
	@echo "# $@ is ready"
	@echo "#"
	@echo "# Run producer"
	@echo "./$@ -P -t <topic> -p <partition> -s <msgsize>"
	@echo ""
	@echo "# or consumer"
	@echo "./$@ -C -t <topic> -p <partition>"
	@echo ""
	@echo "#"
	@echo "# More usage options:"
	@echo "./$@ -h"

misc: ../src/librdkafka.a misc.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $@.c -o $@ $(LDFLAGS) \
		../src/librdkafka.a $(LIBS)

clean:
	rm -f $(EXAMPLES)

