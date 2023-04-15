/*
 *
 * Author: Merih Bora Poçan
 * Mail: mborapocan@gmail.com
 * Github: @borapocan <git@borapocan.com>
 * Creation Date: 12-04-2023
 * Last Modified: Sat Apr 15 17:06:38 2023
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <ctype.h>
#include <librdkafka/rdkafka.h>
//#include "rdkafka.h"

#define RED "\x1B[31m"
#define GREEN "\x1B[32m"
#define RESET "\x1b[0m"

static volatile sig_atomic_t run = 1;
static void stop(int sig) {
        run = 0;
}

static int is_printable(const char *buf, size_t size) {
        size_t i;
        for (i = 0; i < size; i++)
                if (!isprint((int)buf[i]))
                        return 0;
        return 1;
}

int main(int argc, char **argv) {
        rd_kafka_t *rk;
        rd_kafka_conf_t *conf;
        rd_kafka_resp_err_t err;
        char errstr[512];
        const char *brokers = "192.168.212.40:9092";
        const char *groupid = "temperature_sensor_consumer_a";
        char *topics[] = { "temperature_sensor" };
        int topic_cnt = 1;
        rd_kafka_topic_partition_list_t *subscription;
        int temp, i;
        conf = rd_kafka_conf_new();

        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        if (rd_kafka_conf_set(conf, "group.id", groupid, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        if (rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr,
			      sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return 1;
        }
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "%% Failed to create new consumer: %s\n",
                        errstr);
                return 1;
        }
        conf = NULL;
        rd_kafka_poll_set_consumer(rk);
        subscription = rd_kafka_topic_partition_list_new(topic_cnt);
        for (i = 0; i < topic_cnt; i++)
                rd_kafka_topic_partition_list_add(subscription, topics[i],
                                                  RD_KAFKA_PARTITION_UA);

        err = rd_kafka_subscribe(rk, subscription);
        if (err) {
                fprintf(stderr, "%% Failed to subscribe to %d topics: %s\n",
                        subscription->cnt, rd_kafka_err2str(err));
                rd_kafka_topic_partition_list_destroy(subscription);
                rd_kafka_destroy(rk);
                return 1;
        }

        fprintf(stderr,
                "%% Subscribed to %d topic(s), "
                "waiting for rebalance and messages...\n",
                subscription->cnt);

        rd_kafka_topic_partition_list_destroy(subscription);
        signal(SIGINT, stop);

        while (run) {
                rd_kafka_message_t *rkm;

                rkm = rd_kafka_consumer_poll(rk, 100);
                if (!rkm)
                        continue;

                if (rkm->err) {
                        fprintf(stderr, "%% Consumer error: %s\n",
                                rd_kafka_message_errstr(rkm));
                        rd_kafka_message_destroy(rkm);
                        continue;
                }

                printf("\nMessage on %s [%" PRId32 "] at offset %" PRId64
                       " (leader epoch %" PRId32 "):\n",
                       rd_kafka_topic_name(rkm->rkt), rkm->partition,
                       rkm->offset, rd_kafka_message_leader_epoch(rkm));

		temp = atoi(rkm->payload);
		FILE *cmd = popen("pidof temperature_sensor_producer", "r");
		if (temp >= 120 && cmd) {
			char pid[128], kill_pid[256];
			fgets(pid, 128, cmd);
			char *p = strchr(pid, '\n'); if (p) *p = '\0';
			puts("[!] SENSOR OVERHEATS [!]\nStopping producer.");
			pclose(cmd);
			snprintf(kill_pid, sizeof(kill_pid), "kill %s 2>/dev/null", pid);
			system(kill_pid);
		}

                if (rkm->key && is_printable(rkm->key, rkm->key_len))
                        printf(" Key: %.*s\n", (int)rkm->key_len,
                               (const char *)rkm->key);
                else if (rkm->key)
                        printf(" Key: (%d bytes)\n", (int)rkm->key_len);

                if (rkm->payload && is_printable(rkm->payload, rkm->len))
			printf("[!] TEMPERATURE: %.*s\n", (int)rkm->len, (const char *)rkm->payload);
                else if (rkm->payload)
                        printf(" Value: (%d bytes)\n", (int)rkm->len);

                rd_kafka_message_destroy(rkm);
        }

        fprintf(stderr, "%% Closing consumer\n");
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);
        return 0;
}
