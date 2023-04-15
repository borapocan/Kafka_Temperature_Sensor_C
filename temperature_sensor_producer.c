#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <librdkafka/rdkafka.h>

#define RED "\x1B[31m"
#define GREEN "\x1B[32m"
#define RESET "\x1b[0m"

static volatile sig_atomic_t run = 1;

static void stop(int sig) {
        run = 0;
        fclose(stdin); /* abort fgets() */
}

static void
dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
        if (rkmessage->err)
                fprintf(stderr, "%% Message delivery failed: %s\n",
                        rd_kafka_err2str(rkmessage->err));
        else
                fprintf(stderr,
                        "%% Message delivered (%zd bytes, "
                        "partition %" PRId32 ")\n",
                        rkmessage->len, rkmessage->partition);
}

int random(int range){
    int num;
    num = rand() % range;
    return num;
}

int main(int argc, char **argv) {
        rd_kafka_t *rk;        /* Producer instance handle */
        rd_kafka_conf_t *conf; /* Temporary configuration object */
        char errstr[512];      /* librdkafka API error reporting buffer */
        char buf[512];         /* Message value temporary buffer */
        const char *brokers = "192.168.212.40:9092";   /* Argument: broker list */
        const char *topic = "temperature_sensor";     /* Argument: topic to produce to */

        conf = rd_kafka_conf_new();

        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                return 1;
        }

        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "%% Failed to create new producer: %s\n",
                        errstr);
                return 1;
        }

        signal(SIGINT, stop);

        fprintf(stderr,
                "%% Wait for random temperature value\n"
                "%% Press Ctrl-C or Ctrl-D to exit\n");

	time_t t;
	/* Intializes random number generator */
	srand((unsigned) time(&t));
	int temperature;

        while (run) {
		temperature = random(130);
		sprintf(buf, "%d", temperature);
		printf(GREEN "\nTEMPERATURE: %d\n" RESET, temperature);
		sleep(3);
                size_t len = strlen(buf);
                rd_kafka_resp_err_t err;

                if (buf[len - 1] == '\n')
                        buf[--len] = '\0';

                if (len == 0) {
                        rd_kafka_poll(rk, 0);
                        continue;
                }

        retry:
                err = rd_kafka_producev(
                    /* Producer handle */
                    rk,
                    /* Topic name */
                    RD_KAFKA_V_TOPIC(topic),
                    /* Make a copy of the payload. */
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    /* Message value and length */
                    RD_KAFKA_V_VALUE(buf, len),
                    /* Per-Message opaque, provided in
                     * delivery report callback as
                     * msg_opaque. */
                    RD_KAFKA_V_OPAQUE(NULL),
                    /* End sentinel */
                    RD_KAFKA_V_END);

                if (err) {
                        /*
                         * Failed to *enqueue* message for producing.
                         */
                        fprintf(stderr,
                                "%% Failed to produce to topic %s: %s\n", topic,
                                rd_kafka_err2str(err));

                        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                                /* If the internal queue is full, wait for
                                 * messages to be delivered and then retry.
                                 * The internal queue represents both
                                 * messages to be sent and messages that have
                                 * been sent or failed, awaiting their
                                 * delivery report callback to be called.
                                 *
                                 * The internal queue is limited by the
                                 * configuration property
                                 * queue.buffering.max.messages and
                                 * queue.buffering.max.kbytes */
                                rd_kafka_poll(rk,
                                              1000 /*block for max 1000ms*/);
                                goto retry;
                        }
                } else {
                        fprintf(stderr,
                                "%% Enqueued message (%zd bytes) "
                                "for topic %s\n",
                                len, topic);
                }


                /* A producer application should continually serve
                 * the delivery report queue by calling rd_kafka_poll()
                 * at frequent intervals.
                 * Either put the poll call in your main loop, or in a
                 * dedicated thread, or call it after every
                 * rd_kafka_produce() call.
                 * Just make sure that rd_kafka_poll() is still called
                 * during periods where you are not producing any messages
                 * to make sure previously produced messages have their
                 * delivery report callback served (and any other callbacks
                 * you register). */
                rd_kafka_poll(rk, 0 /*non-blocking*/);
        }


        /* Wait for final messages to be delivered or fail.
         * rd_kafka_flush() is an abstraction over rd_kafka_poll() which
         * waits for all messages to be delivered. */
        fprintf(stderr, "%% Flushing final messages..\n");
        rd_kafka_flush(rk, 10 * 1000 /* wait for max 10 seconds */);

        /* If the output queue is still not empty there is an issue
         * with producing messages to the clusters. */
        if (rd_kafka_outq_len(rk) > 0)
                fprintf(stderr, "%% %d message(s) were not delivered\n",
                        rd_kafka_outq_len(rk));

        /* Destroy the producer instance */
        rd_kafka_destroy(rk);

        return 0;
}
