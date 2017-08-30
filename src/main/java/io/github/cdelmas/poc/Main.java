package io.github.cdelmas.poc;


import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import static spark.Spark.get;
import static spark.Spark.port;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.StandardMetricsCollector;
import com.readytalk.metrics.StatsDReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static Logger logger = LoggerFactory.getLogger("ms-ctrl");

    public static void main(String[] args) throws Exception {
        logger.info("Bootstrapping the server");
        port(Integer.parseInt(System.getenv().getOrDefault("PORT", "8080")));
        get("/", (req, res) -> "UP");

        MetricRegistry metrics = new MetricRegistry();
        final Counter upscale = metrics.counter(name(Main.class, "upscale"));
        final Counter downscale = metrics.counter(name(Main.class, "downscale"));

        boolean isProd = Boolean.parseBoolean(System.getenv().getOrDefault("PROD", "false"));
        if (isProd) {
            logger.info("Connection to statsd");
            StatsDReporter.forRegistry(metrics)
                    .build("localhost", 8125) // configuration -> STATSD_HOST, STATSD_PORT
                    .start(10, TimeUnit.SECONDS);
        }

        logger.info("Connection to RabbitMQ");
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(System.getenv("RABBIT_URI"));
        connectionFactory.setMetricsCollector(new StandardMetricsCollector(metrics));
        final Connection connection = connectionFactory.newConnection();

        Channel channel = connection.createChannel();
        final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                final AMQP.Queue.DeclareOk declareOk = channel.queueDeclare("in-queue", true, false, false, new HashMap<>());
                final int messageCount = declareOk.getMessageCount();
                if (messageCount > 5) {
                    // TODO upscale
                    upscale.inc();
                } else if(messageCount <= 2) {
                    // TODO downscale
                    downscale.inc();
                }
            } catch (Exception e) {
                logger.info("Error while contacting RabbitMQ", e);
            }
        }, 30, 10, TimeUnit.SECONDS);

    }
}
