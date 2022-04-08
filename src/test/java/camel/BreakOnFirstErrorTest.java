package camel;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWithRouteBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.endpoint.dsl.DirectEndpointBuilderFactory.DirectEndpointBuilder;
import org.apache.camel.builder.endpoint.dsl.KafkaEndpointBuilderFactory;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.direct;
import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.kafka;
import static org.awaitility.Awaitility.await;

@CamelSpringBootTest
@EnableAutoConfiguration
@UseAdviceWith
@EmbeddedKafka(controlledShutdown = true, partitions = 1)
public class BreakOnFirstErrorTest {

    @Autowired
    protected ProducerTemplate kafkaInputProducer;

    @Autowired
    protected CamelContext camelContext;

    @Value("${spring.embedded.kafka.brokers}")
    private String brokerAddresses;

    private int consumptionCounter = 0;

    @Test
    public void shouldReconsumeFromKafkaOnError() throws Exception {
        DirectEndpointBuilder mockKafkaProducer = direct("mockKafkaProducer");
        kafkaInputProducer.setDefaultEndpoint(mockKafkaProducer.resolve(camelContext));

        AdviceWithRouteBuilder.addRoutes(camelContext, builder -> {
            createProducerRoute(mockKafkaProducer, builder);
            createConsumerRoute(builder);
        });

        camelContext.start();
        kafkaInputProducer.sendBody("A TEST MESSAGE");
        // Counter is only incremented once, and test fails
        await().until(() -> consumptionCounter > 1);
    }

    private void createConsumerRoute(RouteBuilder builder) {
        builder.from(kafkaTestTopic()
                        .breakOnFirstError(true)
                        .autoOffsetReset("earliest"))
                .log("CONSUMED_RECORD")
                .process(e -> {
                    consumptionCounter++;
                    throw new RuntimeException("ERROR_TRIGGER_BY_TEST");
                });
    }

    private void createProducerRoute(DirectEndpointBuilder mockKafkaProducer, RouteBuilder builder) {
        builder.from(mockKafkaProducer)
                .to(kafkaTestTopic());
    }

    private KafkaEndpointBuilderFactory.KafkaEndpointBuilder kafkaTestTopic() {
        return kafka("test_topic")
                .brokers(brokerAddresses);
    }
}
