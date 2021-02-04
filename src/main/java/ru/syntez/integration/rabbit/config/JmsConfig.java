package ru.syntez.integration.rabbit.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.transaction.RabbitTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.transaction.TransactionInterceptorBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import ru.syntez.integration.rabbit.IntegrationRabbitMain;
import ru.syntez.integration.rabbit.entities.RoutingDocument;
import ru.syntez.integration.rabbit.utils.JmsXmlMessageConverter;
import java.nio.charset.Charset;

/**
 * Configuration for spring integration jms
 * 1. Create ConnectionFactory
 * 2. Create queue and binds
 * 3. Create RabbitTemplate
 * 4. Create IntegrationFlow to routing documents
 *
 * @author Skyhunter
 * @date 04.02.2021
 */
@Configuration
@EnableRabbit
public class JmsConfig {

    private static Logger LOG = LogManager.getLogger(IntegrationRabbitMain.class);

    @Value("${jms.rabbit-mq.host}")
    private String brokerHost = "localhost";

    @Value("${jms.rabbit-mq.port}")
    private Integer brokerPort = 5672;

    @Value("${jms.rabbit-mq.virtual-host}")
    private String virtualHost = "user";

    @Value("${jms.rabbit-mq.username}")
    private String username = "user";

    @Value("${jms.rabbit-mq.password}")
    private String password = "user";

    @Value("${jms.rabbit-mq.exchange-input-name}")
    private String exchangeInputName = "user";

    @Value("${jms.rabbit-mq.queue-input-name}")
    private String queueInputName = "input";

    @Value("${jms.rabbit-mq.queue-output-name}")
    private String queueOutputName = "output";

    private Integer consumedDocumentCount = 0;

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setHost(brokerHost);
        connectionFactory.setPort(brokerPort);
        connectionFactory.setVirtualHost(virtualHost);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        //connectionFactory.setA
        return connectionFactory;
    }

    @Bean
    public DirectExchange directInputExchange() {
        return new DirectExchange(exchangeInputName);
    }

    @Bean
    public Queue inputQueue() {
        return QueueBuilder.durable(queueInputName).build();
    }

    @Bean
    public Queue outputQueue() {
        return QueueBuilder.durable(queueOutputName).build();
    }

    @Bean
    public Binding bindingInputQueue() {
        return BindingBuilder.bind(inputQueue()).to(directInputExchange()).withQueueName();
    }

    @Bean
    public Binding bindingOutputOrderQueue() {
        return BindingBuilder.bind(outputQueue()).to(directInputExchange()).withQueueName();
    }

    @Bean
    public MessageConverter converter() {
        return new JmsXmlMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitInputTemplate() {
        RabbitTemplate template = new RabbitTemplate(connectionFactory());
        template.setExchange(exchangeInputName);
        template.setMessageConverter(converter());
        template.setChannelTransacted(true);
        return template;
    }

    @Bean
    public ObjectMapper xmlMapper() {
        JacksonXmlModule xmlModule = new JacksonXmlModule();
        xmlModule.setDefaultUseWrapper(false);
        return new XmlMapper(xmlModule);
    }

    @Bean
    public PlatformTransactionManager transactionManager(final ConnectionFactory connectionFactory) {
        return new RabbitTransactionManager(connectionFactory);
    }

    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerMetadata defaultPoller(final PlatformTransactionManager transactionManager) {
        return Pollers.fixedDelay(10)
                .advice(new TransactionInterceptorBuilder()
                        .transactionManager(transactionManager)
                        .propagation(Propagation.REQUIRED)
                        .build())
                .get();
    }

    @Autowired
    private IntegrationConsumer integrationConsumer;

    @Bean
    public SimpleMessageListenerContainer simpleMessageListenerContainer(final ConnectionFactory connectionFactory) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setDefaultRequeueRejected(false);
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setExposeListenerChannel(true);
        container.setMaxConcurrentConsumers(2);
        container.setConcurrentConsumers(1);
        container.setQueues(inputQueue());
        container.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
            final String xmlPayload = new String(message.getBody(), Charset.defaultCharset());
            try {
                RoutingDocument document = xmlMapper().readValue(xmlPayload, RoutingDocument.class);
                final boolean success = integrationConsumer.execute(document);
                if (success) {
                    rabbitInputTemplate().convertAndSend(queueOutputName, xmlPayload);
                    channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
                }
            } catch (Exception e) {
                LOG.error(String.format("Error send files %s", e.getMessage()));
            }
        });
        return container;
    }

    @Bean
    public MessageChannel channelInput() {
        return MessageChannels.queue(queueInputName).get();
    }

}

