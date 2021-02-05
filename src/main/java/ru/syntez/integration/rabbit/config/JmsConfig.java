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
import org.springframework.amqp.rabbit.transaction.RabbitTransactionManager;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.transaction.TransactionInterceptorBuilder;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import ru.syntez.integration.rabbit.IntegrationRabbitMain;
import ru.syntez.integration.rabbit.utils.JmsXmlMessageConverter;

/**
 * Configuration for spring integration jms
 * 1. Create ConnectionFactory
 * 2. Create queue and binds
 * 3. Create RabbitTemplate
 * 4. Create IntegrationFlow to routing documents
 * 5. TransactionManager
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
    private String exchangeInputName = "exchangeInput";

    @Value("${jms.rabbit-mq.exchange-output-name}")
    private String exchangeOutputName = "exchangeOutput";

    @Value("${jms.rabbit-mq.queue-input-name}")
    private String queueInputName = "input";

    @Value("${jms.rabbit-mq.queue-output-name}")
    private String queueOutputName = "output";

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setHost(brokerHost);
        connectionFactory.setPort(brokerPort);
        connectionFactory.setVirtualHost(virtualHost);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        return connectionFactory;
    }

    @Bean
    public DirectExchange directInputExchange() {
        return new DirectExchange(exchangeInputName);
    }

    @Bean
    public DirectExchange directOutputExchange() {
        return new DirectExchange(exchangeOutputName);
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
        return BindingBuilder.bind(outputQueue()).to(directOutputExchange()).withQueueName();
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
        RabbitTransactionManager rabbitTransactionManager = new RabbitTransactionManager(connectionFactory);
        rabbitTransactionManager.setRollbackOnCommitFailure(true);
        return rabbitTransactionManager;
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

    @Bean
    public IntegrationFlow myFlowInputToOutput() {
        return IntegrationFlows.from(Amqp.inboundAdapter(connectionFactory(), inputQueue()))
                .channel(
                        Amqp.pollableChannel(connectionFactory())
                                .queueName(queueOutputName)
                                .channelTransacted(true)
                )
                .get();
    }

}

