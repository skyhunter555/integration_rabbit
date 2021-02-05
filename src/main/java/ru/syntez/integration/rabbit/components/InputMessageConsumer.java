package ru.syntez.integration.rabbit.components;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.syntez.integration.rabbit.entities.RoutingDocument;
import ru.syntez.integration.rabbit.exceptions.RouterException;
import java.nio.charset.Charset;

/**
 * Configuration custom IntegrationConsumer
 *
 * @author Skyhunter
 * @date 05.02.2021
 */
@Component
public class InputMessageConsumer {

    private static Logger LOG = LogManager.getLogger(InputMessageConsumer.class);

    @Value("${consumer.work-time}")
    private Integer delayMillis;

    private final ObjectMapper xmlMapper;
    public InputMessageConsumer(ObjectMapper xmlMapper) {
        this.xmlMapper = xmlMapper;
    }

    private Integer consumedDocumentCount = 0;

    @RabbitListener(queues = "${jms.rabbit-mq.queue-input-name}")
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = RouterException.class)
    public void recievedMessage(Message message) throws RouterException {

        final String xmlPayload = new String(message.getBody(), Charset.defaultCharset());
        try {
            RoutingDocument document = xmlMapper.readValue(xmlPayload, RoutingDocument.class);
            LOG.info("START CONSUME MESSAGE, docId: {} docType: {}", document.getDocId(), document.getDocType());
        } catch (Exception e) {
            LOG.error(String.format("Error send files %s", e.getMessage()));
        }

        if (consumedDocumentCount >= 10) {
            //Сброс счетчика, для повторной отправки
            consumedDocumentCount = 0;
            message.getMessageProperties().setRedelivered(true);
            throw new RouterException("ConsumedDocumentCount > 10. Send to redelivery");
        }
        try {
            Thread.sleep(delayMillis);
            consumedDocumentCount++;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("FINISH CONSUME MESSAGE. Total consumed: {}", consumedDocumentCount);
    }
}
