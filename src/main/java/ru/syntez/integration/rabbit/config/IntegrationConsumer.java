package ru.syntez.integration.rabbit.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.syntez.integration.rabbit.entities.RoutingDocument;
import ru.syntez.integration.rabbit.exceptions.RouterException;

/**
 * Configuration custom IntegrationConsumer
 *
 * @author Skyhunter
 * @date 05.02.2021
 */
@Component
public class IntegrationConsumer {

    private static Logger LOG = LogManager.getLogger(IntegrationConsumer.class);

    @Value("${consumer.work-time}")
    private Integer delayMillis;

    private Integer consumedDocumentCount = 0;

    public boolean execute(RoutingDocument document) {

        LOG.info("START CONSUME MESSAGE, docId: {} docType: {}", document.getDocId(), document.getDocType());
        if (consumedDocumentCount >= 10) {

            //Сброс счетчика, для повторной отправки
            if (consumedDocumentCount >= 19) {
                consumedDocumentCount = 0;
            }

            throw new RouterException("ConsumedDocumentCount > 10. Send to redelivery");
        }
        try {
            Thread.sleep(delayMillis);
            consumedDocumentCount++;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("FINISH CONSUME MESSAGE, docId: {} docType: {}. Total consumed: {}", document.getDocId(), document.getDocType(), consumedDocumentCount);
        return true;
    }
}
