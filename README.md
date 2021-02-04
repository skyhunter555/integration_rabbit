# Service for testing Spring Integration with RabbitMQ 
Library name: integration-router

  В примере создается одна входящая очередь (inputqueue)
и две исходящих очереди (outputinvoice и outputorder)
для разных типов документов.
При отправке сообщения в виде XML файла проводиться проверка типа документа "docType".
В зависимости от значени "order" или "invoice", сообщение отправляется в первую или во вторую исходящую очередь.
Отправляется по 100 сообщений каждого типа.

После обработки сообщений в логе выводиться информация:

******** SELECTED INVOICE OUTPUT QUEUE FOR DOCTYPE: invoice

******** SELECTED ORDER OUTPUT QUEUE FOR DOCTYPE: order

Ссылки на использованную документацию:

https://github.com/spring-projects/spring-amqp/

https://www.rabbitmq.com/tutorials/tutorial-four-spring-amqp.html

## Example
java -jar integration-router-1.0.0.jar

## Build
mvn clean install
