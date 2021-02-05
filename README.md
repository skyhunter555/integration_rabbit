# Service for testing Spring Integration with RabbitMQ 
Library name: integration-rabbit

  В примере создается одна входящая очередь inputQueue и исходящая очередь outputQueue.
Между очередями настроен маршрутизатор который отправляет сообщение из входящей очереди в исходящую.
Для проверки транзакционного режима, создан компонент слушатель на входящую очередь.
В этом конпоненте эмулируется обработка сообщения и создается ошибка для каждого десятого сообщения. 
При этом, сообщение остается в исходной очереди, а затем повторно пересылается.

После обработки сообщений в логе выводиться информация:

InputMessageConsumer - START CONSUME MESSAGE, docId: 1 docType: order

InputMessageConsumer - FINISH CONSUME MESSAGE. Total consumed: 10

Ссылки на использованную документацию:

https://github.com/spring-projects/spring-amqp/

https://www.rabbitmq.com/tutorials/tutorial-four-spring-amqp.html

## Example
java -jar integration-rabbit-1.0.0.jar

## Build
mvn clean install
