FROM confluentinc/cp-kafka-connect:5.3.0
ENV TZ=Asia/Shanghai
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"
COPY mongodb-kafka-connect-mongodb-1.5.2-SNAPSHOT.zip /usr/share/confluent-hub-components
RUN  confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:latest \
     && confluent-hub install --no-prompt /usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb-1.5.2-SNAPSHOT.zip \
     && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

