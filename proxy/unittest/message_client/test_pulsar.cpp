#include "thread"
#include "pulsar/Client.h"

using namespace pulsar;
using MyData = milvus::grpc::PMessage;

static const std::string exampleSchema =
        "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\","
        "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}, {\"name\":\"reason\",\"type\":\"string\"}]}";

int consumer() {

    Client client("pulsar://localhost:6650");

    ConsumerConfiguration consumerConf;
    Consumer consumer;
    consumerConf.setSchema(SchemaInfo(PROTOBUF, "Protobuf", exampleSchema));
    Result result = client.subscribe("topic-proto", "sub-2", consumerConf, consumer);

    if (result != ResultOk) {
        std::cout << "Failed to subscribe: " << result << std::endl;
        return -1;
    }

    Message msg;
    for (int i = 0; i < 10; i++) {
        consumer.receive(msg);
        MyData data;
        data.ParseFromString(msg.getDataAsString());
        std::cout << " Received: " << msg
                  << "  with payload '" << data.id() << " " << data.reason()  << "'"
                  << std::endl;

        consumer.acknowledge(msg);
    }

    client.close();
    return 0;
}

int main() {
    Client client("pulsar://localhost:6650");

    Producer producer;
    ProducerConfiguration producerConf;
    producerConf.setSchema(SchemaInfo(PROTOBUF, "Protobuf", exampleSchema));
    Result result = client.createProducer("pro", producerConf, producer);

    if (result != ResultOk) {
        std::cout << "Error creating producer: " << result << std::endl;
        return -1;
    }

//    std::thread t(consumer);
    // Publish 10 messages to the topic
    for (int i = 0; i < 1; i++) {
        auto data = MyData();
        auto a = new milvus::grpc::A();
        a->set_a(9999);
        data.set_allocated_a(a);
        data.set_id("999");
        data.set_reason("*****test****");
        Message msg = MessageBuilder().setContent(data.SerializeAsString()).build();
        Result res = producer.send(msg);
        std::cout << " Message sent: " << res << std::endl;
    }
    client.close();
//    t.join();
}