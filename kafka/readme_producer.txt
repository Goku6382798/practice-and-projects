# Kafka Java Producer Configuration - Explained with Examples

This guide explains essential Kafka producer configuration properties used in a Java application. Each setting is annotated with its usage and a sample configuration value.

---

## 🔧 1. key.serializer & value.serializer

These convert your key/value to bytes before sending to Kafka.

```java
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
```

Use `StringSerializer` for simple keys/values, or `JsonSerializer` for POJOs.

---

## ⏱️ 2. linger.ms

Defines how long the producer waits to batch more messages before sending.

```java
props.put(ProducerConfig.LINGER_MS_CONFIG, 5); // 5ms delay
```

Useful for improving batching/throughput.

---

## 📦 3. batch.size

Controls the max size (in bytes) of a batch.

```java
props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024); // 32KB
```

Helps in optimizing throughput.

---

## ✅ 4. acks

Controls how many broker acknowledgments are required for a request.

```java
props.put(ProducerConfig.ACKS_CONFIG, "all");
```

- `"0"`: No acknowledgment
- `"1"`: Leader only
- `"all"`: Leader + replicas (most reliable)

---

## 🧬 5. enable.idempotence

Ensures no duplicate messages are sent during retries.

```java
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
```

Automatically sets:
- `acks=all`
- retries
- limits in-flight requests

---

## 🚦 6. max.in.flight.requests.per.connection

Controls how many messages can be sent before getting acknowledgments.

```java
props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
```

- Use `1` for strict ordering.
- Use `≤5` with idempotence.

---

## 🔁 7. retries

Controls how many times the producer retries sending a failed message.

```java
props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
```

---

## ⏳ 8. delivery.timeout.ms

Maximum time to wait for a message to be acknowledged including retries.

```java
props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000); // 2 minutes
```

Must be ≥ `linger.ms` + retry time.

---

## ✅ Final Sample Properties Block

```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

props.put(ProducerConfig.ACKS_CONFIG, "all");
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);
props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
```

---

These settings help you tune the Kafka producer for high-throughput, fault-tolerant, and exactly-once delivery scenarios.
