# Kafka Java Consumer Configuration - Explained with Examples

This guide explains essential Kafka consumer configuration properties used in a Java application. Each setting is annotated with its usage and a sample configuration value.

---

## 🔧 1. key.deserializer & value.deserializer

These convert the received byte stream back into objects.

```java
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
```

Use `StringDeserializer` for simple data, or `JsonDeserializer` for POJOs.

---

## ⛓️ 2. group.id

Identifies the consumer group this consumer belongs to.

```java
props.put(ConsumerConfig.GROUP_ID_CONFIG, "people.basic.java.grp-0");
```

---

## 🔁 3. enable.auto.commit

Automatically commits offsets at a regular interval.

```java
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // default
```

Set to `false` to control offset commits manually.

---

## 🕒 4. auto.commit.interval.ms

How often to commit offsets if auto-commit is enabled.

```java
props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
```

---

## 🚦 5. auto.offset.reset

Defines what to do when there is no initial offset or if the current offset is invalid.

```java
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
```

Options:
- `"earliest"`: Start from beginning of topic
- `"latest"`: Start from end of topic
- `"none"`: Throw error if no previous offset is found

---

## 📥 6. max.poll.records

Controls how many records are returned per poll.

```java
props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
```

Useful for controlling load.

---

## ⏱️ 7. max.poll.interval.ms

Maximum delay between polls before consumer is considered dead.

```java
props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 min
```

---

## 🧠 8. session.timeout.ms & heartbeat.interval.ms

Used to detect dead consumers and trigger a rebalance.

```java
props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
```

---

## ✅ 9. Sample Consumer Configuration Block

```java
Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ConsumerConfig.GROUP_ID_CONFIG, "people.basic.java.grp-0");
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(List.of("people.basic.java"));
```

---

These settings allow you to build robust Kafka consumers with full control over polling, offset management, and rebalancing.

docker exec -it cli-tools kafka-topics --bootstrap-server broker0:29092 --list
How to list all the consumer-groups
docker exec -it cli-tools kafka-consumer-groups --bootstrap-server broker0:29092 --list
then to describe the group
docker exec -it cli-tools kafka-consumer-groups --bootstrap-server broker0:29092 --describe --group people.adv.java.grp-0
to reset an offset 
docker exec -it cli-tools kafka-consumer-groups --bootstrap-server broker0:29092 --reset-offsets --to-offset 5 --group people.adv.java.grp-0 --topic people.adv.java:0 --dry-run/execute
