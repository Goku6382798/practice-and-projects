package com.example.demo.producers;

import java.time.Instant;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import order.domain.events.Customer;
import order.domain.events.Products;
import order.domain.events.Order;

@Component
public class OrderDataProducers implements ApplicationListener<ApplicationStartedEvent> {

    static final Logger logger = LoggerFactory.getLogger(OrderDataProducers.class);

    @Value("${topics.customers.name:customers}")
    String customersTopic;

    @Value("${topics.products.name:products}")
    String productsTopic;

    @Value("${topics.order-created.name:order.created}")
    String ordersTopic;

    final KafkaTemplate<Integer, Customer> customerKafkaTmpl;
    final KafkaTemplate<Integer, Products> productKafkaTmpl;
    final KafkaTemplate<Integer, Order> orderKafkaTmpl;

    public OrderDataProducers(
            KafkaTemplate<Integer, Customer> customerKafkaTmpl,
            KafkaTemplate<Integer, Products> productKafkaTmpl,
            KafkaTemplate<Integer, Order> orderKafkaTmpl
    ) {
        this.customerKafkaTmpl = customerKafkaTmpl;
        this.productKafkaTmpl = productKafkaTmpl;
        this.orderKafkaTmpl = orderKafkaTmpl;
    }

    public List<Customer> makeCustomers() {
        return List.of(
                new Customer(1000, "Acme Corp."),
                new Customer(1001, "Hooli Corp."),
                new Customer(1002, "Wayne Enterprise"),
                new Customer(1003, "Mystery Inc")
        );
    }

    public List<Products> makeProducts() {
        return List.of(
                new Products(5000, "Wonka Bars", 5),
                new Products(5100, "Chia Pet", 18),
                new Products(5200, "Stress Ball", 4),
                new Products(5300, "Back Scratcher", 7)
        );
    }

    @Override
    public void onApplicationEvent(ApplicationStartedEvent event) {
        logger.info("🚀 Starting to publish customers and products...");

        List<Customer> customers = makeCustomers();
        List<Products> products = makeProducts();

        for (Customer customer : customers) {
            customerKafkaTmpl.send(customersTopic, customer.getId(), customer)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            logger.error("❌ Failed to publish customer {}", customer, ex);
                        } else {
                            logger.info("✅ Published customer {}", customer);
                        }
                    });
        }
        customerKafkaTmpl.flush();

        for (Products product : products) {
            productKafkaTmpl.send(productsTopic, product.getId(), product)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            logger.error("❌ Failed to publish product {}", product, ex);
                        } else {
                            logger.info("✅ Published product {}", product);
                        }
                    });
        }
        productKafkaTmpl.flush();

        logger.info("📦 Starting to stream orders in background...");

        // Run order production in background thread
        new Thread(() -> {
            int orderId = (int) (System.currentTimeMillis() / 1_000_000L);
            Random random = new Random();

            while (true) {
                Customer customer = customers.get(random.nextInt(customers.size()));
                Products product = products.get(random.nextInt(products.size()));
                int qty = random.nextInt(1000) + 1;

                Order order = Order.newBuilder()
                        .setId(orderId++)
                        .setCustomerId(customer.getId())
                        .setProductId(product.getId())
                        .setProductQty(qty)
                        .setCreatedMs(Instant.now())
                        .setValid(false)
                        .build();

                orderKafkaTmpl.send(ordersTopic, order.getId(), order)
                        .whenComplete((result, ex) -> {
                            if (ex != null) {
                                logger.error("❌ Failed to publish order {}", order, ex);
                            } else {
                                logger.info("✅ Produced order {}", order);
                            }
                        });

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error("Thread interrupted", e);
                    break;
                }
            }
        }).start();
    }
}
