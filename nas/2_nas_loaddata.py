from neo4j_config import driver, close_driver

def create_dataset_and_hardware(tx):
    tx.run("""
    MERGE (d:Dataset {
        name: $dataset,
        samples: $samples,
        classes: $classes
    })
    """, dataset="CIFAR-10", samples=60000, classes=10)

    tx.run("""
    MERGE (h:Hardware {
        name: $hardware,
        maxMemoryMB: $memory,
        maxLatencyMs: $latency
    })
    """, hardware="Jetson-Nano", memory=4096, latency=20)

if __name__ == "__main__":
    with driver.session() as session:
        session.execute_write(create_dataset_and_hardware)
    close_driver()
    print("Dataset and hardware loaded")
