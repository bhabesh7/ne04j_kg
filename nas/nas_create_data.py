from neo4j_config import driver, close_driver
from datetime import datetime

def create_constraints(tx):
    tx.run("""
    CREATE CONSTRAINT arch_name IF NOT EXISTS
    FOR (a:Architecture)
    REQUIRE a.name IS UNIQUE
    """)
    
    tx.run("""
    CREATE CONSTRAINT layer_name IF NOT EXISTS
    FOR (l:Layer)
    REQUIRE l.name IS UNIQUE
    """)
    
    tx.run("""
    CREATE CONSTRAINT dataset_name IF NOT EXISTS
    FOR (d:Dataset)
    REQUIRE d.name IS UNIQUE
    """)
    
    tx.run("""
    CREATE CONSTRAINT hardware_name IF NOT EXISTS
    FOR (h:Hardware)
    REQUIRE h.name IS UNIQUE
    """)

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

def create_architecture(tx):
    tx.run("""
    MERGE (a:Architecture {
        name: $name,
        depth: $depth
    })
    """, name="NAS_CNN_v1", depth=5)

    tx.run("""
    MATCH (a:Architecture {name:$arch}),
          (l1:Layer {name:"Conv3x3"}),
          (l2:Layer {name:"ReLU"}),
          (l3:Layer {name:"MaxPool2x2"})
    MERGE (a)-[:COMPOSED_OF {order:1}]->(l1)
    MERGE (a)-[:COMPOSED_OF {order:2}]->(l2)
    MERGE (a)-[:COMPOSED_OF {order:3}]->(l3)
    """, arch="NAS_CNN_v1") 

def create_layers(tx):
    layers = [
        {"name": "Conv3x3", "type": "Conv", "kernel": 3, "params": 1792},
        {"name": "ReLU", "type": "Activation"},
        {"name": "MaxPool2x2", "type": "Pooling", "kernel": 2}
    ]

    for layer in layers:
        tx.run("""
        MERGE (l:Layer {name: $name})
        SET l += $props
        """, name=layer["name"], props=layer)

def create_experiment(tx):
    tx.run("""
    MATCH (a:Architecture {name:$arch}),
          (d:Dataset {name:$dataset}),
          (h:Hardware {name:$hardware})
    CREATE (e:Experiment {
        accuracy: $accuracy,
        latencyMs: $latency,
        flops: $flops,
        energy_mJ: $energy,
        timestamp: datetime($time)
    })
    MERGE (a)-[:HAS_EXPERIMENT]->(e)
    MERGE (a)-[:TRAINED_ON]->(d)
    MERGE (a)-[:EVALUATED_ON]->(h)
    """,
    arch="NAS_CNN_v1",
    dataset="CIFAR-10",
    hardware="Jetson-Nano",
    accuracy=0.87,
    latency=18,
    flops=1.2e8,
    energy=35,
    time=datetime.utcnow().isoformat()
    )

if __name__ == "__main__":
    with driver.session() as session:
        # 1. create schema/constraints
        session.execute_write(create_constraints)
        # 2. data
        session.execute_write(create_dataset_and_hardware)
        # 3. layers
        session.execute_write(create_layers)
        # 4. architectures and relations would go here
        session.execute_write(create_architecture)

    close_driver()
    print("data created successfully")