from datetime import datetime
from neo4j_config import driver, close_driver

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
        session.execute_write(create_experiment)
    close_driver()
    print("Experiment stored")
