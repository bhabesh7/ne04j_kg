
# 1. Propose architecture candidate
# 2. Query KG for constraints & prior knowledge
# 3. Decide: train or skip
# 4. (Mock) evaluate architecture
# 5. Store result in KG
# 6. Repeat

import random
from neo4j_config import driver
# Define a simple search space
SEARCH_SPACE = [
    ["Conv3x3", "ReLU", "MaxPool2x2"],
    ["Conv3x3", "ReLU", "Conv3x3", "ReLU"],
    ["Conv3x3", "ReLU", "MaxPool2x2", "Conv3x3"],
]

# Randomly chose an architecture from search space
def propose_architecture():
    layers = random.choice(SEARCH_SPACE)
    name = f"NAS_CNN_{random.randint(100,999)}"
    return name, layers

# Query KG before training - have similar architectures failed before?
def should_train(tx, layer_count):
    result = tx.run("""
    MATCH (a:Architecture)-[:HAS_EXPERIMENT]->(e:Experiment)
    WHERE a.depth = $depth AND e.latencyMs > 20
    RETURN count(a) AS bad_count
    """, depth=layer_count)
    
    record = result.single()
    return record["bad_count"] == 0

# Mock evaluation function. This can be replaced with real training + eval
def mock_evaluate(layers):
    accuracy = round(0.7 + 0.05 * len(layers), 2)
    latency = 10 + 3 * len(layers)
    return accuracy, latency

from datetime import datetime

# Store the architecture's evaluation result in KG
def store_result(tx, exp_name, arch_name, layers, accuracy, latency):
    tx.run("""
    MERGE (a:Architecture {name:$name})
    SET a.depth = $depth
    """, name=arch_name, depth=len(layers))

    for i, layer in enumerate(layers):
        tx.run("""
        MATCH (a:Architecture {name:$arch}),
              (l:Layer {name:$layer})
        MERGE (a)-[:COMPOSED_OF {order:$order}]->(l)
        """, arch=arch_name, layer=layer, order=i+1)

    tx.run("""
    MATCH (a:Architecture {name:$arch})
    CREATE (e:Experiment {
        name: $name,
        accuracy: $acc,
        latencyMs: $lat,
        timestamp: datetime($time)
    })
    MERGE (a)-[:HAS_EXPERIMENT]->(e)
    """,
    name=exp_name,
    arch=arch_name,
    acc=accuracy,
    lat=latency,
    time=datetime.utcnow().isoformat()
    )


#simulate the entire NAS loop using the funcions above
def nas_loop(iterations=5):
    with driver.session() as session:
        for i in range(iterations):
            print(f"\n NAS Iteration {i+1}")

            arch_name, layers = propose_architecture()
            print("Proposed:", arch_name, layers)

            can_train = session.execute_read(
                should_train, len(layers)
            )

            if not can_train:
                print("Pruned by KG (latency risk)")
                continue

            accuracy, latency = mock_evaluate(layers)
            print("Evaluated → acc:", accuracy, "lat:", latency)
            curr_exp_name = f"exp_{arch_name}"

            session.execute_write(
                store_result,
                exp_name=curr_exp_name,
                arch_name=arch_name,
                layers=layers,
                accuracy=accuracy,
                latency=latency
            )

            print("Stored in Knowledge Graph")

if __name__ == "__main__":
    print("Starting NAS with Knowledge Graph Integration")
    nas_loop(iterations=10)
    driver.close()
