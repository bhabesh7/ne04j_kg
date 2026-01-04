from neo4j_config import driver, close_driver

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

if __name__ == "__main__":
    with driver.session() as session:
        session.execute_write(create_architecture)
    close_driver()
    print("Architecture created")
