from neo4j_config import driver, close_driver

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

if __name__ == "__main__":
    with driver.session() as session:
        session.execute_write(create_layers)
    close_driver()
    print("Layers created")
