from neo4j_config import driver, close_driver

def find_valid_architectures(tx):
    result = tx.run("""
    MATCH (a:Architecture)-[:HAS_EXPERIMENT]->(e:Experiment)
    WHERE e.accuracy > 0.85 AND e.latencyMs < 20
    RETURN a.name, e.accuracy, e.latencyMs
    """)
    return result.data()

with driver.session() as session:
    architectures = session.execute_read(find_valid_architectures)
    for arch in architectures:
        print(arch)
