from neo4j import GraphDatabase

unit_info = {
    "R": "Rankine temperature scale",
    "psia": "Pounds per square inch absolute",
    "rpm": "Revolutions per minute",
    "pps/psia": "Pounds per second per psi absolute",
    # Add other units as needed
}

index_names = ['engine', 'cycle']
setting_names = ['setting_1', 'setting_2', 'setting_3']
sensor_names = [
    ("(Fan inlet temperature) (◦R)", "R"),
    ("(LPC outlet temperature) (◦R)", "R"),
    ("(HPC outlet temperature) (◦R)", "R"),
    ("(LPT outlet temperature) (◦R)", "R"),
    ("(Fan inlet Pressure) (psia)", "psia"),
    ("(bypass-duct pressure) (psia)", "psia"),
    ("(HPC outlet pressure) (psia)", "psia"),
    ("(Physical fan speed) (rpm)", "rpm"),
    ("(Physical core speed) (rpm)", "rpm"),
    ("(Engine pressure ratio(P50/P2)", None),
    ("(HPC outlet Static pressure) (psia)", "psia"),
    ("(Ratio of fuel flow to Ps30) (pps/psia)", "pps/psia"),
    ("(Corrected fan speed) (rpm)", "rpm"),
    ("(Corrected core speed) (rpm)", "rpm"),
    ("(Bypass Ratio) ", None),
    ("(Burner fuel-air ratio)", None),
    ("(Bleed Enthalpy)", None),
    ("(Required fan speed)", None),
    ("(Required fan conversion speed)", None),
    ("(High-pressure turbines Cool air flow)", None),
    ("(Low-pressure turbines Cool air flow)", None)
]

class MetadataIngest:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def ingest_metadata(self, dataset_name, datafile_name, file_type):
        """Ingest train or test datafile metadata (full feature columns)"""
        with self.driver.session() as session:
            session.execute_write(
                ingest_metadata_tx,
                dataset_name,
                datafile_name,
                file_type,
                unit_info,
                index_names,
                setting_names,
                sensor_names
            )

    def ingest_rul_metadata(self, dataset_name, datafile_name, file_type="RUL"):
        """Ingest RUL file metadata with only one feature 'RUL_Value'"""
        with self.driver.session() as session:
            session.execute_write(
                ingest_rul_metadata_tx,
                dataset_name,
                datafile_name,
                file_type,
                unit_info
            )

def ingest_metadata_tx(tx, dataset_name, datafile_name, file_type, unit_info,
                       index_names, setting_names, sensor_names):
    # Create Dataset and DataFile nodes, set file_type property
    tx.run("""
        MERGE (ds:Dataset {name: $dataset})
        MERGE (df:DataFile {name: $file})
        SET df.type = $file_type
        MERGE (ds)-[:CONTAINS]->(df)
    """, dataset=dataset_name, file=datafile_name, file_type=file_type)

    def create_feature(tx, name, category, unit):
        tx.run("""
            MERGE (f:Feature {name: $name})
            MERGE (c:Category {name: $category})
            MERGE (f)-[:BELONGS_TO]->(c)
            MERGE (df:DataFile {name: $file})
            MERGE (df)-[:HAS_FEATURE]->(f)
        """, name=name, category=category, file=datafile_name)

        if unit:
            desc = unit_info.get(unit, None)
            tx.run("""
                MERGE (u:Unit {name: $unit})
                ON CREATE SET u.description = $desc
                MERGE (f:Feature {name: $name})
                MERGE (f)-[:MEASURED_IN]->(u)
            """, unit=unit, desc=desc, name=name)

    for name in index_names:
        create_feature(tx, name, "Index", None)

    for name in setting_names:
        create_feature(tx, name, "Setting", None)

    for name, unit in sensor_names:
        create_feature(tx, name, "Sensor", unit)

def ingest_rul_metadata_tx(tx, dataset_name, datafile_name, file_type, unit_info):
    # Create Dataset and DataFile nodes for RUL file
    tx.run("""
        MERGE (ds:Dataset {name: $dataset})
        MERGE (df:DataFile {name: $file})
        SET df.type = $file_type
        MERGE (ds)-[:CONTAINS]->(df)
    """, dataset=dataset_name, file=datafile_name, file_type=file_type)

    # Create the single RUL feature node and connect
    tx.run("""
        MERGE (f:Feature {name: 'RUL_Value'})
        MERGE (c:Category {name: 'RUL'})
        MERGE (f)-[:BELONGS_TO]->(c)
        MERGE (df:DataFile {name: $file})
        MERGE (df)-[:HAS_FEATURE]->(f)
    """, file=datafile_name)

if __name__ == "__main__":
    URI = "bolt://localhost:7687"
    USER = "neo4j"
    PASSWORD = "cool@1983"

    try:
        ingest = MetadataIngest(URI, USER, PASSWORD)

        # Example calls
        ingest.ingest_metadata("N-CMAPSS", "train_FD001", "train")
        print("Successfully ingested train_FD001 metadata.")

        ingest.ingest_metadata("N-CMAPSS", "test_FD001", "test")
        print("Successfully ingested test_FD001 metadata.")

        ingest.ingest_rul_metadata("N-CMAPSS", "RUL_FD001", "RUL")
        print("Successfully ingested RUL_FD001 metadata.")

    except Exception as e:
        print(f"Error during ingestion: {e}")

    finally:
        try:
            ingest.close()
        except Exception:
            pass
