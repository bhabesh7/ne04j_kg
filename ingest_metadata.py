from neo4j import GraphDatabase
import random

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

    

    def create_asset_and_link(tx, asset_name, datafile_name):
        """
        Create an Asset node with asset_id (random number) and asset_type='turbofan_engine',
        and link it to a DataFile node with a 'linked_asset' relationship.
        """
        asset_id = random.randint(100000, 999999)
        tx.run("""
            MERGE (a:Asset {name: $asset_name})
            ON CREATE SET a.asset_id = $asset_id, a.asset_type = 'turbofan_engine'
            ON MATCH SET a.asset_type = 'turbofan_engine'
            MERGE (df:DataFile {name: $datafile_name})
            MERGE (df)-[:linked_asset]->(a)
        """, asset_name=asset_name, datafile_name=datafile_name, asset_id=asset_id)

    def create_asset_and_link_to_datafile(self, asset_name, datafile_name):
        """
        Public method to create an Asset node and link it to a DataFile node.
        """
        with self.driver.session() as session:
            session.execute_write(create_asset_and_link, asset_name, datafile_name)

    # Add the method to MetadataIngest class
    MetadataIngest.create_asset_and_link_to_datafile = create_asset_and_link_to_datafile


    def create_storage_and_link(tx, datafile_name, storage_type, storage_path, storage_url, storage_name):
        """
        Create a Storage node with given type and path, and link it to a DataFile node.
        """
        tx.run("""
            MERGE (s:Storage {type: $storage_type, path: $storage_path, storage_url: $storage_url, storage_name: $storage_name})
            MERGE (df:DataFile {name: $datafile_name})
            MERGE (df)-[:is_stored_in]->(s)
        """, storage_type=storage_type, storage_path=storage_path, datafile_name=datafile_name, storage_url=storage_url, storage_name = storage_name )

    def create_storage_and_link_to_datafile(self, datafile_name, storage_type, storage_path, storage_url, storage_name):
        """
        Public method to create a Storage node and link it to a DataFile node.
        """
        with self.driver.session() as session:
            session.execute_write(create_storage_and_link, datafile_name, storage_type, storage_path, storage_url, storage_name )

    # Add the method to MetadataIngest class
    MetadataIngest.create_storage_and_link_to_datafile = create_storage_and_link_to_datafile

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

        ingest.create_asset_and_link_to_datafile("FD001", "train_FD001")
        print("Successfully created and linked assset FD001 to train_FD001.")

        ingest.create_asset_and_link_to_datafile("FD001", "test_FD001")
        print("Successfully created and linked asset FD001 to test_FD001.")

        ingest.create_storage_and_link_to_datafile("train_FD001", "minio", "/data/train/train_FD001.txt", "localhost:9009", "train_fd001_storage")
        print("Successfully created and linked minio storage for train_FD001.")

        # ingest.create_storage_and_link_to_datafile("test_FD001", "local", "/data/test_FD001.csv")
        # print("Successfully created and linked storage for test_FD001.")

        # ingest.create_storage_and_link_to_datafile("RUL_FD001", "local", "/data/RUL_FD001.csv")
        # print("Successfully created and linked storage for RUL_FD001.")


    except Exception as e:
        print(f"Error during ingestion: {e}")

    finally:
        try:
            ingest.close()
        except Exception:
            pass
