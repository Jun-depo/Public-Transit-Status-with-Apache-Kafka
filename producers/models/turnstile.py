"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware

from datetime import datetime

logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    
    # Define the value schema in `schemas/turnstile_value.json`
   
    value_schema = avro.load(
       f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )


        super().__init__(
            topic_name = "org.chicago.cta.turnstile", # create topic name
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema, 
            num_partitions=4,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        #logger.info("turnstile kafka integration incomplete - skipping")
        
        # Produce a message to the turnstile topic for the number of entries that were calculated
        # input timestamp is in datetime.datetime format.  Transform the data to timestamp format (long int)
        
        timestamp = int(datetime.timestamp(timestamp)) #  Transform time to timestamp format (long int), python3 only support int
        
        self.producer.produce(  
            topic=self.topic_name,
            key={"timestamp": timestamp},
            key_schema = Turnstile.key_schema,
            value={               
                "station_id": self.station.station_id,
                "station_name": self.station.name, 
                "line": self.station.color.name,
                "num_entries": num_entries
                },
            value_schema = avro.loads("""{
                                    "namespace": "org.chicago.cta",
                                    "type": "record",
                                    "name": "turnstile_entry",
                                    "fields": [
                                    {"name": "station_id", "type": "int"},
                                    {"name": "station_name", "type": "string"},
                                    {"name": "line", "type": "string"},
                                    {"name": "num_entries", "type": "int"}
                                    ]
                                }""")
        )
        self.producer.flush()
