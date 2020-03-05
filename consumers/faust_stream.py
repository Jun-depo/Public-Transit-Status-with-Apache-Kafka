"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("org.chicago.cta.stations", value_type=Station)
# Define the output Kafka Topic
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)

# Define a Faust changelog Table
table = app.Table(
   "org.chicago.cta.stations.changelog_topic",
   default=dict,
   partitions=1,
   changelog_topic=out_topic,
)


@app.agent(topic)
async def station_changelog(stations):
    async for st in stations:
        table["station_id"] = st.station_id
        table["station_name"] = st.station_name
        table["order"] = st.order
        table["line"] = st.line


# Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
# The transformaed stream carries line data
@app.agent(topic)
async def station_transform(stations):
    async for st in stations:
        if st.red:
            line = "red"
        elif st.blue:
            line = "blue"
        elif st.green:
            line = "green"

        out_topic = TransformedStation(
                        station_id = st.station_id,
                        station_name = st.station_name,
                        order = st.order,
                        line = line
                    )

        await out_topic.send(key=st.station_id, value=out_topic)


if __name__ == "__main__":
    app.main()
