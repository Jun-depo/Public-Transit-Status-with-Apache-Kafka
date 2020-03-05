"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

# create a ksql `turnstile` table from your turnstile topic.
# Make sure to use 'avro' datatype!
# Then, create a `turnstile_summary` table by selecting from the
# `turnstile` table and grouping on station_id.
#  Cast the COUNT of station id to `count`
#  Set the value format to JSON

KSQL_STATEMENT = """
CREATE STREAM turnstile (
    station_id INTEGER,
    station_name VARCHAR,
    line VARCHAR)    
WITH (kafka_topic='org.chicago.cta.turnstile',  value_format='AVRO', key='station_id')
;

CREATE TABLE turnstile_summary
 AS SELECT station_id, count(station_id) AS count 
 FROM turnstile
 GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
