import uuid
from datetime import datetime
from typing import MutableMapping
import pytz
import yfinance as yf
import os
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry._sync.avro import AvroSerializer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
import json


BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
STOCKS_TOPIC = os.getenv("STOCKS_TOPIC")
CRYPTO_TOPIC = os.getenv("CRYPTO_TOPIC")
CURRENCY_TOPIC = os.getenv("CURRENCY_TOPIC")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
TICKERS = os.getenv("TICKERS")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

topics = {"stocks": {"topic": STOCKS_TOPIC}, "crypto": {"topic": CRYPTO_TOPIC}, "currency": {"topic": CURRENCY_TOPIC}}


def get_tickers():
    with open("tickers.json", "r") as f:
        return json.loads(f.read())


def get_tickers_topic(ticker: str) -> str:
    tickers = get_tickers()
    asset_class = tickers[ticker]["type"]
    return topics[asset_class]


class KafkaWrapper:
    def __init__(self, bootstrap_servers: str, schema_registry_url: str, user: str, password: str):
        self.topics = topics
        self.tickers = get_tickers()
        schema_registry_config = {"url": schema_registry_url}
        schema_registry_client = SchemaRegistryClient(schema_registry_config)
        for asset, value in self.topics.items():
            value["schema"] = schema_registry_client.get_latest_version(f"{value["topic"]}-value")
            value["schema_str"] = value["schema"].schema.schema_str
            value["fields"] = [field["name"] for field in json.loads(value["schema_str"])["fields"]]
            value["avro_serializer"] = AvroSerializer(schema_registry_client, value["schema_str"], self.to_dict)

            print(f"FIELDS: {value['fields']}")

        self.string_serializer = StringSerializer("utf_8")
        producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "on_delivery": self.delivery_report,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "PLAIN",
            "sasl.username": user,
            "sasl.password": password,
            "client.id": "ingestion-producer",
        }
        self.producer = SerializingProducer(producer_config)

    @staticmethod
    def to_dict(obj, ctx):
        return obj

    def add_metadata(self, data: MutableMapping) -> MutableMapping:
        data["ingestion_ts"] = str(int(datetime.now(pytz.UTC).timestamp()) * 1000)  # epoch ms
        data["event_id"] = self.get_event_id(data["id"], data["ingestion_ts"])
        data["source_api"] = "YahooFinance"
        return data

    @staticmethod
    def get_event_id(id: str, ts: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{id}_{ts}".encode("utf-8")))

    @staticmethod
    def filter_using_schema(data: MutableMapping, fields: list) -> MutableMapping:
        return {k: v for (k, v) in data.items() if k in fields}

    def publish(self, key, value):
        asset_type = self.tickers[key]["type"]
        topic = self.topics[asset_type]["topic"]
        value = self.filter_using_schema(value, self.topics[asset_type]["fields"])
        value = self.add_metadata(value)
        avro_serializer = self.topics[asset_type]["avro_serializer"]
        try:
            self.producer.produce(
                topic=topic,
                key=self.string_serializer(key),
                value=avro_serializer(value, SerializationContext(topic, MessageField.VALUE)),
            )
        except ValueError as e:
            print(f"Invalid input, discarding record... {e}")

        print("\nFlushing records...")
        self.producer.flush()

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            print(f"Delivery failed for record {msg.key()}: {err}")
            return
        print(f"Record {msg.key()} successfully published to {msg.topic} [{msg.partition}] at offset {msg.partition}")


class YahooFinanceConnector:
    def __init__(self, kf: KafkaWrapper = None):
        self.kf = kf
        self.ws = yf.WebSocket()
        self.msg = None

    @staticmethod
    def handle(msg):
        ticker = msg["id"]
        kf.publish(ticker, msg)
        print(f"Published message:", msg)

    def connect(self):
        print("Connecting to YahooFinance WebSocket...")
        with yf.WebSocket() as ws:
            ws.subscribe(get_tickers().keys())
            ws.listen(self.handle)


if __name__ == "__main__":
    kf = KafkaWrapper(BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, USER, PASSWORD)
    yfin = YahooFinanceConnector(kf)
    yfin.connect()
