"""Generates a stream to Kafka from a time series csv file.
"""

import argparse
import csv
import json
import sys
import time
import random
from dateutil.parser import parse
from confluent_kafka import Producer
import socket


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('filename', type=str,
                        help='Time series csv file.')
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')
    parser.add_argument('--speed', type=float, default=1, required=False,
                        help='Speed up time series by a given multiplicative factor.')
    args = parser.parse_args()

    topic = args.topic
    p_key = args.filename

    conf = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}
    producer = Producer(conf)
    


    rdr = csv.reader(open(args.filename), delimiter=';')
    next(rdr)  # Skip header
    firstline = True

    while True:

        try:

            if firstline is True:
                line1 = next(rdr, None)
                ID, Outstanding_Debt, Credit_Mix, Num_Credit_Card, Interest_Rate = line1[0], float(line1[14].replace(',','.')), line1[13], int(line1[6]), int(line1[7])
                # Convert csv columns to key value pair
                result = {}
                result["ID"] = ID
                result["Outstanding_Debt"] = Outstanding_Debt
                result["Credit_Mix"] = Credit_Mix
                result["Num_Credit_Card"] = Num_Credit_Card
                result["Interest_Rate"] = Interest_Rate
                # Convert dict to json as message format
                jresult = json.dumps(result)
                firstline = False

                producer.produce(topic, key=p_key, value=jresult, callback=acked)

            else:
                line = next(rdr, None)
                diff = random.uniform(1,10)/args.speed #wysyłka danych losowo między 1 a 10 sekund
                time.sleep(diff)
                ID, Outstanding_Debt, Credit_Mix, Num_Credit_Card, Interest_Rate = line[0], float(line[14].replace(',','.')), line[13], int(line[6]), int(line[7])
                result["ID"] = ID
                result["Outstanding_Debt"] = Outstanding_Debt
                result["Credit_Mix"] = Credit_Mix
                result["Num_Credit_Card"] = Num_Credit_Card
                result["Interest_Rate"] = Interest_Rate
                jresult = json.dumps(result)

                producer.produce(topic, key=p_key, value=jresult, callback=acked)

            producer.flush()

        except TypeError:
            sys.exit()


if __name__ == "__main__":
    main()
