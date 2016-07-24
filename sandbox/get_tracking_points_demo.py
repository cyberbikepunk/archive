import argparse

from requests import get
import csv
from time import sleep
import os

from connectors import ValkfleetConnector

api = ValkfleetConnector()


def process_batch(writer, cursor):
    while cursor:
        if cursor == 'first':
            tracking_points_url = 'https://api.valkfleet.com/tracking_points/inrange?begin=2015-11-11&end=2015-12-11&batchsize=500'
        else:
            tracking_points_url = 'https://api.valkfleet.com/tracking_points/inrange?begin=2015-11-11&end=2015-12-16&batchsize=500&cursor={}'.format(
                cursor
            )

        response = api.db.get(tracking_points_url)
        if response.status_code == 200:
            res = response.json()
            for item in res['items']:
                del item['location']
                writer.writerow(item)
            print(len(res['items']))
            print("next_cursor %s" % res['cursor'])
            cursor = res['cursor']
        else:
            print("call failed")
            print(response)
            print(response.content)
            return

        sleep(1)

def main(args):

    fieldnames = [
      "datetime",
      "lat",
      "lng",
      "driver",
      "gsm_signal",
      "bearing",
      "battery",
      "ts_milisecond",
      "location_provider",
      "deleted_at",
      "speed",
      "uuid",
      "altitude",
      "accuracy",
      "num_satelites",
      "route",
      "device",
      "shift",
      "created_at",
      "modified_at",
      "network_type"
    ]
    tracking_point_file_exist = os.path.isfile('tracking_points.csv')


    with open('tracking_points.csv', 'a') as extcsvfile:

        writer = csv.DictWriter(
            extcsvfile,
            fieldnames=fieldnames
        )

        if not tracking_point_file_exist:
            writer.writeheader()

        process_batch(writer, args.cursor)



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cursor",
                        help="urlsafe cursor to start",
                        default='first')
    args = parser.parse_args()

    main(args)
