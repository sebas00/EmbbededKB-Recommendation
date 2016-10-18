"""
Import sample data for recommendation engine
"""

import predictionio
import argparse
import random

RATE_ACTIONS_DELIMITER = ","
SEED = 3

def import_events(client, file):
  f = open(file, 'r')
  random.seed(SEED)
  count = 0
  print "Importing data..."
 # generate 10 users, with user ids u1,u2,....,u10
  user_ids = ["U%s" % i for i in range(1, 4)]
  for user_id in user_ids:
    print "Set user", user_id
    client.create_event(
      event="$set",
      entity_type="user",
      entity_id=user_id
    )
    count += 1

    item_ids = ["KV-00%s" % i for i in range(10, 100)]
  for item_id in item_ids:
        print "Set item", item_id
        client.create_event(
            event="$set",
            entity_type="item",
            entity_id=item_id,
            properties={}
        )
        count += 1
        
        item_ids = ["SITE-0%s" % i for i in range(100, 200)]
  for item_id in item_ids:
        print "Set page", item_id
        client.create_event(
            event="$set",
            entity_type="page",
            entity_id=item_id,
            properties={}
        )
        
        item_ids = ["SITE-00%s" % i for i in range(10, 100)]
  for item_id in item_ids:
        print "Set page", item_id
        client.create_event(
            event="$set",
            entity_type="page",
            entity_id=item_id,
            properties={}
        )
        



        item_idsr = ["KV-000%s" % i for i in range(1, 10)]
  for item_id in item_idsr:
        print "Set item", item_id
        client.create_event(
            event="$set",
            entity_type="item",
            entity_id=item_id,
            properties={}
        )
        count += 1

  for line in f:
        data = line.rstrip('\r\n').split(RATE_ACTIONS_DELIMITER)
        # For demonstration purpose, randomly mix in some buy events
        print "create event", data[0]
        client.create_event(
            event=data[0],
            entity_type="user",
            entity_id=data[4],
            target_entity_type="item",
            target_entity_id=data[1],
            properties= { "page" : data[3] }
        )
    
      
        count += 1
  f.close()
  print "%s events are imported." % count

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import sample data for recommendation engine")
  parser.add_argument('--access_key', default='3NwNnM8GBe9aVUGBzFqNMZdPwyK7-UdIwhnZkMmB3kT0usn7wIHmAJ7FRhjLhVBm')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="./data/sample-kbevents.csv")

  args = parser.parse_args()
  print args

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)
  import_events(client, args.file)
