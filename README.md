# Embedded Knowledge Recommendation Template

This PredictionIO Template is a Recommendation engine based on the Ecommerce Recommendation Engine Template.

It will recommend Knowledge Articles to customers on a website based on:
- User
- Page

Event data is pushed from the Salesforce Embedded Knowledge Demo Package to the Event Server. 

## Documentation


Please refer to http://docs.prediction.io/templates/ecommercerecommendation/quickstart/
and the Heroku Buildpack https://github.com/heroku/predictionio-buildpack for installation instructions

## Versions

### v.0.1.0

Inital version, copy from the Ecommerce Template



## Development Notes Original

### import sample data

```
$ python data/importevents.py --access_key <your_access_key>

```

### query

normal:

```
$ curl -H "Content-Type: application/json" \
-d '{
  "user" : "u1",
  "num" : 10, 
  "page" : "p1"}' \
http://localhost:8000/queries.json \
-w %{time_connect}:%{time_starttransfer}:%{time_total}
```

```
$ curl -H "Content-Type: application/json" \
-d '{
  "user" : "u1",
  "num": 10,
  "categories" : ["c4", "c3"]
}' \
http://localhost:8000/queries.json \
-w %{time_connect}:%{time_starttransfer}:%{time_total}
```

```
curl -H "Content-Type: application/json" \
-d '{
  "user" : "u1",
  "num": 10,
  "whiteList": ["i21", "i26", "i40"]
}' \
http://localhost:8000/queries.json \
-w %{time_connect}:%{time_starttransfer}:%{time_total}
```

```
curl -H "Content-Type: application/json" \
-d '{
  "user" : "u1",
  "num": 10,
  "blackList": ["i21", "i26", "i40"]
}' \
http://localhost:8000/queries.json \
-w %{time_connect}:%{time_starttransfer}:%{time_total}
```

unknown user:

```
curl -H "Content-Type: application/json" \
-d '{
  "user" : "unk1",
  "num": 10}' \
http://localhost:8000/queries.json \
-w %{time_connect}:%{time_starttransfer}:%{time_total}
```

### handle new user

new user:

```
curl -H "Content-Type: application/json" \
-d '{
  "user" : "x1",
  "num": 10}' \
http://localhost:8000/queries.json \
-w %{time_connect}:%{time_starttransfer}:%{time_total}
```

import some view events and try to get recommendation for x1 again.

```
accessKey=<YOUR_ACCESS_KEY>
```

Does not work, needs updating:

```
curl -i -X POST http://localhost:7070/events.json?accessKey=$accessKey \
-H "Content-Type: application/json" \
-d '{
  "event" : "view",
  "entityType" : "user"
  "entityId" : "x1",
  "targetEntityType" : "item",
  "targetEntityId" : "i2",
  "eventTime" : "2015-02-17T02:11:21.934Z"
}'

curl -i -X POST http://localhost:7070/events.json?accessKey=$accessKey \
-H "Content-Type: application/json" \
-d '{
  "event" : "view",
  "entityType" : "user"
  "entityId" : "x1",
  "targetEntityType" : "item",
  "targetEntityId" : "i3",
  "eventTime" : "2015-02-17T02:12:21.934Z"
}'

```

## handle unavailable items (not relevant)

Set the following items as unavailable (need to specify complete list each time when this list is changed):

```
curl -i -X POST http://localhost:7070/events.json?accessKey=$accessKey \
-H "Content-Type: application/json" \
-d '{
  "event" : "$set",
  "entityType" : "constraint"
  "entityId" : "unavailableItems",
  "properties" : {
    "items": ["i43", "i20", "i37", "i3", "i4", "i5"],
  }
  "eventTime" : "2015-02-17T02:11:21.934Z"
}'
```

Set empty list when no more items unavailable:

```
curl -i -X POST http://localhost:7070/events.json?accessKey=$accessKey \
-H "Content-Type: application/json" \
-d '{
  "event" : "$set",
  "entityType" : "constraint"
  "entityId" : "unavailableItems",
  "properties" : {
    "items": [],
  }
  "eventTime" : "2015-02-18T02:11:21.934Z"
}'
```
