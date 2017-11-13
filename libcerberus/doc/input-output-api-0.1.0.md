# I/O API for payloads created with libcerberus

**Version: 0.1.0**

This API will use BSON and JSON for the input and output.
The map input will use BSON, everything else uses JSON.

## Map

### Input

The map input is encoded using BSON.

*Fields*

* `key` - A *string* containing the key for the map operation.
* `value` - A *string* containing the value for the map operation.

### Output

*Fields*

*Note*: Duplicate keys inside partitions are allowed, and expected.

* `partitions` - A *map* of *int* partition numbers to array of objects.
    Each object has the following fields:
    * `key` - A *string* containing the intermediate key from the map operation.
    * `value` - A *string* containing a value corresponding to the intermediate key.

*Example*

```json
{
    "partitions":{
        "1":[
            {
                "key":"foo_intermediate",
                "value":"bar"
            },
            {
                "key":"foo_intermediate",
                "value":"bar"
            }
        ],
        "2":[
            {
                "key":"foo_intermediate2",
                "value":"bar"
            }
        ]
    }
}
```

## Reduce

### Input

*Fields*

* `key` - A *string* containing an intermediate key outputted from a map operation.
* `values` - An *array* of *strings* each containing an intermediate value as outputted from a map operation.

*Example*

```json
{
    "key": "foo_intermediate",
    "values": [
        "bar",
        "baz"
    ]
}
```

### Output

*Fields*

* `values` - An *array* of *strings* representing part of the final output of the map-reduce pipeline.

*Example*

```json
{
    "values": [
        "barbaz",
        "bazbar"
    ]
}
```
