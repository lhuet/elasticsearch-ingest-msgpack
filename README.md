## Elasticsearch ingest processor for decoding Msgpack data

Usage in an ingest pipeline:

```
    "processors": [
      {
        "msgpack": {
          "field": "message",
          "target_field": "message_decoded"
        }
      },
      ...
```

Settings:

- `field` (mandatory): Field containing the msgpack message encoded in base64 or hex
- `target_field` (optional): Field where the processor will put the decoded message (default: `msgpack_decoded`)
- `input_format`(optional): Encoded format. `base64`(default) or `hex` are supported 
