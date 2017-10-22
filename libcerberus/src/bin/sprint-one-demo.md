# Sprint 1 Demo Payload

## Required files

https://tbolt.redbrick.dcu.ie/shakes/ contains three of Shakespeare's plays,
and their stripped versions. These stripped versions still include newline and
carriage return characters (`\r\n`). Also included is a script to re-strip the
plays, and a file containing word counts of all of the plays, as outputted by
`wc -w`.

## Inputs

### Map

* `key` - The payload doesn't require the key to be a specific format. Up to
  the servers to decide.
* `value` - The contents of the file to count words from. Must be stripped to
  only include alphabetic characters and apostrophes. Newline or carriage
  return characters will invalidate the JSON. That shouldn't be a problem if
  the JSON is generated with serde.

### Reduce

* `key` - A single word to reduce the counts for.
* `values` - All of the values associated with that key, as produced from the
  map stage.
