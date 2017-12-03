# Sprint 3 Demo Payload

The sprint 3 demo involves performing a distributed grep on the top 100 project
gutenberg books to ouptut all lines that include a word 15+ characters long.

## Required files

Download the payload of the top 100 project gutenberg books from
https://drive.google.com/file/d/1GFyiUnVdsP2MaZ-y4QvhdisWEMPOEcg8/view?usp=sharing

## Inputs

### Map

* `key` - The payload doesn't require the key to be a specific format. Up to
  the servers to decide. The payload will group the output based on the input key.
* `value` - The contents of the file to grep.

### Reduce

* `key` - The same input key that was given to the map operation.
* `values` - All of the values associated with that key, as produced from the
  map stage.
