# Bitstats

A naïve implementation of a bitmap index.

## Structure

The index is broken into Partitions -> Events -> BitSet.
The idea is to store medium size events in the bitset and user bitwise operations to query results.
note: I may drop the paritions, since they can be modeled as key prefixes.

## Example

```golang
stats := bitstats.New()
stats.Add("1994-04-02", "tea", 3)

thirdJanTea, ok := stats.ValuesSet("2012-01-03", "tea")
if !ok {
    return;
}
fourthJanTea, ok := stats.ValuesSet("2012-01-04", "tea")
if !ok {
    return;
}
commonTea := roaring.FastAnd(thirdJanTea, fourthJanTea)
```
