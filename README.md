# malax

A tool for _extracting_ the index prices from BitMex and feeding them into Redis in an Olivia-compatible format.

Olivia is an oracle that attests to various events.
In order to attest to an event, it needs to be told about the event's outcome.

When run, `malax` connects to the BitMex API and extracts the specified index price per minute, for the given number of hours.
It then sends this price into the given Redis instance which is used by Olivia to attest to the given price.

## Usage

To get the bitcoin index price over the last 24 hours:

```bash
malax --redis redis://localhost:6379 --past-hours 24 btc
```

To get the eth index price over the last 24 hours:

```bash
malax --redis redis://localhost:6379 --past-hours 24 eth
```

## What is up with the name?

The name Olivia is derived from the word "olive".
Malaxation is a step in process of extracting oil from olives.
`malax` extracts prices from BitMex and which is a necessary step in the process of attesting to it.
