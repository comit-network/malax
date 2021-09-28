# malax
A tool for _extracting_ the BTC price from BitMex and feeding it into redis in an Olivia compatible format.

Olivia is an oracle that attestes to various events.
In order to attest to an event, it needs to be told about the event's outcome.

When run, `malax` connects to the BitMex API and extracts the hourly Bitcoin price for the given number of hours.
It then sends this price into the given Redis instance which is used by Olivia to attest to the given price.

## Usage

```bash
malax --redis redis://localhost:6379 --past-hours 24
```

## What is up with the name?

The name Olivia is derived from the word "olive".
Malaxation is a step in process of extracting oil from olives.
`malax` extracts prices from BitMex and which is a necessary step in the process of attesting to it.
