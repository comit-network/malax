use anyhow::Result;
use clap::Clap;
use time::format_description::well_known::Rfc3339;
use time::{format_description, OffsetDateTime};

#[derive(Clap)]
struct Opts {
    /// The redis instance to connect to.
    #[clap(long)]
    redis: String,

    /// The number of past hours to fetch prices for, starting from now.
    #[clap(long, default_value = "24")]
    past_hours: u8,

    /// The redis list to push the outcomes into.
    #[clap(long, default_value = "bitmex:outcomes")]
    list: String,
}

fn main() -> Result<()> {
    let opts = Opts::parse();

    let mut url = reqwest::Url::parse("https://www.bitmex.com/api/v1/instrument/compositeIndex")?;
    url.query_pairs_mut()
        .append_pair("symbol", ".BXBT") // only interested in index
        .append_pair(
            "filter",
            r#"{"symbol": ".BXBT", "timestamp.ss": "00", "timestamp.uu": "00"}"#,  // only hourly updates
        )
        .append_pair("columns", "lastPrice,timestamp") // only necessary fields
        .append_pair("reverse", "true") // latest first, allows us to go back in time via `count`
        .append_pair("count", &opts.past_hours.to_string()); // how many hours to report

    let outcomes = reqwest::blocking::get(url)?
        .json::<Vec<Quote>>()?
        .into_iter()
        .map(BtcUsdBitmexOutcome::new)
        .collect::<Result<Vec<_>>>()?;

    let mut redis = redis::Client::open(opts.redis.as_ref())?;

    redis::cmd("RPUSH")
        .arg(&opts.list)
        .arg(&outcomes)
        .query(&mut redis)?;

    eprintln!(
        "Added {} outcomes to redis list '{}'",
        outcomes.len(),
        opts.list
    );

    Ok(())
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct BtcUsdBitmexOutcome {
    pub id: String,
    pub outcome: String,
}

impl BtcUsdBitmexOutcome {
    fn new(quote: Quote) -> Result<Self> {
        let format = format_description::parse("[year]-[month]-[day]T[hour]:[minute]:[second]")?;

        Ok(Self {
            id: format!("/BXBT/{}.price", quote.timestamp.format(&format)?),
            outcome: (quote.last_price as u64).to_string(),
        })
    }
}

impl redis::ToRedisArgs for BtcUsdBitmexOutcome {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        out.write_arg(&serde_json::to_vec(&self).expect("serialization to always work"))
    }
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Quote {
    #[serde(with = "rfc3339")]
    timestamp: OffsetDateTime,
    last_price: f64,
}

mod rfc3339 {
    use super::*;
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'a, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
    where
        D: Deserializer<'a>,
    {
        let string = String::deserialize(deserializer)?;
        let date_time = OffsetDateTime::parse(&string, &Rfc3339).map_err(D::Error::custom)?;

        Ok(date_time)
    }
}
