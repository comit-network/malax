use anyhow::Result;
use clap::Parser;
use time::format_description::well_known::Rfc3339;
use time::{format_description, OffsetDateTime};

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Opts {
    /// The redis instance to connect to.
    #[clap(long)]
    redis: String,

    /// The number of past hours to fetch prices for, starting from now.
    #[clap(long, default_value = "24")]
    past_hours: u32,

    /// The redis list to push the outcomes into.
    #[clap(long, default_value = "bitmex:outcomes")]
    list: String,
}

fn main() -> Result<()> {
    let opts = Opts::parse();

    let mut outcomes = Vec::new();
    for ResultsPage { count, start } in ResultsPages::new(opts.past_hours * 60).0.iter() {
        let mut url =
            reqwest::Url::parse("https://www.bitmex.com/api/v1/instrument/compositeIndex")?;
        url.query_pairs_mut()
            .append_pair("symbol", ".BXBT") // only interested in index
            .append_pair(
                "filter",
                r#"{"symbol": ".BXBT", "timestamp.ss": "00"}"#, // per minute
            )
            .append_pair("columns", "lastPrice,timestamp") // only necessary fields
            .append_pair("reverse", "true") // latest first, allows us to go back in time via `count`
            .append_pair("count", &count.to_string()) // max entries to be returned per page
            .append_pair("start", &start.to_string()); // starting point for results

        let page_outcomes = reqwest::blocking::get(url)?
            .json::<Vec<Quote>>()?
            .into_iter()
            .map(BtcUsdBitmexOutcome::new)
            .collect::<Result<Vec<_>>>()?;

        outcomes.push(page_outcomes);
    }
    let outcomes = outcomes.concat();

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

/// Configuration of paginated results for a BitMEX API.
struct ResultsPages(Vec<ResultsPage>);

/// Structure of a page of results for a BitMEX API request.
///
/// BitMEX API requests can only return 500 elements at a time. In
/// order to access the entire result space we have to use the `count`
/// and `start` parameters to paginate.
struct ResultsPage {
    /// Number of results in the page.
    ///
    /// Maximum value of 500.
    count: u32,
    /// Index used as a starting point for the page.
    start: u32,
}

impl ResultsPages {
    /// Maximum number of results returned by BitMEX APIs per page.
    const BITMEX_MAX_RESULT_COUNT: u32 = 500;

    /// Build a configuration of paginated results based on the total
    /// number of results wanted in the request.
    fn new(n_results: u32) -> Self {
        let full_pages = n_results / Self::BITMEX_MAX_RESULT_COUNT;
        let partial_page = n_results % Self::BITMEX_MAX_RESULT_COUNT;

        let mut pages = (0..full_pages)
            .map(|i| ResultsPage {
                count: Self::BITMEX_MAX_RESULT_COUNT,
                start: i * Self::BITMEX_MAX_RESULT_COUNT,
            })
            .collect::<Vec<_>>();
        if partial_page != 0 {
            pages.push(ResultsPage {
                count: partial_page,
                start: full_pages * Self::BITMEX_MAX_RESULT_COUNT,
            });
        }

        Self(pages)
    }
}
