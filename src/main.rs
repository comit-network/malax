use anyhow::Result;
use clap::Clap;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

#[derive(Clap)]
struct Opts {
    /// The redis instance to connect to.
    #[clap(long)]
    redis: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();

    let client = redis::Client::open(opts.redis.as_ref())?;
    let mut con = client.get_async_connection().await?;

    let now = OffsetDateTime::now_utc();
    let yesterday = now - time::Duration::DAY;

    let mut url = reqwest::Url::parse("https://www.bitmex.com/api/v1/quote/bucketed?binSize=1h&partial=false&symbol=XBT&count=100&reverse=false")?;
    url.query_pairs_mut()
        .append_pair("startTime", &yesterday.format(&Rfc3339)?);

    let quotes = reqwest::get(url).await?.json::<Vec<Quote>>().await?;

    for quote in quotes {
        redis::cmd("RPUSH")
            .arg("outcomes")
            .arg(serde_json::to_string(&WireEventOutcome {
                event_id: "todo".to_owned(), // TODO: create correct event id (needs bid and ask)
                outcome: quote.ask_price.to_string(), // TODO: format price to cents
                time: quote.timestamp,
            })?)
            .query_async(&mut con)
            .await?;
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct WireEventOutcome {
    #[serde(rename = "id")]
    pub event_id: String,
    pub outcome: String,
    #[serde(with = "rfc3339")]
    pub time: OffsetDateTime,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Quote {
    #[serde(with = "rfc3339")]
    timestamp: OffsetDateTime,
    bid_price: f64,
    ask_price: f64,
}

mod rfc3339 {
    use serde::de::Error as _;
    use serde::ser::Error as _;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serializer;
    use time::format_description::well_known::Rfc3339;
    use time::OffsetDateTime;

    pub fn serialize<S: Serializer>(
        datetime: &OffsetDateTime,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let string = datetime.format(&Rfc3339).map_err(S::Error::custom)?;

        serializer.serialize_str(&string)
    }

    pub fn deserialize<'a, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
    where
        D: Deserializer<'a>,
    {
        let string = String::deserialize(deserializer)?;
        let date_time = OffsetDateTime::parse(&string, &Rfc3339).map_err(D::Error::custom)?;

        Ok(date_time)
    }
}
