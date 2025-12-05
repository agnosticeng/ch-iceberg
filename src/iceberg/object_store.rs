use iceberg_rust::object_store::ObjectStoreBuilder;
use object_store::ObjectStoreScheme;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::path::Path;
use std::str::FromStr;
use url::Url;

pub fn parse_url_opts<I, K, V>(
    u: &Url,
    opts: I,
) -> Result<(ObjectStoreBuilder, Path), object_store::Error>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let (scheme, path) = ObjectStoreScheme::parse(u)?;
    let path = Path::parse(path)?;

    match scheme {
        ObjectStoreScheme::AmazonS3 => {
            let mut b = AmazonS3Builder::new();
            b = b.with_url(u.to_string());

            for (k, v) in opts {
                if let Ok(k) = AmazonS3ConfigKey::from_str(k.as_ref()) {
                    b = b.with_config(k, v);
                }
            }

            Ok((ObjectStoreBuilder::S3(Box::new(b)), path))
        }
        _ => Err(object_store::Error::NotImplemented),
    }
}
