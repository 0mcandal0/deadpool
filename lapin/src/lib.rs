#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(
    nonstandard_style,
    rust_2018_idioms,
    rustdoc::broken_intra_doc_links,
    rustdoc::private_intra_doc_links
)]
#![forbid(non_ascii_idents, unsafe_code)]
#![warn(
    deprecated_in_future,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    unreachable_pub,
    unused_import_braces,
    unused_labels,
    unused_lifetimes,
    unused_qualifications,
    unused_results
)]

mod config;

use std::fs;

use deadpool::{async_trait, managed};
use lapin::{ConnectionProperties, Error, tcp::{TLSConfig, AMQPUriTcpExt, NativeTlsConnector}, uri::AMQPUri};

pub use lapin;
use native_tls::Certificate;

pub use self::config::{Config, ConfigError};

pub use deadpool::managed::reexports::*;
deadpool::managed_reexports!(
    "lapin",
    Manager,
    deadpool::managed::Object<Manager>,
    Error,
    ConfigError
);

/// Type alias for ['Object']
pub type Connection = managed::Object<Manager>;

type RecycleResult = managed::RecycleResult<Error>;
type RecycleError = managed::RecycleError<Error>;

/// [`Manager`] for creating and recycling [`lapin::Connection`].
///
/// [`Manager`]: managed::Manager
#[derive(Debug)]
pub struct Manager {
    addr: String,
    connection_properties: ConnectionProperties,
    cert_chain: Option<String>,
}

impl Manager {
    /// Creates a new [`Manager`] using the given AMQP address and
    /// [`lapin::ConnectionProperties`].
    #[must_use]
    pub fn new<S: Into<String>>(addr: S, connection_properties: ConnectionProperties, cert_chain: Option<String>) -> Self {
        Self {
            addr: addr.into(),
            connection_properties,
            cert_chain
        }
    }
}

#[async_trait]
impl managed::Manager for Manager {
    type Type = lapin::Connection;
    type Error = Error;

    async fn create(&self) -> Result<lapin::Connection, Error> {

        let mut tls = TLSConfig::default();

        let conn;
        if let Some(cert_chain) = &self.cert_chain {
            //tls.cert_chain = Some(cert_chain.as_str());
            
            let ca_cert = fs::read(cert_chain).unwrap();
            let root_cert = Certificate::from_pem(&ca_cert).unwrap(); 
            let uri = self.addr.as_str().parse::<AMQPUri>().unwrap();
            let res = uri.connect().and_then(|stream| {
                stream.into_native_tls(NativeTlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .add_root_certificate(root_cert).build().expect("TLS configuration failed"), &uri.authority.host)
            });
            conn = lapin::Connection::connector(self.connection_properties.clone())(uri, res).await?;
        }
        else {
            conn =
                lapin::Connection::connect_with_config(self.addr.as_str(), self.connection_properties.clone(), tls )
            //lapin::Connection::connect(self.addr.as_str(), self.connection_properties.clone())
                .await?;
        }
        Ok(conn)
    }

    async fn recycle(&self, conn: &mut lapin::Connection) -> RecycleResult {
        match conn.status().state() {
            lapin::ConnectionState::Connected => Ok(()),
            other_state => Err(RecycleError::Message(format!(
                "lapin connection is in state: {:?}",
                other_state
            ))),
        }
    }


}
