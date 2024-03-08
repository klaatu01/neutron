use std::fmt::Display;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum BrokerAddress {
    Direct { url: String },
    Proxy { url: String, proxy: String },
}

impl BrokerAddress {
    pub fn base_url(&self) -> &str {
        match self {
            BrokerAddress::Direct { url } => url,
            BrokerAddress::Proxy { url, .. } => url,
        }
    }

    pub fn is_proxy(&self) -> bool {
        match self {
            BrokerAddress::Direct { .. } => false,
            BrokerAddress::Proxy { .. } => true,
        }
    }

    pub fn get_proxy(&self) -> Option<&str> {
        match self {
            BrokerAddress::Direct { .. } => None,
            BrokerAddress::Proxy { proxy, .. } => Some(proxy),
        }
    }

    pub fn is_tls(&self) -> bool {
        self.base_url().starts_with("pulse+ssl://")
    }
}

impl Display for BrokerAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BrokerAddress::Direct { url } => write!(f, "{}", url),
            BrokerAddress::Proxy { url, proxy } => write!(f, "{} -> {}", url, proxy),
        }
    }
}
