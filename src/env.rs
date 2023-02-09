use std::fmt;

#[derive(PartialEq)]
pub enum Env {
    Stag,
    Prod,
}

impl fmt::Display for Env {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Env::Stag => write!(f, "{}", "staging"),
            Env::Prod => write!(f, "{}", "production"),
        }
    }
}

pub fn parse_env(s: String) -> Env {
    match s.as_ref() {
        "stag" => Env::Stag,
        "staging" => Env::Stag,
        "prod" => Env::Prod,
        "production" => Env::Prod,
        _ => {
            panic!("ENV present: {s}, but not stag, staging, prod or production, panicking!")
        }
    }
}

#[derive(PartialEq)]
pub enum Network {
    Mainnet,
    Goerli,
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let str = match &self {
            Network::Mainnet => "mainnet",
            Network::Goerli => "goerli",
        };
        write!(f, "{}", str)
    }
}

pub trait ToNetwork {
    fn to_network(&self) -> Network;
}

impl ToNetwork for Env {
    fn to_network(&self) -> Network {
        match *self {
            Env::Stag => Network::Goerli,
            Env::Prod => Network::Mainnet,
        }
    }
}
