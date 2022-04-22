use core::fmt::Debug;

use bonsaidb::core::{
    api::{Api, ApiName},
    schema::Qualified,
};
use serde::{Deserialize, Serialize};

#[test]
fn core() {
    #[derive(Api, Debug, Serialize, Deserialize)]
    #[api(name = "name", core = ::bonsaidb::core)]
    struct Test;

    assert_eq!(Test::name(), ApiName::private("name"));
}

#[test]
fn name_only() {
    #[derive(Api, Debug, Serialize, Deserialize)]
    #[api(name = "name")]
    struct Test;

    assert_eq!(Test::name(), ApiName::private("name"));
}
#[test]
fn name_and_authority() {
    #[derive(Api, Debug, Serialize, Deserialize)]
    #[api(name = "name", authority = "authority")]
    struct Test;

    assert_eq!(Test::name(), ApiName::new("authority", "name"));
}
#[test]
fn error() {
    #[derive(Api, Debug, Serialize, Deserialize)]
    #[api(name = "name", authority = "authority", error = String)]
    struct Test;

    assert_eq!(<Test as Api>::Error::new(), String::new());
}
#[test]
fn response() {
    #[derive(Api, Debug, Serialize, Deserialize)]
    #[api(name = "name", authority = "authority", response = String)]
    struct Test;

    assert_eq!(<Test as Api>::Response::new(), String::new());
}
