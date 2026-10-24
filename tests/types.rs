use attosend::{
    config::{Config, HttpUrl, PositiveSeconds},
    model::{
        Counter, Event, FailedUnit, MessageId, OkResponse, QueueEntry, QueueState, Request,
        Response, Sample, Status, SubmitResult,
    },
};
use serde_json::{Value, json};

fn event_json() -> Value {
    json!({"key":"incident", "source":"custom", "status":"firing", "summary":"Problem", "details":""})
}

#[test]
fn notification_limits_are_enforced_by_construction_and_deserialization() {
    let valid = event_json();
    let event: Event = serde_json::from_value(valid.clone()).unwrap();
    assert_eq!(serde_json::to_value(event).unwrap(), valid);
    for (field, limit, required) in [
        ("key", 1024, true),
        ("source", 1024, false),
        ("summary", 1024, true),
        ("details", 32768, false),
    ] {
        let mut body = valid.clone();
        body[field] = json!("é".repeat(limit));
        assert!(serde_json::from_value::<Event>(body.clone()).is_ok());
        body[field] = json!("é".repeat(limit + 1));
        assert!(serde_json::from_value::<Event>(body.clone()).is_err());
        body[field] = json!("");
        assert_eq!(serde_json::from_value::<Event>(body).is_err(), required);
    }
    assert!(
        Event::new(
            String::new(),
            "custom".into(),
            Status::Firing,
            "Problem".into(),
            String::new()
        )
        .is_err()
    );
    assert!(Sample::new("sample".into(), true, String::new(), String::new()).is_err());
    assert!(
        serde_json::from_value::<Sample>(
            json!({"key":"sample", "failed":true, "summary":"Problem", "details":"x".repeat(32769)})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<Request>(
            json!({"command":"submit", "events":[valid, {"key":"bad"}]})
        )
        .is_err()
    );
}

#[test]
fn failed_units_and_identifiers_cannot_bypass_validation() {
    let unit = json!({"key":"systemd:example.service", "source":"systemd", "status":"firing", "summary":"Unit failed", "details":""});
    let parsed: FailedUnit = serde_json::from_value(unit.clone()).unwrap();
    assert_eq!(serde_json::to_value(parsed).unwrap(), unit);
    for (field, value) in [
        ("source", "manual"),
        ("key", "example.service"),
        ("status", "resolved"),
    ] {
        let mut bad = unit.clone();
        bad[field] = json!(value);
        assert!(serde_json::from_value::<FailedUnit>(bad.clone()).is_err());
        assert!(
            serde_json::from_value::<Request>(
                json!({"command":"check", "samples":[], "failed_units":[bad]})
            )
            .is_err()
        );
    }
    for bad in ["", "not-a-uuid"] {
        assert!(bad.parse::<MessageId>().is_err());
        assert!(serde_json::from_value::<Request>(json!({"command":"retry", "id":bad})).is_err());
    }
    let id = MessageId::new();
    assert_eq!(id.to_string().parse::<MessageId>().unwrap(), id);
    assert_eq!(serde_json::to_value(id).unwrap(), json!(id.to_string()));
    assert!(Counter::try_from(-1).is_err());
    assert!(serde_json::from_value::<Counter>(json!(-1)).is_err());
    assert!(serde_json::from_value::<Counter>(json!(u64::MAX)).is_err());
    assert_eq!(
        Counter::try_from(i64::MAX).unwrap().increment().get(),
        i64::MAX
    );
    assert!("unknown".parse::<QueueState>().is_err());
}

#[test]
fn configuration_validation_cannot_be_bypassed() {
    let valid = json!({"sender":"a@example.org", "recipient":"b@example.org", "host":"test"});
    let config: Config = serde_json::from_value(valid.clone()).unwrap();
    assert_eq!(config.port().get(), 9169);
    assert_eq!(config.reminder_seconds().get(), 21600);
    assert!(serde_json::from_value::<Config>(serde_json::to_value(config).unwrap()).is_ok());
    for (field, value) in [
        ("sender", json!(" ")),
        ("recipient", json!("x".repeat(1025))),
        ("host", json!("")),
        ("port", json!(0)),
        ("port", json!(65536)),
        ("database", json!("relative.db")),
        ("socket", json!("relative.sock")),
        ("reminderSeconds", json!(0)),
        ("reminderSeconds", json!(-1)),
        ("reminderSeconds", json!(u64::MAX)),
        ("resendUrl", json!("ftp://example.org")),
        ("checks", json!({"failureSeconds":0})),
        ("checks", json!({"peer":"-bad"})),
        ("checks", json!({"peer":"bad peer"})),
        ("checks", json!({"peer":""})),
        (
            "checks",
            json!({"readiness":{"api":"http://user:password@example.org"}}),
        ),
        ("unexpected", json!(true)),
    ] {
        let mut bad = valid.clone();
        bad[field] = value;
        assert!(
            serde_json::from_value::<Config>(bad).is_err(),
            "accepted invalid {field}"
        );
    }
    for url in ["file:///tmp/test", "https://user@example.org", "not a URL"] {
        assert!(HttpUrl::try_from(url.to_owned()).is_err());
    }
    let original = "http://localhost:80/ready";
    let url = HttpUrl::try_from(original.to_owned()).unwrap();
    assert_eq!(url.as_str(), original);
    assert_eq!(serde_json::to_value(url).unwrap(), original);
    assert_eq!(PositiveSeconds::try_from(i64::MAX).unwrap().get(), i64::MAX);
}

#[test]
fn wire_formats_remain_compatible() {
    let event: Event = serde_json::from_value(event_json()).unwrap();
    assert_eq!(
        serde_json::to_value(Request::Submit {
            events: vec![event]
        })
        .unwrap(),
        json!({"command":"submit", "events":[event_json()]})
    );
    for failed_units in [None, Some(vec![])] {
        let wire = serde_json::to_value(Request::Check {
            samples: vec![],
            failed_units: failed_units.clone(),
        })
        .unwrap();
        assert_eq!(
            wire,
            json!({"command":"check", "samples":[], "failed_units":failed_units})
        );
    }
    assert_eq!(
        serde_json::to_value(Response::Submit(SubmitResult { queued: 2 })).unwrap(),
        json!({"queued":2})
    );
    assert_eq!(
        serde_json::to_value(Response::Ok(OkResponse::new())).unwrap(),
        json!({"ok":true})
    );
    assert!(serde_json::from_value::<OkResponse>(json!({"ok":false})).is_err());
    let id = MessageId::new();
    let queue = Response::Queue(vec![QueueEntry {
        id,
        created: -1,
        state: QueueState::Pending,
        attempts: Counter::ZERO,
        error: None,
    }]);
    assert_eq!(
        serde_json::to_value(queue).unwrap(),
        json!([{"id":id, "created":-1, "state":"pending", "attempts":0, "error":null}])
    );
}
