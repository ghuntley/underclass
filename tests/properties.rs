use hegel::generators as gs;
use hegel::TestCase;
use underclass::models::{Account, AccountStatus, BackendId, Outcome};
use underclass::pool::{PoolCore, SelectError};
use underclass::store::{Store, UsageQuery, UsageRecord};
use underclass::resets::{Candidate, RateLimit, Window, blocked_cooling_deadline, choose_candidate, natural_recovery_ms};
use underclass::usage::{TokenCounts, UsageTap};
use std::collections::HashMap;

#[hegel::test]
fn test_monitor_minute_bins_partition_recent_attempts(tc: TestCase) {
    let offsets: Vec<i64> = tc.draw(gs::vecs(gs::integers::<i64>().min_value(-3_800_000).max_value(60_000)).max_size(40));
    let now: i64 = tc.draw(gs::integers::<i64>().min_value(-1_000_000).max_value(5_000_000));
    let first_minute = now.div_euclid(60_000) - 59;
    let mut expected = vec![0i64; 60];
    let store = Store::in_memory().unwrap();
    for (i, offset) in offsets.iter().enumerate() {
        let ts = now + offset;
        store.insert_usage(&UsageRecord {
            id: 0, request_id: format!("monitor-{i}"), ts, endpoint: "/v1/responses".into(),
            backend: "codex".into(), model: "model".into(), account_id: "account".into(),
            account_label: "label".into(), cache_key: None, status: 200,
            input_tokens: None, output_tokens: None,
        }).unwrap();
        let minute = ts.div_euclid(60_000);
        if minute >= first_minute && ts <= now {
            expected[(minute - first_minute) as usize] += 1;
        }
    }
    assert_eq!(store.monitor_minute_bins(now).unwrap(), expected);
}

#[hegel::test]
fn test_monitor_account_totals_partition_month_and_unknown_usage(tc: TestCase) {
    let samples: Vec<(i64, u8, u8)> = tc.draw(gs::vecs(gs::tuples!(
        gs::integers::<i64>().min_value(0).max_value(150_000),
        gs::integers::<u8>().min_value(0).max_value(5),
        gs::integers::<u8>().min_value(0).max_value(200)
    )).max_size(40));
    let store = Store::in_memory().unwrap();
    let mut expected: HashMap<String, (i64, i64, i64, i64)> = HashMap::new();
    for (i, (ts, account, amount)) in samples.iter().enumerate() {
        let id = format!("account-{account}");
        let known = amount % 3 != 0;
        let input = known.then_some(*amount as i64);
        let output = known.then_some((*amount / 2) as i64);
        store.insert_usage(&UsageRecord {
            id: 0, request_id: format!("monitor-{i}"), ts: *ts, endpoint: "/v1/responses".into(),
            backend: "codex".into(), model: "model".into(), account_id: id.clone(),
            account_label: id.clone(), cache_key: None, status: 200,
            input_tokens: input, output_tokens: output,
        }).unwrap();
        if (1_000..100_000).contains(ts) {
            let total = expected.entry(id).or_default();
            total.0 += 1;
            total.1 += i64::from(!known);
            total.2 += input.unwrap_or(0);
            total.3 += output.unwrap_or(0);
        }
    }
    let actual: HashMap<String, (i64, i64, i64, i64)> = store.monitor_account_totals(1_000, 100_000)
        .unwrap().into_iter().map(|row| (row.account_id.unwrap(),
            (row.requests, row.unknown_requests, row.input_tokens, row.output_tokens))).collect();
    assert_eq!(actual, expected);
}

#[hegel::test]
fn test_usage_parser_independent_of_chunk_boundaries(tc: TestCase) {
    let input: i64 = tc.draw(gs::integers::<i64>().min_value(0).max_value(1_000_000));
    let output: i64 = tc.draw(gs::integers::<i64>().min_value(0).max_value(1_000_000));
    let splits: Vec<usize> = tc.draw(gs::vecs(gs::integers::<usize>().min_value(1).max_value(40)).max_size(30));
    let event = format!("event: response.completed\r\ndata: {{\"type\":\"response.completed\",\"response\":{{\"usage\":{{\"input_tokens\":{input},\"output_tokens\":{output}}}}}}}\r\n\r\n");
    let mut tap = UsageTap::new(true, false);
    let mut position = 0;
    for size in splits {
        if position >= event.len() { break; }
        let end = (position + size).min(event.len());
        tap.feed(&event.as_bytes()[position..end]);
        position = end;
    }
    tap.feed(&event.as_bytes()[position..]);
    assert_eq!(tap.counts(), Some(TokenCounts { input_tokens: input, output_tokens: output }));
}

#[hegel::test]
fn test_unknown_usage_never_becomes_zero(tc: TestCase) {
    let input: i64 = tc.draw(gs::integers::<i64>().min_value(-1_000_000).max_value(-1));
    let body = format!("{{\"usage\":{{\"input_tokens\":{input},\"output_tokens\":0}}}}");
    let mut tap = UsageTap::new(false, false);
    tap.feed(body.as_bytes());
    assert_eq!(tap.counts(), None);
}

#[hegel::test]
fn test_usage_groups_partition_measured_and_unknown_attempts(tc: TestCase) {
    let samples: Vec<u8> = tc.draw(gs::vecs(gs::integers::<u8>().min_value(0).max_value(200)).max_size(20));
    let store = Store::in_memory().unwrap();
    let mut measured = 0i64;
    let mut unknown = 0i64;
    let mut expected_input = 0i64;
    for (i, sample) in samples.iter().enumerate() {
        let input = if sample % 2 == 0 { Some(*sample as i64) } else { None };
        if let Some(value) = input { measured += 1; expected_input += value; } else { unknown += 1; }
        store.insert_usage(&UsageRecord {
            id: 0, request_id: format!("request-{i}"), ts: i as i64, endpoint: "/v1/responses".into(), backend: "codex".into(), model: format!("model-{}", i % 2), account_id: format!("account-{}", i % 3), account_label: format!("label-{}", i % 3), cache_key: Some(format!("key-{}", i % 4)), status: 200, input_tokens: input, output_tokens: input,
        }).unwrap();
    }
    let total = store.usage_summary(&UsageQuery::default()).unwrap();
    assert_eq!(total[0].requests, samples.len() as i64);
    assert_eq!(total[0].measured_requests, measured);
    assert_eq!(total[0].unknown_requests, unknown);
    assert_eq!(total[0].input_tokens, expected_input);
    let grouped = store.usage_summary(&UsageQuery { group_by: Some("model,account_id,cache_key".into()), ..Default::default() }).unwrap();
    assert_eq!(grouped.iter().map(|g| g.requests).sum::<i64>(), samples.len() as i64);
    assert_eq!(grouped.iter().map(|g| g.input_tokens).sum::<i64>(), expected_input);
}

#[hegel::test]
fn test_reset_candidate_has_latest_natural_recovery(tc: TestCase) {
    let waits: Vec<i64> = tc.draw(gs::vecs(gs::integers::<i64>().min_value(1).max_value(604_800_000)).min_size(1).max_size(20));
    let expected = *waits.iter().max().unwrap();
    let candidates = waits.into_iter().enumerate().map(|(i, recovery_ms)| Candidate {
        account_id: format!("account-{i}"),
        recovery_ms,
        credit_id: format!("credit-{i}"),
        credit_expiry_ms: None,
    });
    assert_eq!(choose_candidate(candidates).unwrap().recovery_ms, expected);
}

#[hegel::test]
fn test_recovery_is_latest_exhausted_window(tc: TestCase) {
    let primary: i64 = tc.draw(gs::integers::<i64>().min_value(1).max_value(500_000));
    let secondary: i64 = tc.draw(gs::integers::<i64>().min_value(1).max_value(500_000));
    let primary_exhausted = tc.draw(gs::integers::<u8>().min_value(0).max_value(1)) == 1;
    let secondary_exhausted = tc.draw(gs::integers::<u8>().min_value(0).max_value(1)) == 1;
    let make_window = |deadline: i64, exhausted: bool| Window {
        used_percent: if exhausted { 100.0 } else { 40.0 },
        reset_at: Some(deadline),
        limit_window_seconds: None,
    };
    let limit = RateLimit {
        allowed: Some(false),
        limit_reached: Some(true),
        primary_window: Some(make_window(primary, primary_exhausted)),
        secondary_window: Some(make_window(secondary, secondary_exhausted)),
    };
    let expected = [primary_exhausted.then_some(primary * 1000), secondary_exhausted.then_some(secondary * 1000)]
        .into_iter().flatten().max();
    assert_eq!(natural_recovery_ms(&limit, 0), expected);
}

#[hegel::test]
fn test_blocked_usage_never_shortens_cooling(tc: TestCase) {
    let current: i64 = tc.draw(gs::integers::<i64>().min_value(1).max_value(1_000_000));
    let recovery: i64 = tc.draw(gs::integers::<i64>().min_value(1).max_value(1_000_000));
    let result = blocked_cooling_deadline(AccountStatus::Cooling, current, recovery);
    assert!(result.is_none_or(|deadline| deadline >= current && deadline == recovery));
    assert_eq!(
        blocked_cooling_deadline(AccountStatus::Disabled, current, recovery),
        None
    );
    assert_eq!(
        blocked_cooling_deadline(AccountStatus::AuthError, current, recovery),
        None
    );
}

fn account(id: &str, backend: BackendId) -> Account {
    Account {
        id: id.into(),
        backend,
        label: id.into(),
        refresh_token: None,
        access_token: None,
        expires_at: 0,
        account_id: None,
        residency: None,
        enterprise_url: None,
        status: AccountStatus::Healthy,
        reset_at: 0,
        created_at: 0,
        updated_at: 0,
    }
}

struct Built {
    core: PoolCore,
    accounts: Vec<String>,
}

fn build(backend_spread: usize, account_count: usize) -> Built {
    let store = Store::in_memory().unwrap();
    let mut core = PoolCore::new(&store);
    let models: Vec<String> = vec!["gpt-5.5".into(), "gpt-4.1".into()];
    core.set_catalog(BackendId::Codex, models.clone());
    if backend_spread > 1 {
        core.set_catalog(BackendId::Copilot, models.clone());
    }
    let mut accounts = Vec::new();
    for i in 0..account_count.max(1) {
        let backend = if backend_spread > 1 && i % backend_spread != 0 {
            BackendId::Copilot
        } else {
            BackendId::Codex
        };
        let id = format!("acc-{i}");
        core.insert_account(account(&id, backend));
        accounts.push(id);
    }
    Built { core, accounts }
}

#[hegel::test]
fn test_stickiness_is_stable_while_healthy(tc: TestCase) {
    let account_count: usize = tc.draw(gs::integers::<usize>().min_value(1).max_value(6));
    let mut built = build(2, account_count);
    let requests: Vec<String> = tc.draw(gs::vecs(gs::text()).max_size(20));
    let mut bindings: std::collections::HashMap<String, String> = Default::default();
    let mut now = 0i64;
    for key in requests {
        if key.is_empty() {
            continue;
        }
        now += 1;
        if let Ok(sel) = built.core.select(now, Some(&key), "gpt-5.5") {
            match bindings.get(&key) {
                Some(previous) => {
                    assert_eq!(
                        *previous, sel.account_id,
                        "sticky binding changed without health change"
                    );
                }
                None => {
                    bindings.insert(key, sel.account_id.clone());
                }
            }
        }
    }
}

#[hegel::test]
fn test_rebinding_stays_stable_after_rebind(tc: TestCase) {
    let account_count: usize = tc.draw(gs::integers::<usize>().min_value(2).max_value(6));
    let mut built = build(2, account_count);
    let key = "session-key";
    let steps: Vec<u8> = tc.draw(gs::vecs(gs::integers::<u8>().min_value(0).max_value(2)).max_size(30));
    let mut current: Option<String> = None;
    let mut rebound: Option<String> = None;
    let mut now = 0i64;
    for step in steps {
        now += 1;
        match step {
            0 => {
                if let Ok(sel) = built.core.select(now, Some(key), "gpt-5.5") {
                    current = Some(sel.account_id);
                }
            }
            1 => {
                if let Some(id) = &current {
                    built.core.report(
                        id,
                        Outcome::QuotaExhausted {
                            until_ms: now + 100_000,
                        },
                    );
                    rebound = None;
                }
            }
            _ => {
                if let Ok(sel) = built.core.select(now, Some(key), "gpt-5.5") {
                    if let Some(old) = &current {
                        if old != &sel.account_id {
                            let previous_healthy = built.core.accounts[old].account.healthy();
                            assert!(
                                !previous_healthy,
                                "binding changed while previous account was still healthy"
                            );
                            match &rebound {
                                Some(first_rebind) => {
                                    assert_eq!(
                                        *first_rebind, sel.account_id,
                                        "unstable rebinding"
                                    );
                                }
                                None => rebound = Some(sel.account_id.clone()),
                            }
                        }
                    }
                    current = Some(sel.account_id);
                }
            }
        }
    }
}

#[hegel::test]
fn test_selection_never_returns_unhealthy(tc: TestCase) {
    let account_count: usize = tc.draw(gs::integers::<usize>().min_value(1).max_value(8));
    let mut built = build(2, account_count);
    let events: Vec<(usize, u8)> = tc.draw(gs::vecs(gs::tuples!(
        gs::integers::<usize>().min_value(0).max_value(7),
        gs::integers::<u8>().min_value(0).max_value(3)
    )));
    let mut now = 0i64;
    for (index, kind) in events {
        now += 1;
        let Some(id) = built.accounts.get(index % built.accounts.len()).cloned() else {
            continue;
        };
        match kind {
            0 => {
                built
                    .core
                    .report(&id, Outcome::QuotaExhausted { until_ms: now + 500 });
            }
            1 => {
                built.core.report(&id, Outcome::AuthFailed);
            }
            2 => {
                built.core.set_status(&id, AccountStatus::Disabled, 0);
            }
            _ => {
                built.core.report(&id, Outcome::QuotaExhausted { until_ms: now + 100 });
                built.core.sweep(now + 101);
            }
        }
        if let Ok(sel) = built.core.select(now, Some("k"), "gpt-5.5") {
            let state = &built.core.accounts[&sel.account_id];
            assert!(
                state.account.healthy(),
                "selected unhealthy account {:?}",
                state.account.status
            );
        }
    }
}

#[hegel::test]
fn test_saturation_reports_minimum_reset(tc: TestCase) {
    let account_count: usize = tc.draw(gs::integers::<usize>().min_value(2).max_value(8));
    let mut built = build(1, account_count);
    let resets: Vec<i64> = tc.draw(gs::vecs(gs::integers::<i64>().min_value(1).max_value(10_000)));
    let mut latest_by_account: std::collections::HashMap<String, i64> = Default::default();
    for (i, reset) in resets.into_iter().enumerate() {
        let Some(id) = built.accounts.get(i % built.accounts.len()).cloned() else {
            continue;
        };
        built.core.report(&id, Outcome::QuotaExhausted { until_ms: reset });
        latest_by_account.insert(id, reset);
    }
    if latest_by_account.is_empty() {
        return;
    }
    let expected = latest_by_account.values().copied().min().unwrap();
    for id in &built.accounts {
        if built.core.accounts[id].account.healthy() {
            built
                .core
                .report(id, Outcome::QuotaExhausted { until_ms: expected });
        }
    }
    match built.core.select(0, Some("k"), "gpt-5.5") {
        Err(SelectError::Saturated { until_ms }) => assert_eq!(until_ms, expected),
        other => panic!("expected saturated, got {other:?}"),
    }
}

#[hegel::test]
fn test_inflight_stays_nonnegative(tc: TestCase) {
    let account_count: usize = tc.draw(gs::integers::<usize>().min_value(1).max_value(5));
    let mut built = build(2, account_count);
    let ops: Vec<(usize, u8)> = tc.draw(gs::vecs(gs::tuples!(
        gs::integers::<usize>().min_value(0).max_value(4),
        gs::integers::<u8>().min_value(0).max_value(1)
    )));
    for (index, op) in ops {
        let Some(id) = built.accounts.get(index % built.accounts.len()).cloned() else {
            continue;
        };
        if op == 0 {
            built.core.acquire(&id);
        } else {
            built.core.release(&id);
        }
        assert!(built.core.accounts[&id].inflight < u32::MAX);
    }
}

#[hegel::test]
fn test_cooling_expiry_restores_health(tc: TestCase) {
    let account_count: usize = tc.draw(gs::integers::<usize>().min_value(1).max_value(6));
    let mut built = build(1, account_count);
    let events: Vec<(usize, i64)> = tc.draw(gs::vecs(gs::tuples!(
        gs::integers::<usize>().min_value(0).max_value(5),
        gs::integers::<i64>().min_value(1).max_value(1000)
    )));
    let mut now = 0i64;
    for (index, cooldown) in events {
        now += cooldown;
        let Some(id) = built.accounts.get(index % built.accounts.len()).cloned() else {
            continue;
        };
        built.core.report(&id, Outcome::QuotaExhausted { until_ms: now });
        built.core.sweep(now);
        let state = &built.core.accounts[&id];
        assert!(
            state.account.healthy(),
            "account still cooling after reset passed"
        );
    }
}

#[hegel::test]
fn test_unknown_models_still_routable_via_codex(tc: TestCase) {
    let account_count: usize = tc.draw(gs::integers::<usize>().min_value(1).max_value(6));
    let mut built = build(2, account_count);
    let unknown: Vec<String> = tc.draw(gs::vecs(gs::text()).max_size(15));
    let mut now = 0i64;
    for model in unknown {
        if model.is_empty() {
            continue;
        }
        now += 1;
        if let Ok(sel) = built.core.select(now, None, &model) {
            assert_eq!(
                sel.backend,
                BackendId::Codex,
                "unknown model '{model}' routed to non-codex backend"
            );
        }
    }
}

#[hegel::test]
fn test_bindings_respect_ttl_and_cap(tc: TestCase) {
    let account_count: usize = tc.draw(gs::integers::<usize>().min_value(1).max_value(4));
    let mut built = build(1, account_count);
    let mut now = 0i64;
    let keys: Vec<String> = tc.draw(gs::vecs(gs::text()).max_size(30));
    for key in keys {
        if key.is_empty() {
            continue;
        }
        now += 3_600_000;
        let _ = built.core.select(now, Some(&key), "gpt-5.5");
        assert!(
            built.core.bindings().len() <= underclass::pool::DEFAULT_BINDING_CAP,
            "binding cache exceeded cap"
        );
    }
    let later = now + underclass::pool::BINDING_TTL_MS + 1;
    built.core.sweep(later);
    assert!(built.core.bindings().is_empty(), "expired bindings not evicted");
}
