use hegel::TestCase;
use hegel::generators as gs;
use std::collections::HashMap;
use underclass::models::{Account, AccountStatus, BackendId, Outcome, Surface};
use underclass::pool::{PoolCore, SelectError};
use underclass::resets::{
    Candidate, RateLimit, Window, blocked_cooling_deadline, choose_candidate, natural_recovery_ms,
};
use underclass::stats::{self, Dim};
use underclass::store::UpstreamEvent;
use underclass::store::{Store, UsageQuery, UsageRecord};
use underclass::tokens::{
    ROTATION_INTERVAL_MS, refreshes_with_issuer, rotation_due, status_after_manual_refresh,
};
use underclass::usage::{StreamEnd, TokenCounts, UsageDialect, UsageTap};

/// The surfaces the property tests route over: the OpenAI-shaped ones.
fn openai_surfaces() -> HashMap<BackendId, Vec<Surface>> {
    HashMap::from([
        (BackendId::Codex, vec![Surface::OpenAI]),
        (BackendId::Copilot, vec![Surface::OpenAI]),
        (BackendId::Claude, vec![Surface::Anthropic]),
    ])
}

#[hegel::test]
fn test_monitor_minute_bins_partition_recent_attempts(tc: TestCase) {
    let offsets: Vec<i64> = tc.draw(
        gs::vecs(
            gs::integers::<i64>()
                .min_value(-3_800_000)
                .max_value(60_000),
        )
        .max_size(40),
    );
    let now: i64 = tc.draw(
        gs::integers::<i64>()
            .min_value(-1_000_000)
            .max_value(5_000_000),
    );
    let first_minute = now.div_euclid(60_000) - 59;
    let mut expected = vec![0i64; 60];
    let store = Store::in_memory().unwrap();
    for (i, offset) in offsets.iter().enumerate() {
        let ts = now + offset;
        store
            .insert_usage(&UsageRecord {
                id: 0,
                request_id: format!("monitor-{i}"),
                ts,
                endpoint: "/v1/responses".into(),
                backend: "codex".into(),
                model: "model".into(),
                account_id: "account".into(),
                account_label: "label".into(),
                cache_key: None,
                status: 200,
                input_tokens: None,
                output_tokens: None,
                ..Default::default()
            })
            .unwrap();
        let minute = ts.div_euclid(60_000);
        if minute >= first_minute && ts <= now {
            expected[(minute - first_minute) as usize] += 1;
        }
    }
    assert_eq!(store.monitor_minute_bins(now).unwrap(), expected);
}

#[hegel::test]
fn test_monitor_account_totals_partition_month_and_unknown_usage(tc: TestCase) {
    let samples: Vec<(i64, u8, u8)> = tc.draw(
        gs::vecs(gs::tuples!(
            gs::integers::<i64>().min_value(0).max_value(150_000),
            gs::integers::<u8>().min_value(0).max_value(5),
            gs::integers::<u8>().min_value(0).max_value(200)
        ))
        .max_size(40),
    );
    let store = Store::in_memory().unwrap();
    let mut expected: HashMap<String, (i64, i64, i64, i64)> = HashMap::new();
    for (i, (ts, account, amount)) in samples.iter().enumerate() {
        let id = format!("account-{account}");
        let known = amount % 3 != 0;
        let input = known.then_some(*amount as i64);
        let output = known.then_some((*amount / 2) as i64);
        store
            .insert_usage(&UsageRecord {
                id: 0,
                request_id: format!("monitor-{i}"),
                ts: *ts,
                endpoint: "/v1/responses".into(),
                backend: "codex".into(),
                model: "model".into(),
                account_id: id.clone(),
                account_label: id.clone(),
                cache_key: None,
                status: 200,
                input_tokens: input,
                output_tokens: output,
                ..Default::default()
            })
            .unwrap();
        if (1_000..100_000).contains(ts) {
            let total = expected.entry(id).or_default();
            total.0 += 1;
            total.1 += i64::from(!known);
            total.2 += input.unwrap_or(0);
            total.3 += output.unwrap_or(0);
        }
    }
    let actual: HashMap<String, (i64, i64, i64, i64)> = store
        .monitor_account_totals(1_000, 100_000)
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.account_id.unwrap(),
                (
                    row.requests,
                    row.unknown_requests,
                    row.input_tokens,
                    row.output_tokens,
                ),
            )
        })
        .collect();
    assert_eq!(actual, expected);
}

#[hegel::test]
fn test_usage_parser_independent_of_chunk_boundaries(tc: TestCase) {
    let input: i64 = tc.draw(gs::integers::<i64>().min_value(0).max_value(1_000_000));
    let output: i64 = tc.draw(gs::integers::<i64>().min_value(0).max_value(1_000_000));
    let splits: Vec<usize> =
        tc.draw(gs::vecs(gs::integers::<usize>().min_value(1).max_value(40)).max_size(30));
    let event = format!(
        "event: response.completed\r\ndata: {{\"type\":\"response.completed\",\"response\":{{\"usage\":{{\"input_tokens\":{input},\"output_tokens\":{output}}}}}}}\r\n\r\n"
    );
    let mut tap = UsageTap::new(true, UsageDialect::Responses);
    let mut position = 0;
    for size in splits {
        if position >= event.len() {
            break;
        }
        let end = (position + size).min(event.len());
        tap.feed(&event.as_bytes()[position..end]);
        position = end;
    }
    tap.feed(&event.as_bytes()[position..]);
    assert_eq!(
        tap.counts(),
        Some(TokenCounts {
            input_tokens: input,
            output_tokens: output
        })
    );
}

#[hegel::test]
fn test_unknown_usage_never_becomes_zero(tc: TestCase) {
    let input: i64 = tc.draw(gs::integers::<i64>().min_value(-1_000_000).max_value(-1));
    let body = format!("{{\"usage\":{{\"input_tokens\":{input},\"output_tokens\":0}}}}");
    let mut tap = UsageTap::new(false, UsageDialect::Responses);
    tap.feed(body.as_bytes());
    assert_eq!(tap.counts(), None);
}

#[hegel::test]
fn test_usage_groups_partition_measured_and_unknown_attempts(tc: TestCase) {
    let samples: Vec<u8> =
        tc.draw(gs::vecs(gs::integers::<u8>().min_value(0).max_value(200)).max_size(20));
    let store = Store::in_memory().unwrap();
    let mut measured = 0i64;
    let mut unknown = 0i64;
    let mut expected_input = 0i64;
    for (i, sample) in samples.iter().enumerate() {
        let input = if sample % 2 == 0 {
            Some(*sample as i64)
        } else {
            None
        };
        if let Some(value) = input {
            measured += 1;
            expected_input += value;
        } else {
            unknown += 1;
        }
        store
            .insert_usage(&UsageRecord {
                id: 0,
                request_id: format!("request-{i}"),
                ts: i as i64,
                endpoint: "/v1/responses".into(),
                backend: "codex".into(),
                model: format!("model-{}", i % 2),
                account_id: format!("account-{}", i % 3),
                account_label: format!("label-{}", i % 3),
                cache_key: Some(format!("key-{}", i % 4)),
                status: 200,
                input_tokens: input,
                output_tokens: input,
                ..Default::default()
            })
            .unwrap();
    }
    let total = store.usage_summary(&UsageQuery::default()).unwrap();
    assert_eq!(total[0].requests, samples.len() as i64);
    assert_eq!(total[0].measured_requests, measured);
    assert_eq!(total[0].unknown_requests, unknown);
    assert_eq!(total[0].input_tokens, expected_input);
    let grouped = store
        .usage_summary(&UsageQuery {
            group_by: Some("model,account_id,cache_key".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        grouped.iter().map(|g| g.requests).sum::<i64>(),
        samples.len() as i64
    );
    assert_eq!(
        grouped.iter().map(|g| g.input_tokens).sum::<i64>(),
        expected_input
    );
}

#[hegel::test]
fn test_reset_candidate_has_latest_natural_recovery(tc: TestCase) {
    let waits: Vec<i64> = tc.draw(
        gs::vecs(gs::integers::<i64>().min_value(1).max_value(604_800_000))
            .min_size(1)
            .max_size(20),
    );
    let expected = *waits.iter().max().unwrap();
    let candidates = waits
        .into_iter()
        .enumerate()
        .map(|(i, recovery_ms)| Candidate {
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
    let expected = [
        primary_exhausted.then_some(primary * 1000),
        secondary_exhausted.then_some(secondary * 1000),
    ]
    .into_iter()
    .flatten()
    .max();
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

/// A rotation writes `token_refreshed_at`, which is what the next due check reads. Whatever the
/// clock said before, a freshly rotated account must not come due again until a full interval has
/// elapsed, and must come due again once it has.
#[hegel::test]
fn test_rotation_due_respects_interval_after_each_rotation(tc: TestCase) {
    let now: i64 = tc.draw(
        gs::integers::<i64>()
            .min_value(0)
            .max_value(1_000_000_000_000),
    );
    let mut rotated = account("acct", BackendId::Codex);
    rotated.refresh_token = Some("refresh".into());
    rotated.token_refreshed_at = now;
    assert!(!rotation_due(&rotated, now));
    assert!(!rotation_due(&rotated, now + ROTATION_INTERVAL_MS - 1));
    assert!(rotation_due(&rotated, now + ROTATION_INTERVAL_MS));
    assert!(rotation_due(&rotated, now + ROTATION_INTERVAL_MS + 1));
    // Copilot tokens are never rotated, however old they are.
    let mut copilot = account("acct", BackendId::Copilot);
    copilot.refresh_token = Some("gh".into());
    copilot.token_refreshed_at = 0;
    assert!(!rotation_due(&copilot, now + ROTATION_INTERVAL_MS));
}

/// Applying whatever the plan gate asks for settles the account: a second pass asks for nothing.
/// A free plan always ends disabled, a paid plan never ends up disabled by the gate, and an
/// operator's disable of a paid account is never reverted.
#[hegel::test]
fn test_plan_gate_settles_and_respects_operator_disables(tc: TestCase) {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use underclass::models::{FREE_PLAN_REASON, PlanGate};
    let plans = ["free", "FREE", "plus", "pro", "promax", "business"];
    let plan = plans[tc.draw(gs::integers::<usize>().max_value(plans.len() - 1))];
    let statuses = [
        AccountStatus::Healthy,
        AccountStatus::Cooling,
        AccountStatus::AuthError,
        AccountStatus::Disabled,
    ];
    let mut a = account("acct", BackendId::Codex);
    a.status = statuses[tc.draw(gs::integers::<usize>().max_value(3))];
    a.disabled_reason = tc
        .draw(gs::booleans())
        .then(|| FREE_PLAN_REASON.to_string());
    let claims = format!(r#"{{"chatgpt_plan_type":"{plan}"}}"#);
    a.access_token = Some(format!("e30.{}.sig", URL_SAFE_NO_PAD.encode(claims)));
    let operator_disabled = a.status == AccountStatus::Disabled && a.disabled_reason.is_none();

    match a.plan_gate() {
        Some(PlanGate::Disable) => {
            a.status = AccountStatus::Disabled;
            a.disabled_reason = Some(FREE_PLAN_REASON.into());
        }
        Some(PlanGate::Enable) => {
            a.status = AccountStatus::Healthy;
            a.disabled_reason = None;
        }
        None => {}
    }
    assert_eq!(a.plan_gate(), None);
    let free = plan.eq_ignore_ascii_case("free");
    if free {
        assert_eq!(a.status, AccountStatus::Disabled);
    } else if operator_disabled {
        assert_eq!(a.status, AccountStatus::Disabled);
        assert_eq!(a.disabled_reason, None);
    } else {
        assert!(a.status != AccountStatus::Disabled || a.disabled_reason.is_none());
    }
}

/// A successful manual refresh readmits only a credential-rejected account, and only when the
/// refresh was real and the plan gate would not immediately disable it again. Quota cooldowns and
/// disables are never touched, and the readmitted state is one the plan gate leaves alone.
#[hegel::test]
fn test_manual_refresh_only_recovers_auth_errors(tc: TestCase) {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use underclass::models::FREE_PLAN_REASON;
    let backends = [BackendId::Codex, BackendId::Copilot, BackendId::Claude];
    let statuses = [
        AccountStatus::Healthy,
        AccountStatus::Cooling,
        AccountStatus::AuthError,
        AccountStatus::Disabled,
    ];
    let plans = [
        None,
        Some("free"),
        Some("FREE"),
        Some("plus"),
        Some("pro"),
        Some("max"),
    ];
    let backend = backends[tc.draw(gs::integers::<usize>().max_value(backends.len() - 1))];
    let mut a = account("acct", backend);
    a.status = statuses[tc.draw(gs::integers::<usize>().max_value(statuses.len() - 1))];
    a.reset_at = tc.draw(
        gs::integers::<i64>()
            .min_value(0)
            .max_value(1_000_000_000_000),
    );
    a.disabled_reason = tc
        .draw(gs::booleans())
        .then(|| FREE_PLAN_REASON.to_string());
    let plan = plans[tc.draw(gs::integers::<usize>().max_value(plans.len() - 1))];
    if let Some(plan) = plan {
        let claims = format!(r#"{{"chatgpt_plan_type":"{plan}"}}"#);
        a.access_token = Some(format!("e30.{}.sig", URL_SAFE_NO_PAD.encode(claims)));
        a.plan = Some(plan.to_string());
    }

    match status_after_manual_refresh(&a) {
        Some(next) => {
            assert_eq!(next, AccountStatus::Healthy);
            assert_eq!(a.status, AccountStatus::AuthError);
            assert!(refreshes_with_issuer(a.backend));
            a.status = next;
            assert_eq!(a.plan_gate(), None);
        }
        None => assert!(
            a.status != AccountStatus::AuthError
                || !refreshes_with_issuer(a.backend)
                || plan.is_some_and(|p| p.eq_ignore_ascii_case("free"))
        ),
    }
}

fn account(id: &str, backend: BackendId) -> Account {
    Account {
        id: id.into(),
        backend,
        label: id.into(),
        refresh_token: None,
        access_token: None,
        expires_at: 0,
        token_refreshed_at: 0,
        account_id: None,
        residency: None,
        enterprise_url: None,
        plan: None,
        disabled_reason: None,
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
    let mut core = PoolCore::new(&store, openai_surfaces());
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
        if let Ok(sel) = built
            .core
            .select(now, Some(&key), "gpt-5.5", Surface::OpenAI)
        {
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
    let steps: Vec<u8> =
        tc.draw(gs::vecs(gs::integers::<u8>().min_value(0).max_value(2)).max_size(30));
    let mut current: Option<String> = None;
    let mut rebound: Option<String> = None;
    let mut now = 0i64;
    for step in steps {
        now += 1;
        match step {
            0 => {
                if let Ok(sel) = built
                    .core
                    .select(now, Some(key), "gpt-5.5", Surface::OpenAI)
                {
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
                if let Ok(sel) = built
                    .core
                    .select(now, Some(key), "gpt-5.5", Surface::OpenAI)
                {
                    if let Some(old) = &current {
                        if old != &sel.account_id {
                            let previous_healthy = built.core.accounts[old].account.healthy();
                            assert!(
                                !previous_healthy,
                                "binding changed while previous account was still healthy"
                            );
                            match &rebound {
                                Some(first_rebind) => {
                                    assert_eq!(*first_rebind, sel.account_id, "unstable rebinding");
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
                built.core.report(
                    &id,
                    Outcome::QuotaExhausted {
                        until_ms: now + 500,
                    },
                );
            }
            1 => {
                built.core.report(&id, Outcome::AuthFailed);
            }
            2 => {
                built.core.set_status(&id, AccountStatus::Disabled, 0);
            }
            _ => {
                built.core.report(
                    &id,
                    Outcome::QuotaExhausted {
                        until_ms: now + 100,
                    },
                );
                built.core.sweep(now + 101);
            }
        }
        if let Ok(sel) = built
            .core
            .select(now, Some("k"), "gpt-5.5", Surface::OpenAI)
        {
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
fn test_selection_excluding_never_returns_excluded(tc: TestCase) {
    let account_count: usize = tc.draw(gs::integers::<usize>().min_value(1).max_value(8));
    let mut built = build(2, account_count);
    let mask: Vec<bool> = tc.draw(
        gs::vecs(gs::booleans())
            .min_size(account_count)
            .max_size(account_count),
    );
    let sticky: bool = tc.draw(gs::booleans());
    let key = sticky.then_some("session");
    let first = built
        .core
        .select(1, key, "gpt-5.5", Surface::OpenAI)
        .expect("all accounts healthy")
        .account_id;
    let exclude: std::collections::HashSet<String> = built
        .accounts
        .iter()
        .zip(&mask)
        .filter(|(_, excluded)| **excluded)
        .map(|(id, _)| id.clone())
        .collect();
    match built
        .core
        .select_excluding(2, key, "gpt-5.5", Surface::OpenAI, &exclude)
    {
        Ok(sel) => {
            assert!(
                !exclude.contains(&sel.account_id),
                "selected excluded account"
            );
            if sticky && !exclude.contains(&first) {
                assert_eq!(
                    sel.account_id, first,
                    "sticky binding moved off a non-excluded account"
                );
            }
        }
        Err(_) => assert_eq!(
            exclude.len(),
            built.accounts.len(),
            "a healthy account was left"
        ),
    }
    let unfiltered =
        built
            .core
            .select_excluding(3, key, "gpt-5.5", Surface::OpenAI, &Default::default());
    let plain = built.core.select(3, key, "gpt-5.5", Surface::OpenAI);
    assert_eq!(
        unfiltered.map(|s| s.account_id).ok(),
        plain.map(|s| s.account_id).ok()
    );
}

#[hegel::test]
fn test_saturation_reports_minimum_reset(tc: TestCase) {
    let account_count: usize = tc.draw(gs::integers::<usize>().min_value(2).max_value(8));
    let mut built = build(1, account_count);
    let resets: Vec<i64> = tc.draw(gs::vecs(
        gs::integers::<i64>().min_value(1).max_value(10_000),
    ));
    let mut latest_by_account: std::collections::HashMap<String, i64> = Default::default();
    for (i, reset) in resets.into_iter().enumerate() {
        let Some(id) = built.accounts.get(i % built.accounts.len()).cloned() else {
            continue;
        };
        built
            .core
            .report(&id, Outcome::QuotaExhausted { until_ms: reset });
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
    match built.core.select(0, Some("k"), "gpt-5.5", Surface::OpenAI) {
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
        built
            .core
            .report(&id, Outcome::QuotaExhausted { until_ms: now });
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
        if let Ok(sel) = built.core.select(now, None, &model, Surface::OpenAI) {
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
        let _ = built
            .core
            .select(now, Some(&key), "gpt-5.5", Surface::OpenAI);
        assert!(
            built.core.bindings().len() <= underclass::pool::DEFAULT_BINDING_CAP,
            "binding cache exceeded cap"
        );
    }
    let later = now + underclass::pool::BINDING_TTL_MS + 1;
    built.core.sweep(later);
    assert!(
        built.core.bindings().is_empty(),
        "expired bindings not evicted"
    );
}

const SSE_EVENTS: [&str; 6] = [
    "event: response.created\ndata: {\"type\":\"response.created\"}\n\n",
    "event: response.output_text.delta\r\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\r\n\r\n",
    "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"Selected model is at capacity. Please try a different model.\"}}}\n\n",
    "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n",
    "data: {\"type\":\"response.incomplete\",\"response\":{\"incomplete_details\":{\"reason\":\"content_filter\"}}}\n\n",
    "data: {\"type\":\"error\",\"code\":\"cyber_policy\",\"message\":\"This request was blocked.\"}\n\n",
];

#[hegel::test]
fn test_stream_outcome_independent_of_chunk_boundaries(tc: TestCase) {
    let picks: Vec<usize> = tc.draw(
        gs::vecs(
            gs::integers::<usize>()
                .min_value(0)
                .max_value(SSE_EVENTS.len() - 1),
        )
        .max_size(8),
    );
    let splits: Vec<usize> =
        tc.draw(gs::vecs(gs::integers::<usize>().min_value(1).max_value(50)).max_size(40));
    let body: String = picks.iter().map(|i| SSE_EVENTS[*i]).collect();
    let mut whole = UsageTap::new(true, UsageDialect::Responses);
    whole.feed(body.as_bytes());
    let mut chunked = UsageTap::new(true, UsageDialect::Responses);
    let mut position = 0;
    for size in splits {
        if position >= body.len() {
            break;
        }
        let end = (position + size).min(body.len());
        chunked.feed(&body.as_bytes()[position..end]);
        position = end;
    }
    chunked.feed(&body.as_bytes()[position..]);
    for end in [StreamEnd::Eof, StreamEnd::UpstreamError, StreamEnd::Dropped] {
        let report = chunked.report(end);
        assert_eq!(report, whole.report(end));
        // Upstream message text never reaches the recorded code.
        if let Some(code) = &report.error_code {
            assert!(
                code.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.')),
                "{code}"
            );
        }
    }
    let first_terminal = picks.iter().find(|i| matches!(**i, 2..=5));
    let expected = match first_terminal {
        Some(2) => "failed",
        Some(3) => "completed",
        Some(4) => "incomplete",
        Some(5) => "failed",
        _ => "truncated",
    };
    assert_eq!(whole.report(StreamEnd::Eof).outcome.as_str(), expected);
}

#[hegel::test]
fn test_concurrency_matches_brute_force(tc: TestCase) {
    let raw: Vec<(i64, i64)> = tc.draw(
        gs::vecs(gs::tuples!(
            gs::integers::<i64>().min_value(-50).max_value(200),
            gs::integers::<i64>().min_value(1).max_value(80)
        ))
        .max_size(25),
    );
    let bucket_ms: i64 = tc.draw(gs::integers::<i64>().min_value(1).max_value(60));
    let span: i64 = tc.draw(gs::integers::<i64>().min_value(1).max_value(200));
    let intervals: Vec<(i64, i64)> = raw.iter().map(|(s, d)| (*s, s + d)).collect();
    let buckets = stats::concurrency(&intervals, 0, span, bucket_ms);
    assert_eq!(buckets.len(), stats::bucket_count(0, span, bucket_ms));
    let active_at = |t: i64| intervals.iter().filter(|(s, e)| *s <= t && t < *e).count() as i64;
    for (i, (peak, mean_milli)) in buckets.iter().enumerate() {
        let start = i as i64 * bucket_ms;
        let end = (start + bucket_ms).min(span);
        let brute_peak = (start..end).map(active_at).max().unwrap_or(0);
        let area: i64 = (start..end).map(active_at).sum();
        assert_eq!(*peak, brute_peak, "bucket {i}");
        assert_eq!(*mean_milli, area * 1000 / (end - start), "bucket {i}");
        assert!(*mean_milli <= peak * 1000);
    }
}

#[hegel::test]
fn test_stats_series_partitions_rows_and_events(tc: TestCase) {
    let rows: Vec<(i64, i64, u8, u8, u16)> = tc.draw(
        gs::vecs(gs::tuples!(
            gs::integers::<i64>().min_value(-100).max_value(3_000),
            gs::integers::<i64>().min_value(0).max_value(400),
            gs::integers::<u8>().min_value(0).max_value(5),
            gs::integers::<u8>().min_value(0).max_value(3),
            gs::integers::<u16>().min_value(0).max_value(300)
        ))
        .max_size(30),
    );
    let events: Vec<(i64, u8)> = tc.draw(
        gs::vecs(gs::tuples!(
            gs::integers::<i64>().min_value(-100).max_value(3_000),
            gs::integers::<u8>().min_value(0).max_value(6)
        ))
        .max_size(15),
    );
    let bucket_ms: i64 = tc.draw(gs::integers::<i64>().min_value(50).max_value(1_000));
    let (from, to) = (0i64, 2_500i64);
    let store = Store::in_memory().unwrap();
    let mut expected_tokens = 0i64;
    let mut expected_attempts = vec![0i64; stats::bucket_count(from, to, bucket_ms)];
    let mut requests = std::collections::HashSet::new();
    let mut sessions = std::collections::HashSet::new();
    for (ts, duration, request, model, tokens) in &rows {
        store
            .insert_usage(&UsageRecord {
                request_id: format!("r{request}"),
                ts: *ts,
                endpoint: "/v1/responses".into(),
                backend: "codex".into(),
                model: format!("m{model}"),
                account_id: "a".into(),
                account_label: "a".into(),
                cache_key: Some(format!("k{}", request % 3)),
                status: 200,
                input_tokens: Some(i64::from(*tokens)),
                output_tokens: Some(1),
                duration_ms: Some(*duration),
                ..Default::default()
            })
            .unwrap();
        if (from..to).contains(ts) {
            expected_tokens += i64::from(*tokens);
            expected_attempts[((ts - from) / bucket_ms) as usize] += 1;
            requests.insert(*request);
            sessions.insert(request % 3);
        }
    }
    let mut expected_events = vec![0i64; expected_attempts.len()];
    for (i, (ts, kind)) in events.iter().enumerate() {
        store
            .insert_upstream_events(&[UpstreamEvent {
                ts: *ts,
                source: "proxy".into(),
                kind: underclass::store::EVENT_KINDS[*kind as usize].into(),
                dedupe_key: Some(format!("e{i}")),
                ..Default::default()
            }])
            .unwrap();
        if (from..to).contains(ts) {
            expected_events[((ts - from) / bucket_ms) as usize] += 1;
        }
    }
    let series = store.stats_series(from, to, bucket_ms, Dim::Total).unwrap();
    let buckets = series
        .first()
        .map(|s| s.buckets.clone())
        .unwrap_or_default();
    let attempts: Vec<i64> = buckets.iter().map(|b| b.totals.attempts).collect();
    let event_counts: Vec<i64> = buckets
        .iter()
        .map(|b| b.totals.events.values().sum())
        .collect();
    if series.is_empty() {
        assert!(
            expected_attempts.iter().all(|n| *n == 0) && expected_events.iter().all(|n| *n == 0)
        );
    } else {
        assert_eq!(attempts, expected_attempts);
        assert_eq!(event_counts, expected_events);
        assert_eq!(
            buckets.iter().map(|b| b.totals.input_tokens).sum::<i64>(),
            expected_tokens
        );
        for b in &buckets {
            assert!(
                b.totals.sessions <= b.totals.requests && b.totals.requests <= b.totals.attempts
            );
            assert_eq!(b.totals.outcomes.values().sum::<i64>(), b.totals.attempts);
        }
    }
    let distinct = store.stats_distinct(from, to, Dim::Total).unwrap();
    let (r, k) = distinct.first().map(|(_, r, k)| (*r, *k)).unwrap_or((0, 0));
    assert_eq!((r, k), (requests.len() as i64, sessions.len() as i64));
    // Per-model series partition the total.
    let by_model = store.stats_series(from, to, bucket_ms, Dim::Model).unwrap();
    let model_tokens: i64 = by_model
        .iter()
        .flat_map(|s| s.buckets.iter())
        .map(|b| b.totals.input_tokens)
        .sum();
    assert_eq!(model_tokens, expected_tokens);
}

#[hegel::test]
fn test_daily_rollup_matches_raw_day(tc: TestCase) {
    let rows: Vec<(i64, i64, u8)> = tc.draw(
        gs::vecs(gs::tuples!(
            gs::integers::<i64>()
                .min_value(0)
                .max_value(3 * stats::DAY_MS),
            gs::integers::<i64>()
                .min_value(0)
                .max_value(5 * stats::HOUR_MS),
            gs::integers::<u8>().min_value(0).max_value(3)
        ))
        .max_size(25),
    );
    let day: i64 = tc.draw(gs::integers::<i64>().min_value(0).max_value(2));
    let store = Store::in_memory().unwrap();
    for (i, (ts, duration, account)) in rows.iter().enumerate() {
        store
            .insert_usage(&UsageRecord {
                request_id: format!("r{i}"),
                ts: *ts,
                endpoint: "/v1/responses".into(),
                backend: "codex".into(),
                model: "m".into(),
                account_id: format!("a{account}"),
                account_label: format!("label{account}"),
                status: 200,
                input_tokens: Some(i as i64),
                output_tokens: Some(1),
                duration_ms: Some(*duration),
                stream_outcome: Some("completed".into()),
                ..Default::default()
            })
            .unwrap();
    }
    store.rollup_stats_day(day, 10 * stats::DAY_MS).unwrap();
    for dim in Dim::all() {
        let raw = store
            .stats_series(
                day * stats::DAY_MS,
                (day + 1) * stats::DAY_MS,
                stats::DAY_MS,
                dim,
            )
            .unwrap();
        let rolled = store.stats_daily(day, day + 1, dim).unwrap();
        let raw: Vec<(String, stats::Totals)> = raw
            .into_iter()
            .map(|s| (s.key, s.buckets[0].totals.clone()))
            .collect();
        let rolled: Vec<(String, stats::Totals)> =
            rolled.into_iter().map(|(_, k, _, t)| (k, t)).collect();
        if dim == Dim::Total && raw.is_empty() {
            assert_eq!(
                rolled,
                vec![(stats::TOTAL_KEY.to_string(), stats::Totals::default())]
            );
        } else {
            assert_eq!(rolled, raw);
        }
    }
}

/// Selection never crosses protocols: an OpenAI-shaped request never lands on a backend that
/// only speaks the Anthropic surface, and the other way round.
#[hegel::test]
fn test_surface_partition_never_crosses_protocols(tc: TestCase) {
    let account_count: usize = tc.draw(gs::integers::<usize>().min_value(1).max_value(6));
    let now: i64 = tc.draw(gs::integers::<i64>().min_value(0).max_value(1_000_000));
    let store = Store::in_memory().unwrap();
    let mut core = PoolCore::new(&store, openai_surfaces());
    core.set_catalog(BackendId::Codex, vec!["gpt-5.5".into()]);
    core.set_catalog(BackendId::Claude, vec!["claude-opus-5-5".into()]);
    for i in 0..account_count.max(1) {
        core.insert_account(account(&format!("codex-{i}"), BackendId::Codex));
        core.insert_account(account(&format!("claude-{i}"), BackendId::Claude));
    }
    if let Ok(selection) = core.select(now, None, "gpt-5.5", Surface::OpenAI) {
        assert_ne!(
            selection.backend,
            BackendId::Claude,
            "an OpenAI-shaped request must not select a Claude account"
        );
    }
    if let Ok(selection) = core.select(now, None, "claude-opus-5-5", Surface::Anthropic) {
        assert_eq!(
            selection.backend,
            BackendId::Claude,
            "an Anthropic-shaped request must not select an OpenAI account"
        );
    }
}

/// A model-scoped cooldown blocks that model on the account and leaves the account's other
/// models eligible, and it expires on its own.
#[hegel::test]
fn test_model_cooldown_scopes_one_model(tc: TestCase) {
    let now: i64 = tc.draw(gs::integers::<i64>().min_value(0).max_value(1_000_000));
    let window: i64 = tc.draw(gs::integers::<i64>().min_value(1).max_value(100_000));
    let store = Store::in_memory().unwrap();
    let mut core = PoolCore::new(&store, openai_surfaces());
    core.set_catalog(
        BackendId::Claude,
        vec!["claude-opus-5-5".into(), "claude-sonnet-5-5".into()],
    );
    core.insert_account(account("claude-a", BackendId::Claude));

    core.report_model_quota("claude-a", "claude-opus-5-5", now + window);
    assert!(
        matches!(
            core.select(now, None, "claude-opus-5-5", Surface::Anthropic),
            Err(SelectError::Saturated { .. })
        ),
        "the cooled model is not selectable"
    );
    assert!(
        core.select(now, None, "claude-sonnet-5-5", Surface::Anthropic)
            .is_ok(),
        "the account still serves its other models"
    );
    assert!(
        core.select(
            now + window + 1,
            None,
            "claude-opus-5-5",
            Surface::Anthropic
        )
        .is_ok(),
        "the cooldown expires on its own"
    );
}

/// Chunk boundaries must not change the proof, and uncertain earlier data cannot
/// be erased by a subsequent well-formed overload event.
#[hegel::test]
fn retry_certificate_requires_an_entire_known_output_free_prefix(tc: TestCase) {
    let variant = tc.draw(gs::integers::<u8>().min_value(0).max_value(10));
    let chunk_size = tc.draw(gs::integers::<usize>().min_value(1).max_value(113));
    let prefix = match variant {
        0 => {
            "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"output\":[]}}\n\n"
        }
        1 => "data: {broken\n\n",
        2 => "data: {\"type\":\"future.output\"}\n\n",
        3 => "data: {\"type\":\"response.output_text.delta\",\"delta\":\"text\"}\n\n",
        4 => {
            "data: {\"type\":\"response.created\",\"response\":{\"output\":[{\"type\":\"function_call\"}]}}\n\n"
        }
        5 => "data: {\"type\":\"response.completed\"}\n\n",
        6 => "event: response.output_text.delta\ndata: {\"type\":\"response.created\"}\n\n",
        7 => "data: {\"type\":\"error\",\"code\":\"server_error\"}\n\n",
        8 => "data: {\"type\":\"response.created\",\"response\":null}\n\n",
        9 => {
            "data: {\"type\":\"response.created\",\"response\":{\"usage\":{\"output_tokens\":1}}}\n\n"
        }
        _ => "data: {\"type\":\"response.created\",\"response\":{\"usage\":\"unreadable\"}}\n\n",
    };
    let body = format!(
        "{prefix}event: error\ndata: {{\"type\":\"error\",\"error\":{{\"code\":\"server_is_overloaded\"}}}}\n\n"
    );
    let mut tap = UsageTap::new(true, UsageDialect::Responses);
    for chunk in body.as_bytes().chunks(chunk_size) {
        tap.feed(chunk);
    }
    assert_eq!(tap.retry_safe_overload(), variant == 0);
}

#[hegel::test]
fn retry_certificate_requires_exact_unsanitized_code(tc: TestCase) {
    let extra: String = tc.draw(gs::text().min_size(1).max_size(30));
    let raw = format!("server_is_overloaded{extra}");
    let body = format!(
        "data: {}\n\n",
        serde_json::json!({"type":"error","code":raw})
    );
    let mut tap = UsageTap::new(true, UsageDialect::Responses);
    tap.feed(body.as_bytes());
    assert!(!tap.retry_safe_overload());
}

#[test]
fn retry_certificate_does_not_recover_after_overflow_or_partial_output() {
    let mut tap = UsageTap::new(true, UsageDialect::Responses);
    let oversized = format!("data: {}\n\n", "x".repeat(1_048_577));
    tap.feed(oversized.as_bytes());
    let overload = b"data: {\"type\":\"error\",\"code\":\"server_is_overloaded\"}\n\n";
    tap.feed(overload);
    assert!(!tap.retry_safe_overload());

    let mut partial = UsageTap::new(true, UsageDialect::Responses);
    partial.feed(overload);
    assert!(partial.retry_safe_overload());
    partial.feed(b"data: {\"type\":\"response.output_text.delta\"");
    assert!(!partial.retry_safe_overload());
}

#[test]
fn uuidv7_generation_preserves_legacy_account_and_redemption_lookups() {
    let store = Store::in_memory().unwrap();
    let mut account = underclass::flows::new_account(BackendId::Codex, "legacy".to_string());
    assert_eq!(
        uuid::Uuid::parse_str(&account.id).unwrap().get_version(),
        Some(uuid::Version::SortRand)
    );
    account.id = "550e8400-e29b-41d4-a716-446655440000".to_string();
    store.upsert_account(&account);
    let redemption = "9e45fade-954c-4bbe-ad68-f52bb9e97de9";
    store.save_reset_attempt(&account.id, "old-credit", redemption);
    assert_eq!(store.get_account(&account.id).unwrap().id, account.id);
    assert_eq!(store.reset_attempt(&account.id).unwrap().1, redemption);
}

#[test]
fn accepted_anthropic_stream_is_not_replayed_even_with_same_chunk_error() {
    let mut tap = UsageTap::new(true, UsageDialect::Anthropic);
    tap.feed(b"data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1}}}\n\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\"}}\n\n");
    assert!(tap.decided());
    assert_eq!(tap.overload_before_output(), None);
    assert!(!tap.retry_safe_overload());
}
