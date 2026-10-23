//! Native replay uses the same production tool and fixtures as the platform-neutral cases.
use super::*;

#[test]
fn native_macos_nonzero_step_keeps_attempt_running() {
    a_red_step_returns_its_name_and_tail();
}

#[test]
fn native_macos_cancel_keeps_checkout_busy_until_run_and_delivery_end() {
    a_cancel_during_a_run_keeps_the_checkout_busy_until_it_ends();
}

async fn assert_native_settlement(fx: &Fx, task: &str) {
    let op = fx
        .runtime
        .find_by_kind_and_idempotency(TASK_GATE_RUN_KIND, &format!("{task}#r1"))
        .await
        .unwrap()
        .unwrap();
    let id = op.tx_output.as_ref().unwrap().data["gate_execution"]["execution_id"]
        .as_str()
        .unwrap();
    let settlement: String =
        sqlx::query_scalar("SELECT settlement_json FROM workspace_executions WHERE execution_id=?")
            .bind(id)
            .fetch_one(&fx.pool())
            .await
            .unwrap();
    let settlement: workspace_runtime::execution::Settlement =
        serde_json::from_str(&settlement).unwrap();
    assert!(
        matches!(
            settlement,
            workspace_runtime::execution::Settlement::Journal(
                workspace_runtime::execution::ExecutionState::OriginalGroupEmpty { .. }
            )
        ),
        "{settlement:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_macos_timeout_stops_execution() {
    let fx = world(LONG_IDLE).await;
    let worker = fx.new_worker("native-timeout", AgentProvider::Codex).await;
    let lease = fx.kernel_lease(&worker.card_id).await;
    let task = fx.running_task("native-timeout", "codex", &worker.card_id,
        json!({"gate": {"steps": [{"name":"wait", "cmd":"exec sleep 30"}], "timeout_secs":1}, "no_gate_reason":null}))
        .await;
    let a = Attempt {
        worker,
        lease,
        task,
    };
    let answer = gate_run(&fx, &a, Some("test: native timeout"))
        .await
        .unwrap();
    assert_eq!(answer["passed"], false, "{answer}");
    assert_eq!(answer["status_detail"], "gate-timeout", "{answer}");
    assert_native_settlement(&fx, &a.task.id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_macos_root_first_has_original_group_settlement_before_after_sample() {
    let fx = world(LONG_IDLE).await;
    let a = running(
        &fx,
        "native-root-first",
        json!([{
            "name":"background", "cmd":"sleep 30 & printf 'same-group-child=%s\\n' $!"
        }]),
    )
    .await;
    let answer = gate_run(&fx, &a, Some("test: native original group"))
        .await
        .unwrap();
    passed(&answer);
    assert_native_settlement(&fx, &a.task.id).await;
    let result = run_result(&fx, &a.task.id, 1).await;
    assert!(result.verdict.log_tail.contains("same-group-child="));
    assert!(
        matches!(result.evidence, Some(VerifyTargetEvidence::Verified { .. })),
        "{result:?}"
    );
}
