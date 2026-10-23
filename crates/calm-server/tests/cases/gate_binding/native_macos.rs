//! Reuse the production report -> delivery -> automatic task-verify cases.
use super::*;

#[test]
fn native_macos_task_verify_succeeds_and_wakes_once() {
    gated_candidate_happy_path_wakes_planner_once();
}

#[test]
fn native_macos_task_verify_after_sample_failure_refuses_success() {
    after_sample_failure_is_gate_infra_unsampled_finalize();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_macos_task_verify_nonzero_is_red_and_next_task_succeeds() {
    let fx = fixture().await;
    fx.dispatcher.abort_event_listener_for_test();
    let (_, task, lease, candidate) =
        settled_gated_task(&fx, "native-red", gated("printf native-red; exit 23")).await;
    fx.scheduler()
        .drive_gate_for_test(task.clone())
        .await
        .unwrap();
    let gate = gate_result(&wait_gate_result(&fx, &task.id).await);
    assert!(!gate.passed, "{gate:?}");
    assert_eq!(gate.status_detail.as_deref(), Some("gate-red"));
    assert_eq!(gate.exit_code, Some(23));
    assert!(gate.log_tail.contains("native-red"));
    assert_eq!(
        current(&fx.boot, "native-red").await.status,
        TaskStatus::Failed
    );
    assert_native_settled(&fx, &task).await;
    let lease_state: String =
        sqlx::query_scalar("SELECT state FROM workspace_leases WHERE lease_id=?")
            .bind(&lease.lease_id)
            .fetch_one(&fx.pool())
            .await
            .unwrap();
    assert_eq!(lease_state, "released");
    let first_log = std::fs::read_to_string(&gate.log_path).unwrap();
    assert!(first_log.contains("native-red"));

    // Reuse the same supervisor, database, track and checkout. The production
    // lease admission must succeed after the first report/delivery/gate settles.
    let (_, next, next_lease, next_candidate) =
        settled_gated_task(&fx, "native-next", gated("printf native-next")).await;
    assert_eq!(task.track_id, fx.track());
    assert_eq!(next.track_id, task.track_id);
    assert_ne!(next.id, task.id);
    assert_eq!(next_lease.path, lease.path);
    assert_eq!(next_lease.path, fx.worktree);
    assert_ne!(next_lease.lease_id, lease.lease_id);
    assert_eq!(next_lease.base_sha, candidate.commit_sha);
    assert_ne!(next_candidate.candidate_id, candidate.candidate_id);
    assert_ne!(next_candidate.commit_sha, candidate.commit_sha);
    fx.scheduler()
        .drive_gate_for_test(next.clone())
        .await
        .unwrap();
    let next_gate = gate_result(&wait_gate_result(&fx, &next.id).await);
    assert!(next_gate.passed, "{next_gate:?}");
    assert_eq!(next_gate.status_detail, None);
    assert_eq!(next_gate.exit_code, Some(0));
    assert_eq!(
        current(&fx.boot, "native-next").await.status,
        TaskStatus::Done
    );
    assert_native_settled(&fx, &next).await;
    for (attempt, verdict, saved, checkout) in [
        (&task, &gate, &candidate, &lease),
        (&next, &next_gate, &next_candidate, &next_lease),
    ] {
        assert_eq!(verdict.task_id, attempt.id);
        assert_eq!(verdict.attempt, 1);
        assert_eq!(gate_result_events(&fx, &attempt.id).await.len(), 1);
        let (id, sha, lease_id, evidence) = verdict.candidate();
        assert_eq!(id, saved.candidate_id);
        assert_eq!(sha, saved.commit_sha);
        assert_eq!(lease_id, checkout.lease_id);
        assert!(matches!(evidence, VerifyTargetEvidence::Verified { .. }));
    }
    let first_op = gate_op(&fx, &task.id).await.unwrap();
    let next_op = gate_op(&fx, &next.id).await.unwrap();
    assert_ne!(first_op.id, next_op.id);
    assert_ne!(
        first_op.tx_output.unwrap().data["gate_execution"]["execution_id"],
        next_op.tx_output.unwrap().data["gate_execution"]["execution_id"]
    );
    assert_ne!(next_gate.log_path, gate.log_path);
    let next_log = std::fs::read_to_string(&next_gate.log_path).unwrap();
    assert!(next_log.contains("native-next"));
    assert!(!next_log.contains("native-red"));
    assert_eq!(std::fs::read_to_string(&gate.log_path).unwrap(), first_log);
    assert_eq!(
        current(&fx.boot, "native-red").await.status,
        TaskStatus::Failed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_macos_task_verify_timeout_waits_for_settlement() {
    let fx = fixture().await;
    fx.dispatcher.abort_event_listener_for_test();
    let mut gate = gated("exec sleep 30");
    gate["gate"]["timeout_secs"] = json!(1);
    let (_, task, _, _) = settled_gated_task(&fx, "native-verify-timeout", gate).await;
    fx.scheduler()
        .drive_gate_for_test(task.clone())
        .await
        .unwrap();
    let result = gate_result(&wait_gate_result(&fx, &task.id).await);
    assert!(!result.passed, "{result:?}");
    assert_eq!(result.status_detail.as_deref(), Some("gate-timeout"));
    assert_native_settled(&fx, &task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_macos_task_verify_root_first_settles_before_after_sample() {
    let fx = fixture().await;
    fx.dispatcher.abort_event_listener_for_test();
    let (_, task, _, _) = settled_gated_task(
        &fx,
        "native-verify-root-first",
        gated("sleep 30 & printf 'child=%s\\n' $!"),
    )
    .await;
    fx.scheduler()
        .drive_gate_for_test(task.clone())
        .await
        .unwrap();
    let result = gate_result(&wait_gate_result(&fx, &task.id).await);
    assert!(result.passed, "{result:?}");
    assert!(matches!(
        result.candidate().3,
        VerifyTargetEvidence::Verified { .. }
    ));
    assert_native_settled(&fx, &task).await;
}

async fn assert_native_settled(fx: &Fx, task: &Task) {
    let op = gate_op(fx, &task.id).await.unwrap();
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
