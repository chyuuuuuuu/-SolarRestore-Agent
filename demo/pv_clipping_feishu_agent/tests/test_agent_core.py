import unittest

from demo.pv_clipping_feishu_agent.agent_core import (
    answer_question,
    build_monitor_snapshot,
    create_confirmation_task,
    handle_feishu_event,
    simulate_card_callback,
)
from demo.pv_clipping_feishu_agent.k8s_logs import KubernetesDashboardClient, analyze_log_text
from demo.pv_clipping_feishu_agent.online_data import build_restoration_curve_payload


class AgentCoreTest(unittest.TestCase):
    def test_snapshot_rates(self):
        snapshot = build_monitor_snapshot()
        self.assertEqual(snapshot["summary"]["covered_regions"], 13)
        self.assertEqual(snapshot["summary"]["potential_ratio"], 58.2)
        self.assertEqual(snapshot["summary"]["success_rate"], 35.46)

    def test_question_about_failures(self):
        answer = answer_question("失败最多的原因是什么？")
        self.assertIn("功率数据插值失败", answer)
        self.assertIn("重复索引", answer)

    def test_sample_feedback_and_dataset_reconstruction_are_implemented(self):
        snapshot = build_monitor_snapshot()
        feedback = snapshot["sample_feedback"]
        dataset = snapshot["dataset_reconstruction"]

        self.assertEqual(feedback["status"], "implemented")
        self.assertEqual(dataset["status"], "implemented")
        self.assertEqual(feedback["total_feedback_records"], 3262)
        self.assertEqual(feedback["trainable_records"], 2347)
        self.assertEqual(feedback["contract_blocked_records"], 187)
        self.assertEqual(dataset["splits"]["train"], 1642)
        self.assertEqual(dataset["splits"]["validation"], 352)
        self.assertEqual(dataset["splits"]["gray_eval"], 353)
        self.assertEqual(snapshot["evolution_steps"][3]["state"], "running")
        self.assertEqual(snapshot["evolution_steps"][4]["state"], "running")
        self.assertTrue(
            any(label["scene_label"] == "duplicate_timestamp_index" for label in dataset["scene_labels"])
        )

    def test_question_about_sample_feedback(self):
        answer = answer_question("高价值样本回流和数据集重构实现了吗？")
        self.assertIn("高价值样本回流已实现", answer)
        self.assertIn("数据集与场景标签重构已实现", answer)
        self.assertIn("2,347", answer)

    def test_question_about_region(self):
        answer = answer_question("Europe/Berlin 为什么风险最高？")
        self.assertIn("Europe/Berlin", answer)
        self.assertIn("9,066", answer)

    def test_feishu_challenge(self):
        response = handle_feishu_event({"challenge": "abc"})
        self.assertEqual(response, {"challenge": "abc"})

    def test_feishu_message_dry_run(self):
        payload = {
            "header": {"event_type": "im.message.receive_v1"},
            "event": {
                "message": {
                    "chat_id": "oc_demo",
                    "content": "{\"text\":\"今天状态如何\"}",
                }
            },
        }
        response = handle_feishu_event(payload)
        self.assertTrue(response["ok"])
        self.assertIn("欧洲", response["answer"])
        self.assertTrue(response["send_result"]["dry_run"])

    def test_confirmation_card_yes_flow(self):
        created = create_confirmation_task(question="是否继续？")
        task_id = created["task"]["task_id"]
        result = simulate_card_callback(task_id, "yes")
        self.assertTrue(result["ok"])
        self.assertEqual(result["signal"], "yes")
        self.assertEqual(result["task"]["status"], "answered")
        self.assertIn("updated_card", result)
        self.assertTrue(result["update_result"]["dry_run"])

    def test_confirmation_card_duplicate_after_answered(self):
        created = create_confirmation_task(question="是否继续？")
        task_id = created["task"]["task_id"]
        first = simulate_card_callback(task_id, "no")
        second = simulate_card_callback(task_id, "no")
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(second["signal"], "no")

    def test_feishu_card_action_payload(self):
        created = create_confirmation_task(question="是否继续？")
        task = created["task"]
        payload = {
            "header": {"event_id": "evt_card_action_unit"},
            "event": {
                "operator": {"operator_id": {"open_id": "ou_unit"}},
                "context": {"open_message_id": "om_unit"},
                "action": {
                    "value": {
                        "task_id": task["task_id"],
                        "card_id": task["card_id"],
                        "action": "yes",
                    }
                },
            },
        }
        response = handle_feishu_event(payload)
        self.assertTrue(response["ok"])
        self.assertEqual(response["signal"], "yes")
        self.assertEqual(response["task"]["message_id"], "om_unit")

    def test_k8s_log_issue_analysis(self):
        log_text = """
        2026-05-28 | WARNING | pv_clipping.integration | Europe/Berlin 区域削峰还原处理完成
        2026-05-28 | WARNING | pv_clipping.integration | 潜在削峰站点数: 9066
        2026-05-28 | WARNING | pv_clipping.integration | 成功处理: 2966 个
        2026-05-28 | WARNING | pv_clipping.integration | 失败/跳过: 6100 个
        2026-05-28 | ERROR | pv_clipping.power_restoration | 功率数据插值失败: cannot reindex on an axis with duplicate labels
        2026-05-28 | ERROR | pv_clipping.clipping_processor | too many values to unpack
        """
        analysis = analyze_log_text(log_text)
        self.assertEqual(analysis["severity_counts"]["ERROR"], 2)
        self.assertEqual(analysis["run_summary"]["potential_clipping_stations"], 9066)
        self.assertIn("功率数据插值失败", analysis["issue_counts"])
        self.assertIn("too many values to unpack", analysis["issue_counts"])

    def test_k8s_log_client_uses_dashboard_log_paths(self):
        class FakeClient(KubernetesDashboardClient):
            def __init__(self):
                self.calls = []

            def get_json(self, path, params=None):
                self.calls.append((path, params or {}))
                if path.startswith("api/v1/log/source/"):
                    return {"containerNames": ["sigen-pv-clipping"]}
                return {
                    "logs": [
                        {
                            "timestamp": "2026-06-14T10:00:00Z",
                            "content": "ERROR no power data",
                        }
                    ]
                }

        client = FakeClient()
        text = client.read_log("pod-1", namespace="data-platform", tail_lines=200)

        self.assertIn("ERROR no power data", text)
        self.assertEqual(client.calls[0][0], "api/v1/log/source/data-platform/pod-1/pod")
        self.assertEqual(client.calls[1][0], "api/v1/log/data-platform/pod-1/sigen-pv-clipping")
        self.assertEqual(client.calls[1][1]["logFilePosition"], "end")
        self.assertEqual(client.calls[1][1]["referenceTimestamp"], "newest")

    def test_online_restoration_curve_payload(self):
        payload = build_restoration_curve_payload(
            station_id="s1",
            date_str="2026-05-22",
            prediction_rows=[
                {
                    "model_name": "PowerRestorationModel_10000station",
                    "model_version": "0.95",
                    "record_time": 123,
                    "statistics_time": "[100, 400, 700]",
                    "predicted_value": "[0.0, 1.2, 0.4]",
                }
            ],
            observed_rows=[
                {"statistics_time": 100, "observed_power": 0.0},
                {"statistics_time": 400, "observed_power": 0.8},
                {"statistics_time": 700, "observed_power": 0.5},
            ],
        )

        curve = payload["selected_curve"]
        self.assertEqual(payload["model_count"], 1)
        self.assertEqual(curve["point_count"], 3)
        self.assertEqual(curve["stats"]["restored_peak"], 14.4)
        self.assertEqual(curve["stats"]["observed_peak"], 0.8)
        self.assertGreater(curve["stats"]["restored_gain_energy"], 0)
        self.assertEqual(curve["points"][1]["restored_power_raw"], 1.2)
        self.assertEqual(curve["points"][1]["gain"], 13.6)
        self.assertEqual(curve["stats"]["scale_mode"], "fixed_prediction_x12")
        self.assertEqual(curve["stats"]["scale_factor"], 12.0)
        self.assertEqual(curve["stats"]["extra_generation_energy"], curve["stats"]["restored_gain_energy"])
        self.assertGreater(curve["stats"]["net_energy_delta"], 0)

    def test_online_restoration_curve_uses_fixed_prediction_scale(self):
        payload = build_restoration_curve_payload(
            station_id="s1",
            date_str="2026-05-22",
            prediction_rows=[
                {
                    "model_name": "PowerRestorationModel_10000station",
                    "model_version": "0.95",
                    "record_time": 123,
                    "statistics_time": "[100, 400, 700]",
                    "predicted_value": "[0.0, 0.5, 0.4]",
                }
            ],
            observed_rows=[
                {"statistics_time": 100, "observed_power": 0.0},
                {"statistics_time": 400, "observed_power": 5.0},
                {"statistics_time": 700, "observed_power": 4.0},
            ],
        )

        curve = payload["selected_curve"]
        self.assertEqual(curve["stats"]["scale_mode"], "fixed_prediction_x12")
        self.assertEqual(curve["stats"]["scale_factor"], 12.0)
        self.assertEqual(curve["stats"]["raw_restored_peak"], 0.5)
        self.assertEqual(curve["stats"]["restored_peak"], 6.0)
        self.assertGreater(curve["stats"]["restored_gain_energy"], 0)


if __name__ == "__main__":
    unittest.main()
