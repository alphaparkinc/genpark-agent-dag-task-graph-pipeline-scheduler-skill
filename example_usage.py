from client import DAGTaskPipelineScheduler

scheduler = DAGTaskPipelineScheduler()
scheduler.add_task("ingest_docs", 120)
scheduler.add_task("extract_entities", 240, ["ingest_docs"])
scheduler.add_task("calculate_metrics", 180, ["ingest_docs"])
scheduler.add_task("synthesize_dossier", 300, ["extract_entities", "calculate_metrics"])

plan = scheduler.schedule_execution_plan()
print("=== DAG Task Pipeline Telemetry ===")
print(f"Schedulable: {plan['schedulable']} | Stages: {plan['stages_count']}")
print(f"Estimated Critical Path: {plan['estimated_critical_path_ms']} ms")
for i, stage in enumerate(plan["execution_stages"], 1):
    print(f"  Stage {i}: {stage}")
