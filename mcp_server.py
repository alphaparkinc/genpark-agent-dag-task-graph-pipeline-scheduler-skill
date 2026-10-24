import sys
import json
from client import DAGTaskPipelineScheduler

def handle_request(req):
    method = req.get("method")
    params = req.get("params", {})
    req_id = req.get("id")

    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "tools": [
                    {
                        "name": "schedule_dag_tasks",
                        "description": "Calculates topological stages and critical path for task graphs.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "tasks": {"type": "array"}
                            }
                        }
                    }
                ]
            }
        }
    elif method == "tools/call":
        args = params.get("arguments", {})
        scheduler = DAGTaskPipelineScheduler()
        for t in args.get("tasks", []):
            scheduler.add_task(t["id"], t.get("duration_ms", 100), t.get("depends_on", []))
        res = scheduler.schedule_execution_plan()
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "content": [{"type": "text", "text": json.dumps(res, indent=2)}]
            }
        }
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": "Method not found"}}

def main():
    scheduler = DAGTaskPipelineScheduler()
    scheduler.add_task("a", 100)
    scheduler.add_task("b", 150, ["a"])
    print(json.dumps(scheduler.schedule_execution_plan(), indent=2))

if __name__ == "__main__":
    main()
