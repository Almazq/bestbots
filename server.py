from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI(title="BestsBot Backend", version="1.0.0")

# CORS: fully open for all origins/methods/headers (no credentials)
app.add_middleware(
	CORSMiddleware,
	allow_origins=["*"],
	allow_credentials=False,
	allow_methods=["*"],
	allow_headers=["*"],
	expose_headers=["*"],
)


DATA_DIR = Path(__file__).parent / "data"
DB_FILE = DATA_DIR / "records.json"
DB_MANAGERS_FILE = DATA_DIR / "managers.json"
DB_ORDERS_FILE = DATA_DIR / "orders.json"
_db_lock = Lock()


def _ensure_db_file() -> None:
	"""
	Ensure data directory and JSON file exist.
	"""
	DATA_DIR.mkdir(parents=True, exist_ok=True)
	if not DB_FILE.exists():
		DB_FILE.write_text("[]", encoding="utf-8")
	if not DB_MANAGERS_FILE.exists():
		DB_MANAGERS_FILE.write_text("[]", encoding="utf-8")
	# Seed default managers if file is empty or invalid
	_seed_default_managers_if_empty()
	if not DB_ORDERS_FILE.exists():
		DB_ORDERS_FILE.write_text("[]", encoding="utf-8")


def _load_records() -> List[Dict[str, Any]]:
	_ensure_db_file()
	try:
		raw = DB_FILE.read_text(encoding="utf-8")
		data = json.loads(raw or "[]")
		if isinstance(data, list):
			return data
		return []
	except json.JSONDecodeError:
		# Reset corrupt file
		return []


def _save_records(records: List[Dict[str, Any]]) -> None:
	_tmp = DB_FILE.with_suffix(".tmp")
	_tmp.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
	_tmp.replace(DB_FILE)


def _seed_default_managers_if_empty() -> None:
	"""
	Populate default managers if no managers are present or file is corrupt.
	"""
	try:
		raw = DB_MANAGERS_FILE.read_text(encoding="utf-8") if DB_MANAGERS_FILE.exists() else ""
		data = json.loads(raw or "[]")
		if not isinstance(data, list) or len(data) == 0:
			defaults = [
				{"id": "m1", "name": "Айгерім"},
				{"id": "m2", "name": "Мақпал"},
			]
			DB_MANAGERS_FILE.write_text(json.dumps(defaults, ensure_ascii=False, indent=2), encoding="utf-8")
	except json.JSONDecodeError:
		defaults = [
			{"id": "m1", "name": "Айгерім"},
			{"id": "m2", "name": "Мақпал"},
		]
		DB_MANAGERS_FILE.write_text(json.dumps(defaults, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_list(file_path: Path) -> List[Dict[str, Any]]:
	_ensure_db_file()
	try:
		raw = file_path.read_text(encoding="utf-8")
		data = json.loads(raw or "[]")
		return data if isinstance(data, list) else []
	except json.JSONDecodeError:
		return []


def _save_list(file_path: Path, items: List[Dict[str, Any]]) -> None:
	_tmp = file_path.with_suffix(".tmp")
	_tmp.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
	_tmp.replace(file_path)


@app.post("/api/records")
def create_record(payload: Dict[str, Any]) -> Dict[str, Any]:
	"""
	Accepts arbitrary JSON describing a downloadable file and its metadata.
	Expected common fields:
	- id: str
	- name: str
	- date: str (ISO date/datetime)
	- file or file_url: str (URL to download)

	All extra fields are stored as-is.
	"""
	if not isinstance(payload, dict):
		raise HTTPException(status_code=400, detail="Invalid JSON body")

	# Minimal validation
	record_id = str(payload.get("id", "")).strip()
	name = str(payload.get("name", "")).strip()
	date_str = str(payload.get("date", "")).strip()
	file_url = (payload.get("file") or payload.get("file_url") or "").strip()

	if not record_id:
		raise HTTPException(status_code=400, detail="Field 'id' is required")
	if not name:
		raise HTTPException(status_code=400, detail="Field 'name' is required")
	if not date_str:
		raise HTTPException(status_code=400, detail="Field 'date' is required")
	if not file_url:
		raise HTTPException(status_code=400, detail="Field 'file' or 'file_url' is required")

	now_iso = datetime.utcnow().isoformat() + "Z"

	# Normalize to a consistent structure while preserving payload
	record: Dict[str, Any] = {
		"id": record_id,
		"name": name,
		"date": date_str,
		"file_url": file_url,
		"payload": payload,
		"created_at": now_iso,
	}

	with _db_lock:
		records = _load_records()
		records.append(record)
		_save_records(records)

	return {"ok": True, "record": record}


@app.get("/api/records")
def list_records() -> Dict[str, Any]:
	"""
	Returns all stored records.
	"""
	with _db_lock:
		records = _load_records()
	return {"ok": True, "count": len(records), "records": records}


@app.post("/api/managers")
def create_or_update_manager(payload: Dict[str, Any]) -> Dict[str, Any]:
	"""
	Managers table:
	- id: str
	- name: str
	Upsert by id.
	"""
	if not isinstance(payload, dict):
		raise HTTPException(status_code=400, detail="Invalid JSON body")

	manager_id = str(payload.get("id", "")).strip()
	name = str(payload.get("name", "")).strip()
	if not manager_id:
		raise HTTPException(status_code=400, detail="Field 'id' is required")
	if not name:
		raise HTTPException(status_code=400, detail="Field 'name' is required")

	with _db_lock:
		managers = _load_list(DB_MANAGERS_FILE)
		mode = "created"
		for m in managers:
			if str(m.get("id")) == manager_id:
				m["name"] = name
				mode = "updated"
				break
		else:
			managers.append({"id": manager_id, "name": name})
		_save_list(DB_MANAGERS_FILE, managers)

	return {"ok": True, "mode": mode, "manager": {"id": manager_id, "name": name}}


@app.get("/api/managers")
def list_managers() -> Dict[str, Any]:
	with _db_lock:
		managers = _load_list(DB_MANAGERS_FILE)
	return {"ok": True, "count": len(managers), "managers": managers}


@app.delete("/api/managers/{manager_id}")
def delete_manager(manager_id: str) -> Dict[str, Any]:
	"""
	Delete manager by id. Returns 404 if not found.
	"""
	with _db_lock:
		managers = _load_list(DB_MANAGERS_FILE)
		remaining = [m for m in managers if str(m.get("id")) != str(manager_id)]
		if len(remaining) == len(managers):
			raise HTTPException(status_code=404, detail="Manager not found")
		_save_list(DB_MANAGERS_FILE, remaining)
	return {"ok": True, "deleted_id": manager_id, "count": len(remaining)}


@app.post("/api/orders")
def create_order(payload: Dict[str, Any]) -> Dict[str, Any]:
	"""
	Orders table:
	- id: str
	- company_name: str
	- company_bin: str
	- manager_id: str
	- full_data: dict (stores the entire request payload as-is)
	"""
	if not isinstance(payload, dict):
		raise HTTPException(status_code=400, detail="Invalid JSON body")

	order_id = str(payload.get("id", "")).strip()
	company_name = str(payload.get("name_company") or payload.get("company_name") or "").strip()
	company_bin = str(payload.get("bin_company") or payload.get("company_bin") or "").strip()
	manager_id = str(payload.get("id_manager") or payload.get("manager_id") or "").strip()
	if not order_id:
		raise HTTPException(status_code=400, detail="Field 'id' is required")
	if not company_name:
		raise HTTPException(status_code=400, detail="Field 'name_company' (company_name) is required")
	if not company_bin:
		raise HTTPException(status_code=400, detail="Field 'bin_company' (company_bin) is required")
	if not manager_id:
		raise HTTPException(status_code=400, detail="Field 'id_manager' (manager_id) is required")

	now_iso = datetime.utcnow().isoformat() + "Z"
	order: Dict[str, Any] = {
		"id": order_id,
		"company_name": company_name,
		"company_bin": company_bin,
		"manager_id": manager_id,
		"full_data": payload,
		"created_at": now_iso,
	}

	with _db_lock:
		orders = _load_list(DB_ORDERS_FILE)
		# replace if id exists, else append
		for i, existing in enumerate(orders):
			if str(existing.get("id")) == order_id:
				orders[i] = order
				break
		else:
			orders.append(order)
		_save_list(DB_ORDERS_FILE, orders)

	return {"ok": True, "order": order}


@app.get("/api/orders")
def list_orders() -> Dict[str, Any]:
	with _db_lock:
		orders = _load_list(DB_ORDERS_FILE)
	return {"ok": True, "count": len(orders), "orders": orders}


@app.get("/")
def root() -> Dict[str, Any]:
	"""
	Simple health/info endpoint for load balancers and uptime checks.
	"""
	return {"ok": True, "service": "bestsbot-backend", "time": datetime.utcnow().isoformat() + "Z"}


@app.get("/healthz")
def healthz() -> Dict[str, Any]:
	return {"status": "ok"}


if __name__ == "__main__":
	# Local run:
	#   uvicorn server:app --host 0.0.0.0 --port 8000
	import uvicorn

	uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)


