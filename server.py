from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List
from urllib.parse import parse_qs, unquote

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from aiogram import Bot


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
DB_INVOICES_FILE = DATA_DIR / "invoices.json"
INVOICES_DIR = DATA_DIR / "invoices"
DB_INVOICE_COUNTERS_FILE = DATA_DIR / "invoice_counters.json"  # { "YYYY-MM": last_suffix }
_db_lock = Lock()

# Telegram Bot configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", "8425860077:AAESfF3o_58rN9uKMtnWStW0iCyrJNqa56w")
_telegram_bot: Bot | None = None


def get_telegram_bot() -> Bot:
	"""Get or create Telegram Bot instance."""
	global _telegram_bot
	if _telegram_bot is None:
		_telegram_bot = Bot(token=BOT_TOKEN)
	return _telegram_bot


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
	if not DB_INVOICES_FILE.exists():
		DB_INVOICES_FILE.write_text("[]", encoding="utf-8")
	if not DB_INVOICE_COUNTERS_FILE.exists():
		DB_INVOICE_COUNTERS_FILE.write_text("{}", encoding="utf-8")
	INVOICES_DIR.mkdir(parents=True, exist_ok=True)

# (moved below after function definitions)


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


# Ensure data directories exist before mounting static
_ensure_db_file()
# Serve files from data directory, including invoices
app.mount("/static", StaticFiles(directory=str(DATA_DIR)), name="static")


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

def _load_dict(file_path: Path) -> Dict[str, Any]:
	_ensure_db_file()
	try:
		raw = file_path.read_text(encoding="utf-8")
		data = json.loads(raw or "{}")
		return data if isinstance(data, dict) else {}
	except json.JSONDecodeError:
		return {}

def _save_dict(file_path: Path, data: Dict[str, Any]) -> None:
	_tmp = file_path.with_suffix(".tmp")
	_tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
	_tmp.replace(file_path)


def _extract_user_id_from_init_data(init_data: str) -> str | None:
	"""
	Extract user_id from Telegram WebApp init_data.
	init_data format: "user=%7B%22id%22%3A123456789%2C..."
	"""
	if not init_data:
		return None
	try:
		# Parse query string
		params = parse_qs(init_data)
		user_str = params.get("user", [None])[0]
		if user_str:
			# Decode URL encoding
			user_json = unquote(user_str)
			user_data = json.loads(user_json)
			user_id = user_data.get("id")
			return str(user_id) if user_id else None
	except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
		# Log error if needed, but don't fail
		pass
	return None


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


def _parse_iso_date_or_now(date_str: str | None) -> datetime:
	if date_str:
		try:
			# Accept both date and datetime
			return datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(tz=None).replace(tzinfo=None)
		except ValueError:
			pass
	return datetime.utcnow()


def _next_invoice_number(for_dt: datetime) -> str:
	month_prefix = f"{for_dt.month:02d}-"
	invoices = _load_list(DB_INVOICES_FILE)
	max_suffix = 0
	for inv in invoices:
		num = str(inv.get("number") or "")
		if num.startswith(month_prefix):
			try:
				suf = int(num.split("-")[1])
				if suf > max_suffix:
					max_suffix = suf
			except Exception:
				continue
	next_suffix = max_suffix + 1
	return f"{month_prefix}{next_suffix:03d}"

def _reserve_next_invoice_number(for_dt: datetime) -> str:
	"""
	Atomically persist and return next invoice number for the month.
	Keyed by YYYY-MM to avoid collisions across years.
	"""
	key = f"{for_dt.year:04d}-{for_dt.month:02d}"
	counters = _load_dict(DB_INVOICE_COUNTERS_FILE)
	# Ensure counter is at least the max from existing invoices
	try:
		preview_suffix = int(_next_invoice_number(for_dt).split("-")[1]) - 1
	except Exception:
		preview_suffix = 0
	last = int(counters.get(key, 0))
	next_suffix = max(last, preview_suffix) + 1
	counters[key] = next_suffix
	_save_dict(DB_INVOICE_COUNTERS_FILE, counters)
	return f"{for_dt.month:02d}-{next_suffix:03d}"

def _invoice_number_exists(number: str) -> bool:
	invoices = _load_list(DB_INVOICES_FILE)
	for inv in invoices:
		if str(inv.get("number")) == number:
			return True
	return False

def _create_invoice_record(*, when: datetime, number: str, order_id: str | None, stored_filename: str | None, public_url: str | None) -> Dict[str, Any]:
	invoices = _load_list(DB_INVOICES_FILE)
	record = {
		"id": f"inv_{int(when.timestamp())}_{len(invoices)+1}",
		"number": number,
		"order_id": order_id,
		"date": when.isoformat() + "Z",
		"file_name": stored_filename,
		"file_url": public_url,
		"created_at": datetime.utcnow().isoformat() + "Z",
	}
	invoices.append(record)
	_save_list(DB_INVOICES_FILE, invoices)
	return record


@app.post("/api/invoices")
async def create_invoice(
	order_id: str | None = Form(default=None),
	date: str | None = Form(default=None),
	number: str | None = Form(default=None),
	file: UploadFile | None = File(default=None),
) -> Dict[str, Any]:
	"""
	Create an invoice with auto number MM-XXX. Optionally attach a file.
	- order_id: optional link to an order
	- date: optional ISO date; used for month in number; defaults to now
	- file: optional file to store and serve later
	"""
	with _db_lock:
		when = _parse_iso_date_or_now(date)
		if number and str(number).strip():
			number = str(number).strip()
			if _invoice_number_exists(number):
				raise HTTPException(status_code=400, detail="Invoice number already exists")
		else:
			number = _reserve_next_invoice_number(when)

		stored_filename = None
		public_url = None
		if file is not None:
			original_name = Path(file.filename or "invoice.bin").name
			stored_filename = f"{number}_{original_name}"
			target_path = INVOICES_DIR / stored_filename
			content = await file.read()
			target_path.write_bytes(content)
			public_url = f"/static/invoices/{stored_filename}"

		invoices = _load_list(DB_INVOICES_FILE)
		record = {
			"id": f"inv_{int(when.timestamp())}_{len(invoices)+1}",
			"number": number,
			"order_id": order_id,
			"date": when.isoformat() + "Z",
			"file_name": stored_filename,
			"file_url": public_url,
			"created_at": datetime.utcnow().isoformat() + "Z",
		}
		invoices.append(record)
		_save_list(DB_INVOICES_FILE, invoices)

	return {"ok": True, "invoice": record}


@app.get("/api/invoices")
def list_invoices() -> Dict[str, Any]:
	with _db_lock:
		invoices = _load_list(DB_INVOICES_FILE)
	return {"ok": True, "count": len(invoices), "invoices": invoices}


@app.get("/api/invoices/next-number")
def preview_next_invoice_number(date: str | None = None, reserve: str | None = None, response: Response = None) -> Dict[str, Any]:
	when = _parse_iso_date_or_now(date)
	with _db_lock:
		# default: reserve to avoid повторов
		do_reserve = True if reserve is None else str(reserve).lower() in ("1", "true", "yes")
		number = _reserve_next_invoice_number(when) if do_reserve else _next_invoice_number(when)
	# prevent browser/proxy caching
	if response is not None:
		response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
		response.headers["Pragma"] = "no-cache"
		response.headers["Expires"] = "0"
	return {"ok": True, "number": number}

@app.post("/api/invoices/json")
def create_invoice_json(payload: Dict[str, Any]) -> Dict[str, Any]:
	"""
	Create invoice using JSON (no file upload). Useful when the frontend
	generates XLS locally and only needs to persist the number/linkage.
	Body:
	- order_id: optional str
	- date: optional ISO string (used for month-based numbering)
	- number: optional str; if omitted, auto-generate (MM-XXX). If provided,
	  must be unique, otherwise 400.
	- file_url: optional str (if already hosted elsewhere)
	"""
	if not isinstance(payload, dict):
		raise HTTPException(status_code=400, detail="Invalid JSON body")
	order_id = payload.get("order_id")
	date_str = payload.get("date")
	explicit_number = payload.get("number")
	file_url = payload.get("file_url")

	with _db_lock:
		when = _parse_iso_date_or_now(date_str)
		if explicit_number:
			number = str(explicit_number).strip()
			if not number:
				raise HTTPException(status_code=400, detail="Field 'number' cannot be empty")
			if _invoice_number_exists(number):
				raise HTTPException(status_code=400, detail="Invoice number already exists")
		else:
			number = _reserve_next_invoice_number(when)

		record = _create_invoice_record(
			when=when,
			number=number,
			order_id=str(order_id) if order_id else None,
			stored_filename=None,
			public_url=str(file_url) if file_url else None,
		)
	return {"ok": True, "invoice": record}

@app.patch("/api/invoices/{invoice_id}/file")
async def attach_invoice_file(invoice_id: str, file: UploadFile = File(...)) -> Dict[str, Any]:
	"""
	Attach or replace a file for an existing invoice.
	"""
	if not file:
		raise HTTPException(status_code=400, detail="File is required")
	with _db_lock:
		invoices = _load_list(DB_INVOICES_FILE)
		target = None
		for inv in invoices:
			if str(inv.get("id")) == invoice_id:
				target = inv
				break
		if target is None:
			raise HTTPException(status_code=404, detail="Invoice not found")

		number = str(target.get("number"))
		original_name = Path(file.filename or "invoice.bin").name
		stored_filename = f"{number}_{original_name}"
		target_path = INVOICES_DIR / stored_filename
		content = await file.read()
		target_path.write_bytes(content)
		public_url = f"/static/invoices/{stored_filename}"

		target["file_name"] = stored_filename
		target["file_url"] = public_url
		_save_list(DB_INVOICES_FILE, invoices)

	return {"ok": True, "invoice": target}


@app.post("/api/files/send-telegram")
async def send_file_telegram(
	file: UploadFile = File(...),
	filename: str = Form(...),
	init_data: str | None = Form(default=None),
	user_id: str | None = Form(default=None),
) -> Dict[str, Any]:
	"""
	Send file to user via Telegram Bot API.
	Used by Telegram Mini App to send files that cannot be downloaded directly.
	
	Request:
	- file: file to send
	- filename: name of the file
	- init_data: Telegram WebApp init_data (optional, for validation)
	- user_id: Telegram user ID (optional, can be extracted from init_data)
	
	Response:
	- success: bool
	- message: str
	"""
	if not file:
		raise HTTPException(status_code=400, detail="File is required")
	
	# Get chat_id from user_id or extract from init_data
	chat_id = user_id
	if not chat_id and init_data:
		chat_id = _extract_user_id_from_init_data(init_data)
	
	if not chat_id:
		raise HTTPException(
			status_code=400,
			detail="Не удалось определить ID пользователя. Укажите user_id или init_data"
		)
	
	try:
		chat_id_int = int(chat_id)
	except (ValueError, TypeError):
		raise HTTPException(status_code=400, detail="Некорректный user_id")
	
	# Read file content
	file_content = await file.read()
	
	# Check file size (Telegram limit: 50 MB)
	max_size = 50 * 1024 * 1024  # 50 MB
	if len(file_content) > max_size:
		raise HTTPException(
			status_code=400,
			detail=f"Файл слишком большой. Максимальный размер: 50 MB"
		)
	
	try:
		# Send file via Telegram Bot API
		bot = get_telegram_bot()
		
		# Use BufferedInputFile for aiogram 3.x
		from aiogram.types import BufferedInputFile
		input_file = BufferedInputFile(file_content, filename=filename)
		
		await bot.send_document(
			chat_id=chat_id_int,
			document=input_file,
		)
		
		return {
			"success": True,
			"message": f"Файл '{filename}' отправлен успешно"
		}
	except Exception as e:
		error_msg = str(e)
		# Handle common Telegram API errors
		if "chat not found" in error_msg.lower() or "user not found" in error_msg.lower():
			raise HTTPException(
				status_code=404,
				detail="Пользователь не найден. Убедитесь, что пользователь начал диалог с ботом."
			)
		elif "forbidden" in error_msg.lower():
			raise HTTPException(
				status_code=403,
				detail="Бот заблокирован пользователем или не имеет доступа к отправке сообщений."
			)
		else:
			raise HTTPException(
				status_code=500,
				detail=f"Ошибка при отправке файла: {error_msg}"
			)


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


