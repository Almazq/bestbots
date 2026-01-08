from __future__ import annotations

import json
import os
import random
import string
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


def _migrate_add_status_field() -> None:
	"""
	Миграция: добавляет поле status в существующие заказы и накладные.
	Вызывается при старте приложения для обеспечения обратной совместимости.
	"""
	try:
		# Миграция заказов
		if DB_ORDERS_FILE.exists():
			orders = _load_list(DB_ORDERS_FILE)
			needs_save = False
			for order in orders:
				if "status" not in order:
					order["status"] = ""
					needs_save = True
			if needs_save:
				_save_list(DB_ORDERS_FILE, orders)
		
		# Миграция накладных
		if DB_INVOICES_FILE.exists():
			invoices = _load_list(DB_INVOICES_FILE)
			needs_save = False
			for invoice in invoices:
				if "status" not in invoice:
					invoice["status"] = ""
					needs_save = True
			if needs_save:
				_save_list(DB_INVOICES_FILE, invoices)
	except Exception:
		# Игнорируем ошибки миграции, чтобы не сломать старт приложения
		pass


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
	
	# Выполняем миграцию для добавления поля status
	_migrate_add_status_field()

# (moved below after function definitions)


def _load_records() -> List[Dict[str, Any]]:
	"""
	Загружает записи из файла.
	Оптимизировано: не вызывает _ensure_db_file() для избежания рекурсивных блокировок.
	"""
	try:
		if not DB_FILE.exists():
			return []
		raw = DB_FILE.read_text(encoding="utf-8")
		if not raw or not raw.strip():
			return []
		data = json.loads(raw)
		return data if isinstance(data, list) else []
	except (json.JSONDecodeError, IOError, OSError):
		# В случае ошибки возвращаем пустой список
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
	"""
	Загружает список из JSON файла.
	Оптимизировано: не вызывает _ensure_db_file() для избежания рекурсивных блокировок.
	"""
	try:
		if not file_path.exists():
			return []
		raw = file_path.read_text(encoding="utf-8")
		if not raw or not raw.strip():
			return []
		data = json.loads(raw)
		return data if isinstance(data, list) else []
	except (json.JSONDecodeError, IOError, OSError):
		# В случае ошибки возвращаем пустой список
		return []


def _save_list(file_path: Path, items: List[Dict[str, Any]]) -> None:
	"""
	Сохраняет список в JSON файл.
	Использует временный файл для атомарной записи.
	"""
	try:
		# Убеждаемся, что директория существует
		file_path.parent.mkdir(parents=True, exist_ok=True)
		
		# Записываем во временный файл
		_tmp = file_path.with_suffix(".tmp")
		_tmp.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
		
		# Атомарно заменяем оригинальный файл
		_tmp.replace(file_path)
	except Exception as e:
		# Логируем ошибку и пробрасываем дальше
		import sys
		print(f"ERROR: Failed to save list to {file_path}: {e}", file=sys.stderr)
		raise

def _load_dict(file_path: Path) -> Dict[str, Any]:
	"""
	Загружает словарь из JSON файла.
	Оптимизировано: не вызывает _ensure_db_file() для избежания рекурсивных блокировок.
	"""
	try:
		if not file_path.exists():
			return {}
		raw = file_path.read_text(encoding="utf-8")
		if not raw or not raw.strip():
			return {}
		data = json.loads(raw)
		return data if isinstance(data, dict) else {}
	except (json.JSONDecodeError, IOError, OSError):
		# В случае ошибки возвращаем пустой словарь
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
	"""
	Возвращает список менеджеров.
	Оптимизировано: быстрое чтение данных.
	"""
	try:
		with _db_lock:
			managers = _load_list(DB_MANAGERS_FILE)
		return {"ok": True, "count": len(managers), "managers": managers}
	except Exception as e:
		# В случае ошибки возвращаем пустой список
		return {"ok": True, "count": 0, "managers": []}


@app.post("/api/orders")
def create_order(payload: Dict[str, Any]) -> Dict[str, Any]:
	"""
	Создает или обновляет заказ.
	
	Принимает JSON с полями:
	- id: опциональное (если начинается с temp_ или пустое, генерируется автоматически)
	- company_name или name_company: опциональное
	- company_bin или bin_company: опциональное
	- manager_id или id_manager: опциональное
	- full_data: опциональное (полные данные заказа, может быть пустым объектом)
	- status: опциональное (статус заказа)
	"""
	if not isinstance(payload, dict):
		raise HTTPException(status_code=400, detail="Invalid JSON body")

	# Логируем входящий запрос для отладки
	import sys
	print(f"DEBUG: Received order creation request: {json.dumps(payload, ensure_ascii=False)[:500]}", file=sys.stderr)

	order_id = str(payload.get("id", "")).strip()
	company_name = str(payload.get("name_company") or payload.get("company_name") or "").strip()
	company_bin = str(payload.get("bin_company") or payload.get("company_bin") or "").strip()
	manager_id = str(payload.get("id_manager") or payload.get("manager_id") or "").strip()
	full_data = payload.get("full_data", {})
	
	# company_name, company_bin, manager_id - все опциональные (могут быть пустыми)
	# full_data тоже опциональное (может быть пустым объектом или любым объектом)
	# Бэкенд не валидирует содержимое full_data - принимает как есть
	
	now_iso = datetime.utcnow().isoformat() + "Z"
	
	# Генерируем ID, если не указан, пустой или начинается с temp_
	try:
		with _db_lock:
			orders = _load_list(DB_ORDERS_FILE)
			
			# Если ID начинается с temp_ или пустой, генерируем новый уникальный ID
			if not order_id or order_id.startswith("temp_"):
				# Генерируем уникальный ID: order_{timestamp_ms}_{random}
				timestamp_ms = int(datetime.utcnow().timestamp() * 1000)
				random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
				order_id = f"order_{timestamp_ms}_{random_suffix}"
			
			# Проверяем, не существует ли уже заказ с таким ID
			existing_index = None
			for i, existing in enumerate(orders):
				if str(existing.get("id")) == order_id:
					existing_index = i
					break
			
			# Получаем статус из payload или используем пустой по умолчанию
			status = str(payload.get("status", "")).strip()
			ALLOWED_STATUSES = ["", "production", "waiting", "shipped", "rejected"]
			if status not in ALLOWED_STATUSES:
				status = ""
			
			# Подготавливаем данные заказа
			order: Dict[str, Any] = {
				"id": order_id,
				"company_name": company_name,
				"name_company": payload.get("name_company", ""),
				"company_bin": company_bin,
				"bin_company": payload.get("bin_company", ""),
				"manager_id": manager_id,
				"status": status,
				"full_data": full_data,
			}
			
			if existing_index is not None:
				# Обновляем существующий заказ - сохраняем оригинальную дату создания
				existing_order = orders[existing_index]
				order["created_at"] = existing_order.get("created_at", now_iso)
				order["updated_at"] = now_iso
				orders[existing_index] = order
			else:
				# Создаем новый заказ
				order["created_at"] = now_iso
				order["updated_at"] = now_iso
				orders.append(order)
			
			try:
				_save_list(DB_ORDERS_FILE, orders)
			except Exception as e:
				# Логируем ошибку сохранения
				import sys
				print(f"ERROR: Failed to save order: {e}", file=sys.stderr)
				raise HTTPException(status_code=500, detail=f"Failed to save order: {str(e)}")

		return {"ok": True, "id": order_id, "order": order}
	except HTTPException:
		# Пробрасываем HTTP исключения как есть
		raise
	except Exception as e:
		# Логируем общую ошибку
		import sys
		print(f"ERROR: Failed to create order: {e}", file=sys.stderr)
		raise HTTPException(status_code=500, detail=f"Failed to create order: {str(e)}")


@app.patch("/api/orders/{order_id}/status")
def update_order_status(order_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
	"""
	Обновляет статус заказа.
	Принимает JSON: {"status": "production"} или {"status": ""}
	Допустимые статусы: "", "production", "waiting", "shipped", "rejected"
	"""
	ALLOWED_STATUSES = ["", "production", "waiting", "shipped", "rejected"]
	
	if not isinstance(payload, dict):
		raise HTTPException(status_code=400, detail="Invalid JSON body")
	
	status = str(payload.get("status", "")).strip()
	if status not in ALLOWED_STATUSES:
		raise HTTPException(
			status_code=400,
			detail=f"Invalid status. Allowed values: {ALLOWED_STATUSES}"
		)
	
	with _db_lock:
		orders = _load_list(DB_ORDERS_FILE)
		order_index = None
		for i, o in enumerate(orders):
			if str(o.get("id")) == order_id:
				order_index = i
				break
		
		if order_index is None:
			raise HTTPException(status_code=404, detail="Order not found")
		
		orders[order_index]["status"] = status
		orders[order_index]["updated_at"] = datetime.utcnow().isoformat() + "Z"
		_save_list(DB_ORDERS_FILE, orders)
		
		return {"ok": True, "order": orders[order_index]}


@app.get("/api/orders")
def list_orders() -> Dict[str, Any]:
	with _db_lock:
		orders = _load_list(DB_ORDERS_FILE)
	return {"ok": True, "count": len(orders), "orders": orders}


@app.delete("/api/orders/{order_id}")
def delete_order(order_id: str) -> Dict[str, Any]:
	"""
	Удаляет заказ по ID.
	"""
	try:
		with _db_lock:
			orders = _load_list(DB_ORDERS_FILE)
			
			# Ищем заказ для удаления
			order_index = None
			for i, order in enumerate(orders):
				if str(order.get("id")) == order_id:
					order_index = i
					break
			
			if order_index is None:
				raise HTTPException(status_code=404, detail="Заказ не найден")
			
			# Удаляем заказ
			orders.pop(order_index)
			_save_list(DB_ORDERS_FILE, orders)
		
		return {"ok": True, "message": "Заказ удален"}
	except HTTPException:
		raise
	except Exception as e:
		import sys
		print(f"ERROR: Failed to delete order: {e}", file=sys.stderr)
		raise HTTPException(status_code=500, detail=f"Ошибка при удалении заказа: {str(e)}")


@app.get("/api/orders/history")
def get_orders_history() -> Dict[str, Any]:
	"""
	Возвращает историю заказов с привязанными накладными.
	Каждый заказ содержит массив связанных накладных.
	Удобно для фронтенда для отображения истории заказов.
	"""
	# Читаем данные быстро, минимизируя время блокировки
	try:
		with _db_lock:
			orders = _load_list(DB_ORDERS_FILE)
			invoices = _load_list(DB_INVOICES_FILE)
			managers = _load_list(DB_MANAGERS_FILE)
	except Exception:
		# В случае ошибки возвращаем пустой результат
		return {"ok": True, "count": 0, "orders": []}
	
	# Обработка данных выполняется вне блокировки для оптимизации производительности
	# Создаем словарь менеджеров для быстрого поиска
	managers_dict = {str(m.get("id")): m.get("name") for m in managers}
	
	# Создаем словарь заказов для быстрого поиска
	orders_dict = {str(o.get("id")): o for o in orders}
	
	# Группируем накладные по order_id
	invoices_by_order: Dict[str, List[Dict[str, Any]]] = {}
	invoices_without_order: List[Dict[str, Any]] = []
	
	for inv in invoices:
		order_id = inv.get("order_id")
		if order_id:
			order_id_str = str(order_id)
			if order_id_str not in invoices_by_order:
				invoices_by_order[order_id_str] = []
			invoices_by_order[order_id_str].append(inv)
		else:
			# Накладные без заказа тоже показываем
			invoices_without_order.append(inv)
	
	# Объединяем заказы с накладными
	orders_with_invoices = []
	for order in orders:
		order_id = str(order.get("id"))
		order_copy = {
			"id": order.get("id"),
			"company_name": order.get("company_name", ""),
			"company_bin": order.get("company_bin", ""),
			"manager_id": order.get("manager_id", ""),
			"status": order.get("status", ""),  # Добавляем статус заказа
			"created_at": order.get("created_at", ""),
			"updated_at": order.get("updated_at", ""),  # Добавляем дату обновления
		}
		# Добавляем имя менеджера
		manager_id = str(order.get("manager_id", ""))
		order_copy["manager_name"] = managers_dict.get(manager_id, "")
		
		# Добавляем только нужные поля накладных, включая статус
		order_invoices = invoices_by_order.get(order_id, [])
		order_copy["invoices"] = [
			{
				"id": inv.get("id"),
				"number": inv.get("number"),
				"status": inv.get("status", ""),  # Добавляем статус накладной
				"date": inv.get("date"),
				"file_url": inv.get("file_url"),
			}
			for inv in order_invoices
		]
		orders_with_invoices.append(order_copy)
	
	# Добавляем накладные без заказов как виртуальные заказы
	# Это нужно, чтобы фронтенд мог показать все накладные в истории
	for inv in invoices_without_order:
		# Создаем виртуальный заказ для накладной без заказа
		order_copy = {
			"id": f"no_order_{inv.get('id')}",
			"company_name": "",
			"company_bin": "",
			"manager_id": "",
			"manager_name": "",
			"status": "",  # Виртуальный заказ без статуса
			"created_at": inv.get("created_at", inv.get("date", "")),
			"invoices": [
				{
					"id": inv.get("id"),
					"number": inv.get("number"),
					"status": inv.get("status", ""),  # Добавляем статус накладной
					"date": inv.get("date"),
					"file_url": inv.get("file_url"),
				}
			],
		}
		orders_with_invoices.append(order_copy)
	
	# Сортируем по дате создания (новые сначала)
	orders_with_invoices.sort(
		key=lambda x: x.get("created_at", ""),
		reverse=True
	)

	return {
		"ok": True,
		"count": len(orders_with_invoices),
		"orders": orders_with_invoices
	}


@app.get("/api/invoices/history")
def get_invoices_history() -> Dict[str, Any]:
	"""
	Возвращает историю всех накладных с информацией о заказах (если есть).
	Удобно для фронтенда для отображения истории накладных.
	"""
	# Читаем данные быстро, минимизируя время блокировки
	with _db_lock:
		orders = _load_list(DB_ORDERS_FILE)
		invoices = _load_list(DB_INVOICES_FILE)
		managers = _load_list(DB_MANAGERS_FILE)
	
	# Обработка данных выполняется вне блокировки для оптимизации производительности
	# Создаем словари для быстрого поиска
	orders_dict = {str(o.get("id")): o for o in orders}
	managers_dict = {str(m.get("id")): m.get("name") for m in managers}
	
	# Обогащаем накладные информацией о заказах
	invoices_with_orders = []
	for inv in invoices:
		inv_copy = inv.copy()
		order_id = inv.get("order_id")
		if order_id:
			order = orders_dict.get(str(order_id))
			if order:
				inv_copy["order"] = {
					"id": order.get("id"),
					"company_name": order.get("company_name"),
					"company_bin": order.get("company_bin"),
					"manager_id": order.get("manager_id"),
					"manager_name": managers_dict.get(str(order.get("manager_id", "")), "")
				}
			else:
				inv_copy["order"] = None
		else:
			inv_copy["order"] = None
		invoices_with_orders.append(inv_copy)
	
	# Сортируем по дате создания (новые сначала)
	invoices_with_orders.sort(
		key=lambda x: x.get("created_at", x.get("date", "")),
		reverse=True
	)

	return {
		"ok": True,
		"count": len(invoices_with_orders),
		"invoices": invoices_with_orders
	}


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

def _create_invoice_record(*, when: datetime, number: str, order_id: str | None, stored_filename: str | None, public_url: str | None, status: str = "") -> Dict[str, Any]:
	invoices = _load_list(DB_INVOICES_FILE)
	record = {
		"id": f"inv_{int(when.timestamp())}_{len(invoices)+1}",
		"number": number,
		"order_id": order_id,
		"status": status,
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
			"status": "",  # Статус по умолчанию - пустая строка
			"date": when.isoformat() + "Z",
			"file_name": stored_filename,
			"file_url": public_url,
			"created_at": datetime.utcnow().isoformat() + "Z",
		}
		invoices.append(record)
		_save_list(DB_INVOICES_FILE, invoices)

	return {"ok": True, "id": record.get("id"), "invoice": record}


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

		# Получаем статус из payload или используем пустой по умолчанию
		status = str(payload.get("status", "")).strip()
		ALLOWED_STATUSES = ["", "production", "waiting", "shipped", "rejected"]
		if status not in ALLOWED_STATUSES:
			status = ""
		
		record = _create_invoice_record(
			when=when,
			number=number,
			order_id=str(order_id) if order_id else None,
			stored_filename=None,
			public_url=str(file_url) if file_url else None,
			status=status,
		)
	return {"ok": True, "id": record.get("id"), "invoice": record}

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

	return {"ok": True, "id": target.get("id"), "invoice": target}


@app.patch("/api/invoices/{invoice_id}/status")
def update_invoice_status(invoice_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
	"""
	Обновляет статус накладной.
	Принимает JSON: {"status": "shipped"} или {"status": ""}
	Допустимые статусы: "", "production", "waiting", "shipped", "rejected"
	"""
	ALLOWED_STATUSES = ["", "production", "waiting", "shipped", "rejected"]
	
	if not isinstance(payload, dict):
		raise HTTPException(status_code=400, detail="Invalid JSON body")
	
	status = str(payload.get("status", "")).strip()
	if status not in ALLOWED_STATUSES:
		raise HTTPException(
			status_code=400,
			detail=f"Invalid status. Allowed values: {ALLOWED_STATUSES}"
		)
	
	with _db_lock:
		invoices = _load_list(DB_INVOICES_FILE)
		invoice_index = None
		for i, inv in enumerate(invoices):
			if str(inv.get("id")) == invoice_id:
				invoice_index = i
				break
		
		if invoice_index is None:
			raise HTTPException(status_code=404, detail="Invoice not found")
		
		invoices[invoice_index]["status"] = status
		invoices[invoice_index]["updated_at"] = datetime.utcnow().isoformat() + "Z"
		_save_list(DB_INVOICES_FILE, invoices)
		
		return {"ok": True, "invoice": invoices[invoice_index]}


@app.delete("/api/invoices/{invoice_id}")
def delete_invoice(invoice_id: str) -> Dict[str, Any]:
	"""
	Удаляет накладную по ID.
	Также удаляет файл накладной с диска, если он существует.
	"""
	try:
		with _db_lock:
			invoices = _load_list(DB_INVOICES_FILE)
			
			# Ищем накладную для удаления
			invoice_index = None
			target_invoice = None
			for i, inv in enumerate(invoices):
				if str(inv.get("id")) == invoice_id:
					invoice_index = i
					target_invoice = inv
					break
			
			if invoice_index is None:
				raise HTTPException(status_code=404, detail="Накладная не найдена")
			
			# Удаляем файл накладной с диска, если он существует
			file_name = target_invoice.get("file_name")
			if file_name:
				file_path = INVOICES_DIR / file_name
				try:
					if file_path.exists():
						file_path.unlink()
				except Exception as e:
					# Логируем ошибку удаления файла, но не прерываем удаление записи
					import sys
					print(f"WARNING: Failed to delete invoice file {file_name}: {e}", file=sys.stderr)
			
			# Удаляем накладную из БД
			invoices.pop(invoice_index)
			_save_list(DB_INVOICES_FILE, invoices)
		
		return {"ok": True, "message": "Накладная удалена"}
	except HTTPException:
		raise
	except Exception as e:
		import sys
		print(f"ERROR: Failed to delete invoice: {e}", file=sys.stderr)
		raise HTTPException(status_code=500, detail=f"Ошибка при удалении накладной: {str(e)}")


@app.post("/api/files/send-telegram")
async def send_file_telegram(
	file: UploadFile = File(...),
	filename: str | None = Form(default=None),
	init_data: str | None = Form(default=None),
	user_id: str | None = Form(default=None),
) -> Dict[str, Any]:
	"""
	Отправляет файл пользователю через Telegram Bot API.
	Используется Telegram Mini App для отправки файлов, которые нельзя скачать напрямую.
	
	Request:
	- file: файл для отправки (обязательное)
	- filename: имя файла (опциональное, если не указано, берется из file.filename)
	- init_data: Telegram WebApp init_data (опциональное, для проверки подлинности)
	- user_id: Telegram user ID (опциональное, может быть извлечен из init_data)
	
	Response:
	- success: bool
	- message: str
	"""
	if not file:
		raise HTTPException(status_code=400, detail="File is required")
	
	# Используем переданное имя файла или имя из UploadFile
	file_name = filename or file.filename or "file"
	
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
		input_file = BufferedInputFile(file_content, filename=file_name)
		
		await bot.send_document(
			chat_id=chat_id_int,
			document=input_file,
		)
		
		return {
			"success": True,
			"message": f"Файл '{file_name}' отправлен вам в Telegram"
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


