import os
import uuid
import boto3
import zlib
import base64

import json
from functools import wraps
from botocore.exceptions import ClientError, NoCredentialsError
from flask import Flask, render_template, request, jsonify, session, Response
import pandas as pd
from io import StringIO, BytesIO
import requests
import urllib.parse
from datetime import datetime
import time
import re
import math
import contextlib
import subprocess
import tempfile
import shutil
from concurrent.futures import ThreadPoolExecutor

# Analysis runs fan the (many, network-bound) Azure retail-price lookups out across
# a thread pool — the dominant cost of an analysis. Tune with AZURE_PRICE_WORKERS.
AZURE_PRICE_WORKERS = max(1, int(os.environ.get("AZURE_PRICE_WORKERS", "8")))

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-me-in-production-please")

def read_compressed_json(file_path):
    with open(file_path, 'rb') as f:
        compressed_data = f.read()
        decompressed_data = zlib.decompress(compressed_data)
        decoded_str = decompressed_data.decode('utf-8')
        old = """'"""
        new = '"'
        replaced_str = decoded_str.replace(old, new)
        new_decompressed_data = replaced_str.encode('utf-8')
        data = json.loads(new_decompressed_data)
    return data

def _cfg_int(cfg, key, default=-99):
    """Read an integer column-index / threshold from a config dict, tolerating a
    missing key or a null value (returns `default`). Preserves a legitimate 0
    (so `int(x or default)` pitfalls are avoided)."""
    v = cfg.get(key, default)
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default

# ── Auth decorator ─────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return jsonify({"error": "Authentication required", "redirect": "/"}), 401
        return f(*args, **kwargs)
    return decorated

# ── S3 helper ──────────────────────────────────────────────
# ══════════════════════════════════════════════════════════
#  Storage backend abstraction (S3 or local disk)
# ══════════════════════════════════════════════════════════
# The whole app talks to storage through a boto3-S3-shaped client (get_object,
# put_object, list_objects_v2, copy_object, delete_object(s), head_object,
# generate_presigned_url) and the module globals s3_bucket/s3_region. To support
# a local-disk backend without rewriting the ~50 call sites, LocalS3Client below
# emulates exactly that slice of the S3 API against a directory tree. The chosen
# backend (MikeS3 / Other S3 / Local) is set at login and stored per-session.

LOCAL_STORAGE_BASE = "EverpureTCO"   # folder created on the chosen local drive
DEFAULT_S3_BUCKET  = '980182764859-virg-bucket'

def _guess_ct(path):
    pl = path.lower()
    if pl.endswith(".json"): return "application/json"
    if pl.endswith(".csv"):  return "text/csv"
    if pl.endswith(".pdf"):  return "application/pdf"
    return "application/octet-stream"

def _no_such_key(op="GetObject"):
    return ClientError({"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}}, op)

def _local_root_for_drive(spec):
    """Resolve the Local Storage root. Accepts a Windows drive letter (e.g. "D")
    OR, on any OS (Mac/Linux/container), an absolute or relative folder path.
    Returns <root>/EverpureTCO."""
    s = str(spec or "").strip()
    if not s:
        raise ValueError("Enter a drive letter (Windows) or a folder path.")
    # A bare single letter (optionally trailing ':' or slash) -> Windows drive.
    if re.match(r"^[A-Za-z][:\\/]?$", s):
        d = s.rstrip(":\\/").upper()
        return os.path.join(f"{d}:\\", LOCAL_STORAGE_BASE)
    # Otherwise treat it as a directory path (Mac/Linux/container or a Windows folder).
    return os.path.join(os.path.abspath(os.path.expanduser(s)), LOCAL_STORAGE_BASE)

class _LocalPaginator:
    def __init__(self, client): self.client = client
    def paginate(self, Bucket=None, Prefix="", **kw):
        yield self.client.list_objects_v2(Bucket=Bucket, Prefix=Prefix)

class LocalS3Client:
    """Emulates the subset of the boto3 S3 client API this app uses, backed by a
    local directory tree rooted at `root`. The Bucket argument is ignored; keys
    map 1:1 to files under root. Missing objects raise a botocore ClientError
    with Code 'NoSuchKey' so existing S3 error handling works unchanged."""
    def __init__(self, root):
        self.root = root
        os.makedirs(root, exist_ok=True)
    def _path(self, key):
        key = str(key).replace("\\", "/").lstrip("/")
        p = os.path.normpath(os.path.join(self.root, *key.split("/")))
        if not os.path.abspath(p).startswith(os.path.abspath(self.root)):
            raise _no_such_key()
        return p
    def get_object(self, Bucket=None, Key=None, **kw):
        p = self._path(Key)
        if not os.path.isfile(p):
            raise _no_such_key("GetObject")
        with open(p, "rb") as f:
            data = f.read()
        return {"Body": BytesIO(data), "ContentLength": len(data),
                "LastModified": datetime.fromtimestamp(os.path.getmtime(p)),
                "ContentType": _guess_ct(p), "ETag": '"%d"' % len(data)}
    def put_object(self, Bucket=None, Key=None, Body=b"", ContentType=None, **kw):
        p = self._path(Key)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        if hasattr(Body, "read"):
            data = Body.read()
        elif isinstance(Body, str):
            data = Body.encode("utf-8")
        else:
            data = Body
        with open(p, "wb") as f:
            f.write(data)
        return {"ETag": '"%d"' % len(data)}
    def head_object(self, Bucket=None, Key=None, **kw):
        p = self._path(Key)
        if not os.path.isfile(p):
            raise _no_such_key("HeadObject")
        sz = os.path.getsize(p)
        return {"ContentLength": sz, "LastModified": datetime.fromtimestamp(os.path.getmtime(p)),
                "ContentType": _guess_ct(p), "ETag": '"%d"' % sz}
    def delete_object(self, Bucket=None, Key=None, **kw):
        p = self._path(Key)
        if os.path.isfile(p):
            os.remove(p)
        return {}
    def delete_objects(self, Bucket=None, Delete=None, **kw):
        deleted = []
        for o in (Delete or {}).get("Objects", []):
            p = self._path(o.get("Key"))
            if os.path.isfile(p):
                os.remove(p); deleted.append({"Key": o.get("Key")})
        return {"Deleted": deleted}
    def copy_object(self, Bucket=None, CopySource=None, Key=None, **kw):
        src = CopySource.get("Key") if isinstance(CopySource, dict) else str(CopySource).split("/", 1)[-1]
        sp = self._path(src); dp = self._path(Key)
        if not os.path.isfile(sp):
            raise _no_such_key("CopyObject")
        os.makedirs(os.path.dirname(dp), exist_ok=True)
        shutil.copyfile(sp, dp)
        return {}
    def list_objects_v2(self, Bucket=None, Prefix="", **kw):
        contents = []
        for dirpath, _dirs, files in os.walk(self.root):
            for fn in files:
                full = os.path.join(dirpath, fn)
                rel = os.path.relpath(full, self.root).replace("\\", "/")
                if Prefix and not rel.startswith(Prefix):
                    continue
                contents.append({"Key": rel, "Size": os.path.getsize(full),
                                 "LastModified": datetime.fromtimestamp(os.path.getmtime(full))})
        return {"Contents": contents, "KeyCount": len(contents)}
    def get_paginator(self, name):
        return _LocalPaginator(self)
    def generate_presigned_url(self, *a, **kw):
        raise NotImplementedError("presigned URLs are not available for local storage")

def _default_storage():
    return {"kind": "mikes3"}

def _env_storage():
    """A deployment storage backend defined via environment variables, or None.
    Lets a containerized/headless deploy run WITHOUT the interactive setup screen:
      EVERPURE_STORAGE=local   [+ EVERPURE_LOCAL_ROOT=/data]     (default local root /data)
      EVERPURE_STORAGE=mikes3
      EVERPURE_STORAGE=others3 [+ EVERPURE_S3_BUCKET=my-bucket]  (creds via AWS_* env)
    When set, this counts as "configured" so the storage gate is satisfied."""
    kind = os.environ.get("EVERPURE_STORAGE", "").strip().lower()
    if kind == "local":
        root = os.environ.get("EVERPURE_LOCAL_ROOT", "/data")
        return {"kind": "local",
                "root": os.path.join(os.path.abspath(os.path.expanduser(root)), LOCAL_STORAGE_BASE)}
    if kind == "mikes3":
        return {"kind": "mikes3"}
    if kind == "others3":
        return {"kind": "others3",
                "bucket": os.environ.get("EVERPURE_S3_BUCKET", ""),
                "access_key": os.environ.get("AWS_ACCESS_KEY_ID"),
                "secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
                "region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1")}
    return None

# The storage location is a DEPLOYMENT setting, chosen once and saved to a local
# file next to app.py so it survives restarts and applies to everyone until cleared.
STORAGE_CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage_config.json")

def _load_storage_config():
    """Return the effective deployment storage config, or None if not configured.
    Precedence: the saved file (interactive setup) first, then environment variables
    (containers/headless). None means "not configured" -> the setup gate applies."""
    try:
        with open(STORAGE_CONFIG_FILE, encoding="utf-8") as f:
            d = json.load(f)
        if isinstance(d, dict) and d.get("kind") in ("mikes3", "others3", "local"):
            return d
    except Exception:
        pass
    return _env_storage()

def _save_storage_config(storage):
    with open(STORAGE_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(storage, f)

def _clear_storage_config():
    try:
        os.remove(STORAGE_CONFIG_FILE)
    except FileNotFoundError:
        pass

def _storage_offering_location(cfg):
    """Human-readable offering + location (no secrets) for the login screen."""
    kind = (cfg or {}).get("kind", "")
    label = {"mikes3": "MikeS3", "others3": "Other S3", "local": "Local Storage"}.get(kind, kind)
    if kind == "others3":
        loc = f"S3 bucket: {cfg.get('bucket')} ({cfg.get('region', 'us-east-1')})"
    elif kind == "local":
        loc = f"Local drive {cfg.get('drive')}: — {cfg.get('root')}"
    else:
        loc = f"Default S3 bucket ({DEFAULT_S3_BUCKET})"
    return {"kind": kind, "offering": label, "location": loc}

def _session_storage():
    """The active storage backend — the deployment setting, or MikeS3 as a safe
    fallback so infrastructure calls never crash before it's configured."""
    return _load_storage_config() or _default_storage()

def _mikes3_creds():
    arch = os.environ.get("AWS_ARCH_FILE", r'C:\Users\micha\aws.arch')
    try:
        d = read_compressed_json(arch)
        return d['aws_access_key_id'], d['aws_secret_access_key']
    except Exception:
        return os.environ.get("AWS_ACCESS_KEY_ID"), os.environ.get("AWS_SECRET_ACCESS_KEY")

def _s3_verify():
    # TLS trust: on networks that intercept TLS (corporate proxy), botocore's
    # default CA bundle won't trust the presented cert. Allow a CA bundle path
    # (AWS_CA_BUNDLE) or an explicit, opt-in insecure fallback.
    verify = os.environ.get("AWS_CA_BUNDLE")
    if os.environ.get("S3_INSECURE_TLS", "").lower() in ("1", "true", "yes"):
        verify = False
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
    return verify

def _build_client(storage):
    """Return a storage client (real boto3 S3 or LocalS3Client) for a config."""
    kind = (storage or {}).get("kind", "mikes3")
    if kind == "local":
        return LocalS3Client(storage.get("root") or _local_root_for_drive(storage.get("drive")))
    if kind == "others3":
        key, sec = storage.get("access_key"), storage.get("secret_key")
        region = storage.get("region") or "us-east-1"
    else:  # mikes3
        key, sec = _mikes3_creds()
        region = "us-east-1"
    base_session = boto3.Session(aws_access_key_id=key, aws_secret_access_key=sec, region_name=region)
    return base_session.client('s3', verify=_s3_verify())

def _bucket_for(storage):
    kind = (storage or {}).get("kind", "mikes3")
    if kind == "local":   return "local"
    if kind == "others3": return storage.get("bucket") or ""
    return DEFAULT_S3_BUCKET

def _region_for(storage):
    if (storage or {}).get("kind") == "others3":
        return storage.get("region") or "us-east-1"
    return "us-east-1"

def _storage_write_test(storage):
    """Write (then read + delete) a tiny health-check object to confirm the chosen
    location is writable. Returns (ok, message)."""
    test_key = f"TCO-GUI/_healthcheck/login_test_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.txt"
    try:
        client = _build_client(storage)
        bucket = _bucket_for(storage)
        client.put_object(Bucket=bucket, Key=test_key, Body=b"ok", ContentType="text/plain")
        client.get_object(Bucket=bucket, Key=test_key)["Body"].read()
        client.delete_object(Bucket=bucket, Key=test_key)
        return True, "ok"
    except Exception as exc:
        return False, str(exc)

def _s3_client():
    """The storage client for the current session's chosen backend."""
    return _build_client(_session_storage())

@app.before_request
def _apply_session_storage():
    """Point the module-level bucket/region at the deployment storage backend,
    and GATE the app: until a storage location is configured, block every page
    and API except the storage-config setup, the SPA shell, and auth status."""
    global s3_bucket, s3_region
    cfg = _load_storage_config()
    st = cfg or _default_storage()
    s3_bucket = _bucket_for(st)
    s3_region = _region_for(st)
    if cfg is None:
        p = request.path
        allowed = (
            p == "/" or p.startswith("/static/")
            or p.startswith("/api/storage/config")
            or p in ("/api/auth/status", "/api/auth/logout")
        )
        if not allowed:
            return jsonify({"error": "Storage location is not configured for this deployment."}), 409

@app.errorhandler(Exception)
def _json_error_handler(e):
    """Ensure /api/* endpoints always return JSON (never an HTML error page) so
    the frontend's res.json() never chokes on '<!doctype ...'. Non-API routes
    keep their normal behavior. (In Flask debug mode the interactive debugger
    still intercepts unhandled exceptions — endpoints that must be robust there
    catch their own errors.)"""
    from werkzeug.exceptions import HTTPException
    if request.path.startswith("/api/"):
        code = e.code if isinstance(e, HTTPException) else 500
        msg  = getattr(e, "description", None) if isinstance(e, HTTPException) else str(e)
        return jsonify({"error": msg or "Internal server error"}), code
    if isinstance(e, HTTPException):
        return e
    raise e

def _ensure_backend_configs(storage):
    """A newly selected Other-S3 / Local backend won't have the global engine
    config files (ec_config.json, ecan_config.json). Copy any that are missing
    from MikeS3 so analysis works there. Idempotent and best-effort — only
    contacts MikeS3 when something is actually missing. Returns the list of keys
    that ended up seeded."""
    if (storage or {}).get("kind") == "mikes3":
        return []
    seeded = []
    try:
        dst, dbucket = _build_client(storage), _bucket_for(storage)
        missing = []
        for key in (EC_CONFIG_KEY, ECAN_CONFIG_KEY):
            try:
                dst.head_object(Bucket=dbucket, Key=key)
            except Exception:
                missing.append(key)
        if not missing:
            return []
        # 1) Prefer the engine configs bundled in the image (notes/*.json) — works
        #    offline and for Local Storage backends with no AWS access. This is the
        #    durable seed source; MikeS3 is only a fallback.
        notes_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "notes")
        notes_for = {EC_CONFIG_KEY: "ec_config.json", ECAN_CONFIG_KEY: "ecan_config.json"}
        still_missing = []
        for key in missing:
            fn = os.path.join(notes_dir, notes_for.get(key, ""))
            try:
                if notes_for.get(key) and os.path.isfile(fn):
                    with open(fn, "rb") as f:
                        dst.put_object(Bucket=dbucket, Key=key, Body=f.read(), ContentType="application/json")
                    seeded.append(key)
                else:
                    still_missing.append(key)
            except Exception as exc:
                print(f"Could not seed {key} from bundled notes: {exc}")
                still_missing.append(key)
        # 2) Fall back to MikeS3 for anything the image didn't provide.
        if still_missing:
            try:
                src, sbucket = _build_client({"kind": "mikes3"}), DEFAULT_S3_BUCKET
                for key in still_missing:
                    try:
                        data = src.get_object(Bucket=sbucket, Key=key)["Body"].read()
                        dst.put_object(Bucket=dbucket, Key=key, Body=data, ContentType="application/json")
                        seeded.append(key)
                    except Exception as exc:
                        print(f"Could not seed {key} from MikeS3: {exc}")
            except Exception as exc:
                print(f"MikeS3 seeding unavailable: {exc}")
    except Exception as exc:
        print(f"Backend config seeding skipped: {exc}")
    return seeded

# ══════════════════════════════════════════════════════════
#  Page route
# ══════════════════════════════════════════════════════════

@app.route("/")
def index():
    # print("in index")
    return render_template("index.html")

# ══════════════════════════════════════════════════════════
#  Results / Parsed Data API
# ══════════════════════════════════════════════════════════

@app.route("/api/results/list", methods=["GET"])
@login_required
def results_list():
    """List all parsed_data.csv objects for the active customer across all runs."""
    global s3_region, s3_bucket
    active_username = session.get("username", "unknown")
    active_customer = request.args.get("customer", "").strip()
    if not active_customer:
        active_customer = session.get("active_customer", "")
    if not active_customer:
        return jsonify({"error": "No customer selected. Please go to the Customers tab first."}), 400

    prefix = f"TCO-GUI/{active_username}/{active_customer}/"
    try:
        s3 = _s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=s3_bucket, Prefix=prefix)
        parse_files = []
        cfg_files = []
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("parsed_data.csv"):
                    # Extract scenario and datetime from path:
                    # TCO-GUI/<user>/<customer>/<scenario>/<datetime>/results/parsed_data.csv
                    parts = key.split("/")
                    scenario = parts[3] if len(parts) > 3 else "—"
                    dt_str   = parts[4] if len(parts) > 4 else "—"
                    entry = {
                        "key":           key,
                        "scenario":      scenario,
                        "run_datetime":  dt_str,
                        "size":          obj["Size"],
                        "last_modified": obj["LastModified"].isoformat(),
                        "is_filtered":   "/filters/" in key,
                        "is_consolidation": "/consolidations/" in key,
                    }
                    # Filtered datasets carry a filter.json describing the use-case
                    # filter that produced them — surface its label/mode/terms.
                    if entry["is_filtered"]:
                        fj = _load_filter_json(s3, key.rsplit("/", 1)[0] + "/filter.json")
                        if fj:
                            entry["filter_label"] = fj.get("label", "")
                            entry["filter_mode"]  = fj.get("mode", "")
                            entry["filter_terms"] = fj.get("terms", [])
                            entry["filter_kept"]  = fj.get("kept_rows")
                            entry["filter_total"] = fj.get("total_rows")
                    parse_files.append(entry)
                #if key.endswith("source_data_config.json"):
                    # Extract scenario and datetime from path:
                    # TCO-GUI/<user>/<customer>/<scenario>/<datetime>/results/parsed_data.csv
                #    parts = key.split("/")
                #    scenario = parts[3] if len(parts) > 3 else "—"
                #    dt_str   = parts[4] if len(parts) > 4 else "—"
                #    cfg_files.append({
                #        "key":           key,
                #        "scenario":      scenario,
                #        "run_datetime":  dt_str,
                #        "size":          obj["Size"],
                #        "last_modified": obj["LastModified"].isoformat(),
                #    })
        parse_files.sort(key=lambda x: x["last_modified"], reverse=True)
        #cfg_files.sort(key=lambda x: x["last_modified"], reverse=True)
        return jsonify({"ok": True, "customer": active_customer, "files": parse_files})
    except NoCredentialsError:
        return jsonify({"error": "AWS credentials are not configured on the server."}), 500
    except ClientError as exc:
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

@app.route("/api/results/download", methods=["GET"])
@login_required
def results_download():
    """Stream a selected parsed_data.csv back to the browser as a file download."""
    global s3_region, s3_bucket
    key = request.args.get("key", "").strip()
    if not key:
        return jsonify({"error": "key is required."}), 400
    # Restrict to the current user's own parsed_data.csv objects
    active_username = session.get("username", "unknown")
    if not key.startswith(f"TCO-GUI/{active_username}/") or not key.endswith("parsed_data.csv"):
        return jsonify({"error": "Invalid key."}), 400
    try:
        s3  = _s3_client()
        obj = s3.get_object(Bucket=s3_bucket, Key=key)
        body = obj["Body"].read()
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return jsonify({"error": "File not found in S3."}), 404
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    # Friendly download filename built from scenario/run-datetime in the key
    parts    = key.split("/")
    scenario = parts[3] if len(parts) > 3 else "data"
    run_dt   = parts[4] if len(parts) > 4 else ""
    fname    = f"parsed_data_{scenario}_{run_dt}.csv".replace(" ", "_")
    return Response(
        body,
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )

# ══════════════════════════════════════════════════════════
#  TCO Review API  (browse generated TCO results)
# ══════════════════════════════════════════════════════════
@app.route("/api/tco/list", methods=["GET"])
@login_required
def tco_list():
    """List every generated TCO run (group_summary.csv) for the active customer.

    TCO outputs live at:
      TCO-GUI/<user>/<customer>/<scenario>/<run_datetime>/results/tco/<tco_id>/group_summary.csv
    """
    global s3_region, s3_bucket
    active_username = session.get("username", "unknown")
    active_customer = request.args.get("customer", "").strip()
    if not active_customer:
        active_customer = session.get("active_customer", "")
    if not active_customer:
        return jsonify({"error": "No customer selected. Please go to the Customers tab first."}), 400

    prefix = f"TCO-GUI/{active_username}/{active_customer}/"
    try:
        s3 = _s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=s3_bucket, Prefix=prefix)
        runs = []
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith("group_summary.csv"):
                    continue
                # TCO-GUI/<user>/<customer>/<scenario>/<run_datetime>/results/tco/<tco_id>/group_summary.csv
                parts = key.split("/")
                scenario = parts[3] if len(parts) > 3 else "—"
                run_dt   = parts[4] if len(parts) > 4 else "—"
                tco_id   = key.split("/tco/")[1].split("/")[0] if "/tco/" in key else "—"
                # Read the optional description + run parameters saved alongside the run.
                description = ""
                run_params = {}
                try:
                    meta_obj = s3.get_object(Bucket=s3_bucket, Key=key.replace("group_summary.csv", "meta.json"))
                    meta_data = json.loads(meta_obj["Body"].read().decode("utf-8")) or {}
                    description = meta_data.get("description", "")
                    run_params = meta_data.get("params", {}) or {}
                except Exception:
                    description = ""
                    run_params = {}
                # Deployment model: prefer the saved param; fall back to the tco_id
                # prefix ("azn_" marks Azure Native runs) for older runs.
                method = str(run_params.get("method", "")).strip().lower()
                if method not in ("dedicated", "azure_native"):
                    method = "azure_native" if tco_id.startswith("azn_") else "dedicated"
                runs.append({
                    "group_summary_key": key,
                    "cost_sheet_key":    key.replace("group_summary.csv", "cost_sheet.csv"),
                    "df_groups_key":     key.replace("group_summary.csv", "df_groups.csv"),
                    "scenario":          scenario,
                    "run_datetime":      run_dt,
                    "tco_id":            tco_id,
                    "description":       description,
                    "method":            method,
                    "params":            run_params,
                    "size":              obj["Size"],
                    "last_modified":     obj["LastModified"].isoformat(),
                })
        runs.sort(key=lambda x: x["last_modified"], reverse=True)
        return jsonify({"ok": True, "customer": active_customer, "runs": runs})
    except NoCredentialsError:
        return jsonify({"error": "AWS credentials are not configured on the server."}), 500
    except ClientError as exc:
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

def _adjusted_parsed_for_tco(group_summary_key):
    """Load the parsed_data.csv behind a TCO run and rescale snap_cost/total_cost to the
    snapshot rate that TCO was generated with (from its meta.json params), so the parsed
    rows reflect the exact Azure costs that TCO used. Returns (df, tco_rate).

    The parsed dataset lives at the run's dataset dir (the path before '/tco/'); the
    snapshot rate the analysis used is in the run's meta.json. snap_cost is linear in
    ((rate/2)+1) and total_cost == cap+iops+mbps+snap, so the rescale is exact.
    """
    global s3_bucket
    base       = group_summary_key.split("/tco/")[0]      # dataset results dir
    parsed_key = f"{base}/parsed_data.csv"
    cfg_key    = f"{base}/source_data_config.json"
    meta_key   = group_summary_key.replace("group_summary.csv", "meta.json")
    s3 = _s3_client()
    text = s3.get_object(Bucket=s3_bucket, Key=parsed_key)["Body"].read().decode("utf-8")
    df = pd.read_csv(StringIO(text), on_bad_lines="warn")
    df.columns = [c.replace("pscd", "ec") if isinstance(c, str) else c for c in df.columns]
    # Parse-time rate (r0) from the dataset config; analysis rate from the run's meta.
    cfg = {}
    try:
        cfg = json.loads(s3.get_object(Bucket=s3_bucket, Key=cfg_key)["Body"].read().decode("utf-8")) or {}
        r0 = float(cfg.get("monthly_snapshot_rate", 0.1))
    except Exception:
        r0 = 0.1
    try:
        meta = json.loads(s3.get_object(Bucket=s3_bucket, Key=meta_key)["Body"].read().decode("utf-8")) or {}
        tco_rate = float((meta.get("params") or {}).get("monthly_snapshot_rate", r0))
    except Exception:
        tco_rate = r0
    cost_cols = {"cap_cost", "iops_cost", "mbps_cost", "snap_cost", "total_cost"}
    if tco_rate != r0 and cost_cols.issubset(df.columns):
        for c in cost_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        old_factor = (r0 / 2) + 1
        new_factor = 0.0 if tco_rate == 0 else (tco_rate / 2) + 1
        snap_unit = df["snap_cost"] / old_factor
        df["snap_cost"] = snap_unit * new_factor
        df["total_cost"] = (df["cap_cost"] + df["iops_cost"] + df["mbps_cost"] + df["snap_cost"])
    return df, tco_rate, cfg


@app.route("/api/tco/parsed-group", methods=["POST"])
@login_required
def tco_parsed_group():
    """Return the parsed data rows for ONE group of a TCO run, with snap_cost/total_cost
    adjusted to the snapshot rate that TCO was generated with. Powers the TCO Review
    'Parsed Data' view (the user picks a group; default is none)."""
    global s3_bucket
    body  = request.get_json(force=True) or {}
    key   = str(body.get("group_summary_key", "")).strip()
    group = str(body.get("group", "")).strip()
    if not key.endswith("group_summary.csv") or "/tco/" not in key:
        return jsonify({"error": "A valid group_summary_key is required."}), 400
    if not group:
        return jsonify({"error": "A group is required."}), 400
    try:
        df, tco_rate, cfg = _adjusted_parsed_for_tco(key)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return jsonify({"error": "parsed_data.csv not found for this TCO."}), 404
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    if "group_id" not in df.columns:
        return jsonify({"error": "parsed data has no group_id column."}), 400
    full = df[df["group_id"].astype(str) == group]
    total = int(len(full))

    # ── Per-group summary: identity attributes + capacity + all cost values ──
    col_names = df.columns.tolist()
    def _attr(field):
        try:
            idx = int(cfg.get(field, -99))
        except (TypeError, ValueError):
            idx = -99
        if not (0 <= idx < len(col_names)) or col_names[idx] not in full.columns:
            return "—"
        vals = [v for v in full[col_names[idx]].dropna().unique().tolist()]
        if not vals:
            return "—"
        return str(vals[0]) if len(vals) == 1 else f"mixed ({len(vals)})"
    def _sum(col):
        return round(float(pd.to_numeric(full[col], errors="coerce").fillna(0).sum()), 2) if col in full.columns else None
    cap_col_idx = cfg.get("disk_size", -99)
    cap_col = col_names[cap_col_idx] if isinstance(cap_col_idx, int) and 0 <= cap_col_idx < len(col_names) else None
    perf = None
    if "iops_cost" in full.columns and "mbps_cost" in full.columns:
        perf = round(float((pd.to_numeric(full["iops_cost"], errors="coerce").fillna(0)
                            + pd.to_numeric(full["mbps_cost"], errors="coerce").fillna(0)).sum()), 2)
    summary = {
        "region": _attr("region"), "zone": _attr("zone"),
        "subscription": _attr("subscription_or_account_id"), "vnet": _attr("vnet_or_vpc"),
        "volume_count": total,
        "capacity_gib": (round(float(pd.to_numeric(full[cap_col], errors="coerce").fillna(0).sum()), 2)
                         if cap_col and cap_col in full.columns else None),
        "total_cost": _sum("total_cost"), "capacity_cost": _sum("cap_cost"),
        "performance_cost": perf, "snapshot_cost": _sum("snap_cost"),
    }

    LIMIT = 1000
    sub = full.head(LIMIT).copy()
    # Round cost columns for display and make the frame JSON-safe (NaN -> None).
    for c in ("total_cost", "cap_cost", "iops_cost", "mbps_cost", "snap_cost", "paid_capacity"):
        if c in sub.columns:
            sub[c] = pd.to_numeric(sub[c], errors="coerce").round(4)
    safe = sub.astype(object).where(pd.notnull(sub), None)
    return jsonify({"ok": True, "group": group, "snapshot_rate": tco_rate,
                    "diag_marker": "SUMV2", "summary": summary,
                    "columns": list(sub.columns), "rows": safe.to_dict(orient="records"),
                    "row_count": total, "truncated": total > LIMIT})


@app.route("/api/tco/parsed-download", methods=["POST"])
@login_required
def tco_parsed_download():
    """Download the parsed data for the groups INCLUDED in a TCO's summary, with
    snap_cost/total_cost adjusted to the snapshot rate that TCO was generated with."""
    global s3_bucket
    body   = request.get_json(force=True) or {}
    key    = str(body.get("group_summary_key", "")).strip()
    groups = body.get("groups")
    if not key.endswith("group_summary.csv") or "/tco/" not in key:
        return jsonify({"error": "A valid group_summary_key is required."}), 400
    try:
        df, tco_rate, _cfg = _adjusted_parsed_for_tco(key)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return jsonify({"error": "parsed_data.csv not found for this TCO."}), 404
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    if isinstance(groups, list) and groups and "group_id" in df.columns:
        gset = {str(g) for g in groups}
        df = df[df["group_id"].astype(str).isin(gset)]
    scenario = key.split("/")[3] if len(key.split("/")) > 3 else "tco"
    tco_id   = key.split("/tco/")[1].split("/")[0] if "/tco/" in key else "run"
    fname = f"parsed_included_{scenario}_{tco_id}.csv".replace(" ", "_")
    return Response(df.to_csv(index=False).encode("utf-8"), mimetype="text/csv",
                    headers={"Content-Disposition": f'attachment; filename="{fname}"'})


@app.route("/api/tco/delete", methods=["POST"])
@login_required
def tco_delete():
    """Delete a single generated TCO and ALL data saved under its tco/<id>/ folder
    (group_summary/cost_sheet/df_groups/meta/projection/evaluation, etc.)."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    key  = str(body.get("group_summary_key", "")).strip()
    if not key or not key.endswith("group_summary.csv") or "/tco/" not in key:
        return jsonify({"error": "A valid group_summary_key (in a tco/ folder) is required."}), 400
    # Everything for a run lives under its tco/<id>/ folder.
    prefix = key.rsplit("/", 1)[0] + "/"
    deleted = 0
    try:
        s3 = _s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        batch = []
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                batch.append({"Key": obj["Key"]})
                if len(batch) == 1000:
                    s3.delete_objects(Bucket=s3_bucket, Delete={"Objects": batch})
                    deleted += len(batch)
                    batch = []
        if batch:
            s3.delete_objects(Bucket=s3_bucket, Delete={"Objects": batch})
            deleted += len(batch)
        if deleted == 0:
            return jsonify({"error": f"No objects found for this TCO ({prefix})."}), 404
        # print(f"Deleted TCO run: {prefix} ({deleted} objects)")
        return jsonify({"ok": True, "deleted_objects": deleted,
                        "message": f"Deleted this TCO and {deleted} data object(s)."})
    except NoCredentialsError:
        return jsonify({"error": "AWS credentials are not configured on the server."}), 500
    except ClientError as exc:
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

def _tco_links_key(customer):
    user = session.get("username", "unknown")
    return f"TCO-GUI/{user}/{customer}/_tco_links.json"

@app.route("/api/tco/links", methods=["GET"])
@login_required
def tco_links_get():
    """Return the primary/follower links for a customer (which generated TCO is the
    'primary', and which other runs 'follow' its included-group set)."""
    global s3_region, s3_bucket
    customer = request.args.get("customer", "").strip() or session.get("active_customer", "")
    empty = {"primary_key": None, "followers": [], "primary_settings": None, "primary_groups": []}
    if not customer:
        return jsonify({"ok": True, "links": empty})
    try:
        obj = _s3_client().get_object(Bucket=s3_bucket, Key=_tco_links_key(customer))
        data = json.loads(obj["Body"].read().decode("utf-8")) or {}
        return jsonify({"ok": True, "links": {
            "primary_key": data.get("primary_key"),
            "followers": data.get("followers", []) or [],
            "primary_settings": data.get("primary_settings") or None,
            "primary_groups": data.get("primary_groups", []) or [],
        }})
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return jsonify({"ok": True, "links": empty})
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

@app.route("/api/tco/links", methods=["POST"])
@login_required
def tco_links_set():
    """Persist the primary/follower links for a customer. A run that is the primary
    cannot also be a follower."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    customer = str(body.get("customer", "")).strip() or session.get("active_customer", "")
    if not customer:
        return jsonify({"error": "customer is required."}), 400
    primary_key = body.get("primary_key") or None
    followers = [f for f in (body.get("followers", []) or []) if f and f != primary_key]
    payload = {"primary_key": primary_key, "followers": followers,
               "primary_settings": body.get("primary_settings") or None,
               "primary_groups": [str(g) for g in (body.get("primary_groups", []) or [])]}
    try:
        _s3_client().put_object(
            Bucket=s3_bucket, Key=_tco_links_key(customer),
            Body=json.dumps(payload).encode("utf-8"), ContentType="application/json")
        return jsonify({"ok": True, "links": payload})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

def _find_chromium():
    """Locate a Chromium-based browser for headless PDF rendering. Cross-platform:
    honours CHROMIUM_PATH, then checks Windows, macOS and Linux install locations,
    then PATH."""
    env = os.environ.get("CHROMIUM_PATH")
    if env and os.path.exists(env):
        return env
    candidates = [
        # Windows
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        # macOS
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
        # Linux / container
        "/usr/bin/chromium", "/usr/bin/chromium-browser",
        "/usr/bin/google-chrome", "/usr/bin/google-chrome-stable",
        "/usr/bin/microsoft-edge",
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    for name in ("chromium", "chromium-browser", "google-chrome", "google-chrome-stable", "chrome", "msedge"):
        found = shutil.which(name)
        if found:
            return found
    return None

def _html_to_pdf(html):
    """Render an HTML document to PDF bytes. Tries WeasyPrint first (needs the GTK
    runtime on Windows), then falls back to headless Chromium (Chrome/Edge).
    Returns PDF bytes, or None if no renderer is available."""
    try:
        from weasyprint import HTML as _WeasyHTML
        return _WeasyHTML(string=html).write_pdf()
    except Exception as exc:
        print(f"WeasyPrint unavailable ({type(exc).__name__}); trying headless browser.")
    exe = _find_chromium()
    if not exe:
        return None
    tmp = tempfile.mkdtemp(prefix="tcopdf_")
    html_path = os.path.join(tmp, "doc.html")
    pdf_path  = os.path.join(tmp, "doc.pdf")
    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
        file_url = "file:///" + html_path.replace("\\", "/")
        subprocess.run(
            [exe, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
             # Required when running headless Chromium as root inside a container;
             # harmless on desktop OSes.
             "--no-sandbox", "--disable-dev-shm-usage",
             f"--user-data-dir={tmp}", f"--print-to-pdf={pdf_path}", file_url],
            timeout=90, capture_output=True)
        if os.path.exists(pdf_path):
            with open(pdf_path, "rb") as f:
                return f.read()
        print("Headless browser did not produce a PDF.")
        return None
    except Exception as exc:
        print(f"Headless PDF generation failed: {exc}")
        return None
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

@app.route("/api/tco/save-graph", methods=["POST"])
@login_required
def tco_save_graph():
    """Archive a downloaded graph/comparison export to S3 under the customer's
    `downloaded_graphs/` subfolder. Renders the HTML to a real PDF server-side
    (WeasyPrint or headless Chromium); falls back to saving the HTML if no PDF
    renderer is available."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    customer = str(body.get("customer", "")).strip() or session.get("active_customer", "")
    html = body.get("html", "")
    name = str(body.get("name", "graph")).strip() or "graph"
    if not customer:
        return jsonify({"error": "customer is required."}), 400
    if not html:
        return jsonify({"error": "no content to save."}), 400
    user = session.get("username", "unknown")
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    safe = re.sub(r"[^\w.\-]+", "_", name)[:80]
    base = f"TCO-GUI/{user}/{customer}/downloaded_graphs/{safe}_{stamp}"
    try:
        pdf = _html_to_pdf(html)
        if pdf:
            key = f"{base}.pdf"
            _s3_client().put_object(Bucket=s3_bucket, Key=key,
                                    Body=pdf, ContentType="application/pdf")
            # print(f"Saved downloaded graph PDF to {key}")
            return jsonify({"ok": True, "key": key, "format": "pdf"})
        # Fallback: persist the HTML document (re-printable copy).
        key = f"{base}.html"
        _s3_client().put_object(Bucket=s3_bucket, Key=key,
                                Body=html.encode("utf-8"), ContentType="text/html")
        # print(f"Saved downloaded graph HTML (no PDF renderer) to {key}")
        return jsonify({"ok": True, "key": key, "format": "html"})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

@app.route("/api/tco/render-pdf", methods=["POST"])
@login_required
def tco_render_pdf():
    """Render an HTML document to a real PDF server-side (headless Chrome/Edge or
    WeasyPrint) and stream it back as a file download. Optionally archives a copy to
    the customer's downloaded_graphs/ folder. This avoids relying on the browser's
    window.print() dialog, which can produce an empty file for content with large
    inline SVG/tables."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    html = body.get("html", "")
    name = str(body.get("name", "export")).strip() or "export"
    customer = str(body.get("customer", "")).strip() or session.get("active_customer", "")
    archive = body.get("archive", True)
    if not html:
        return jsonify({"error": "no content to render."}), 400
    pdf = _html_to_pdf(html)
    if not pdf:
        return jsonify({"error": "No server-side PDF renderer is available on this deployment "
                                 "(needs Chrome/Edge or WeasyPrint)."}), 415
    safe = re.sub(r"[^\w.\-]+", "_", name)[:80] or "export"
    if archive and customer:
        try:
            user = session.get("username", "unknown")
            stamp = datetime.now().strftime("%Y%m%d%H%M%S")
            _s3_client().put_object(
                Bucket=s3_bucket,
                Key=f"TCO-GUI/{user}/{customer}/downloaded_graphs/{safe}_{stamp}.pdf",
                Body=pdf, ContentType="application/pdf")
        except Exception as exc:
            print(f"Warning: could not archive rendered PDF: {exc}")
    # Return the PDF base64-encoded inside JSON. The Werkzeug dev server (which this
    # app runs on) can drop a raw binary response body mid-transfer to the browser
    # ("Failed to fetch"); JSON transports reliably. The client decodes to a Blob.
    return jsonify({"ok": True, "filename": f"{safe}.pdf",
                    "pdf_b64": base64.b64encode(pdf).decode("ascii")})

@app.route("/api/tco/downloads", methods=["GET"])
@login_required
def tco_downloads():
    """List previously downloaded graph/comparison exports for a customer
    (objects under TCO-GUI/<user>/<customer>/downloaded_graphs/)."""
    global s3_region, s3_bucket
    customer = request.args.get("customer", "").strip() or session.get("active_customer", "")
    if not customer:
        return jsonify({"ok": True, "files": []})
    user = session.get("username", "unknown")
    prefix = f"TCO-GUI/{user}/{customer}/downloaded_graphs/"
    try:
        s3 = _s3_client()
        files = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                name = key.rsplit("/", 1)[-1]
                if not name:
                    continue
                files.append({
                    "key": key,
                    "name": name,
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                    "format": name.rsplit(".", 1)[-1].lower() if "." in name else "",
                })
        files.sort(key=lambda f: f["last_modified"], reverse=True)
        return jsonify({"ok": True, "files": files})
    except ClientError as exc:
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

@app.route("/api/tco/download-url", methods=["POST"])
@login_required
def tco_download_url():
    """Return a short-lived presigned URL to view or re-download a saved export.
    The key must live in the requesting user's own downloaded_graphs folder."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    key = str(body.get("key", "")).strip()
    download = bool(body.get("download"))
    user = session.get("username", "unknown")
    if not key or not key.startswith(f"TCO-GUI/{user}/") or not ("/downloaded_graphs/" in key or "/migration/" in key):
        return jsonify({"error": "Invalid or unauthorized key."}), 400
    fname = key.rsplit("/", 1)[-1]
    ctype = "application/pdf" if key.endswith(".pdf") else ("text/html" if key.endswith(".html") else "application/octet-stream")
    disp = ("attachment" if download else "inline") + f'; filename="{fname}"'
    try:
        url = _s3_client().generate_presigned_url(
            "get_object",
            Params={"Bucket": s3_bucket, "Key": key,
                    "ResponseContentDisposition": disp, "ResponseContentType": ctype},
            ExpiresIn=300)
        return jsonify({"ok": True, "url": url})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

@app.route("/api/tco/download-delete", methods=["POST"])
@login_required
def tco_download_delete():
    """Delete a saved downloaded export. The key must live in the requesting
    user's own downloaded_graphs folder."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    key = str(body.get("key", "")).strip()
    user = session.get("username", "unknown")
    if not key or not key.startswith(f"TCO-GUI/{user}/") or not ("/downloaded_graphs/" in key or "/migration/" in key):
        return jsonify({"error": "Invalid or unauthorized key."}), 400
    try:
        _s3_client().delete_object(Bucket=s3_bucket, Key=key)
        # print(f"Deleted downloaded export: {key}")
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

@app.route("/api/tco/ecan-cost-modes", methods=["GET"])
@login_required
def tco_ecan_cost_modes():
    """Return the Azure Native cost modes from ecan_config.json (the keys of
    cfg["cost_mode"]) plus which one is the lowest-cost default."""
    cfg_all = _load_ecan_config()
    if not cfg_all:
        return jsonify({"ok": True, "modes": [], "default": None})
    cfg = cfg_all[list(cfg_all.keys())[0]]
    cm = cfg.get("cost_mode") or {}
    modes = [{"key": k,
              "capacity_cost_normal": v.get("capacity_cost_normal"),
              "capacity_cost_encrypt": v.get("capacity_cost_encrypt"),
              "per_mbps_cost_per_array": v.get("per_mbps_cost_per_array")}
             for k, v in cm.items()]
    def _total(m):
        return ((m.get("capacity_cost_normal") or 0) + (m.get("capacity_cost_encrypt") or 0)
                + (m.get("per_mbps_cost_per_array") or 0))
    default = min(modes, key=_total)["key"] if modes else None
    return jsonify({"ok": True, "modes": modes, "default": default})

@app.route("/api/tco/detail", methods=["POST"])
@login_required
def tco_detail():
    """Return the raw per-group rows of a generated TCO (group_summary.csv).

    All commercial adjustments (discounts/margin/min-savings filter) are applied
    client-side, so this just returns the saved rows.
    """
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    key  = str(body.get("group_summary_key", "")).strip()
    if not key:
        return jsonify({"error": "group_summary_key is required."}), 400
    if not key.endswith("group_summary.csv"):
        return jsonify({"error": "group_summary_key must point to a group_summary.csv file."}), 400

    df = _load_df_from_s3(key)
    if df is None:
        return jsonify({"error": f"group_summary.csv not found in S3: {key}"}), 404
    return jsonify({
        "ok": True,
        "group_summary_key": key,
        "columns": list(df.columns),
        "rows": df.to_dict(orient="records"),
    })

@app.route("/api/tco/dfgroups", methods=["POST"])
@login_required
def tco_dfgroups():
    """Return the per-group, per-SKU cost comparison rows (df_groups.csv) for a
    generated TCO — used by the advanced view in the TCO Review tab."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    key  = str(body.get("df_groups_key", "")).strip()
    if not key:
        return jsonify({"error": "df_groups_key is required."}), 400
    if not key.endswith("df_groups.csv"):
        return jsonify({"error": "df_groups_key must point to a df_groups.csv file."}), 400

    df = _load_df_from_s3(key)
    if df is None:
        return jsonify({"error": f"df_groups.csv not found in S3: {key}"}), 404
    return jsonify({
        "ok": True,
        "df_groups_key": key,
        "columns": list(df.columns),
        "rows": df.to_dict(orient="records"),
    })

def evaluate_migration(df, params):
    """Evaluate a generated TCO's group data against the review parameters and
    build a capacity-metered migration schedule.

    params (all optional): everpure_discount, partner_margin, azure_native_discount,
    min_savings_rate (fractions 0-1), capacity_per_month (GiB of ORIGINAL capacity
    migrated per month). Returns a month-by-month schedule plus a summary.
    """
    def _f(x, d=0.0):
        try:
            v = float(x)
            return v if v == v else d   # guard NaN
        except (TypeError, ValueError):
            return d

    everpure_discount = _f(params.get("everpure_discount", 0))
    partner_margin    = _f(params.get("partner_margin", 0))
    azure_discount    = _f(params.get("azure_native_discount", 0))
    min_savings_rate  = _f(params.get("min_savings_rate", 0))
    cap_per_month     = _f(params.get("capacity_per_month", 0))
    months_factor     = 12  # annualize monthly costs

    # Adjust + filter groups by minimum savings rate
    groups = []
    for _, r in df.iterrows():
        license0 = _f(r.get("Y1 PSC Lic $"))
        infra    = _f(r.get("Y1 PSC Res $"))
        azure0   = _f(r.get("Y1 Azure Native $"))
        orig_cap = _f(r.get("Original Capacity"))
        lic_cap  = _f(r.get("Y1 PSC Licensed Capacity"))
        adj_license    = license0 * (1 - everpure_discount) * (1 + partner_margin)
        everpure_total = adj_license + infra
        adj_azure      = azure0 * (1 - azure_discount)
        savings        = adj_azure - everpure_total
        rate           = (savings / adj_azure) if adj_azure else 0.0
        if rate < min_savings_rate:
            continue
        groups.append({
            "group": r.get("desc"), "region": r.get("Region"),
            "orig_cap": orig_cap, "lic_cap": lic_cap,
            "adj_license": adj_license, "infra": infra,
            "everpure_total": everpure_total, "adj_azure": adj_azure,
            "azure_orig": azure0, "savings": savings, "rate": rate,
            "arrays": _f(r.get("Y1 PSC Array Count")),
        })

    # Migration order: primary = precedence tier (early < middle < late), secondary
    # = explicit per-group order (lower first), tertiary = highest savings rate.
    prec  = params.get("precedence", {}) or {}
    order = params.get("order", {}) or {}
    tier_rank = {"early": 0, "middle": 1, "late": 2}
    def _order_key(g):
        gid = str(g["group"])
        tier = tier_rank.get(str(prec.get(gid, "middle")).lower(), 1)
        try:
            ov = float(order.get(gid))
        except (TypeError, ValueError):
            ov = float("inf")            # unordered groups fall after ordered ones
        return (tier, ov, -g["rate"])
    groups.sort(key=_order_key)

    # Assign groups to months (cap_per_month = GiB of ORIGINAL capacity migrated per
    # month). Migration is a continuous capacity stream filled tightly so every month
    # uses its full capacity: each group occupies the capacity interval
    # (pos, pos+C]; a group that crosses a month boundary SPANS those months (and
    # appears in each with that month's migrated slice); when a group finishes
    # mid-month the next group starts in the same month to use the remaining capacity.
    # A group flips to Everpure only once fully migrated (its last month).
    pos = 0.0
    cap_by_month = {}   # month -> GiB migrated that month
    for g in groups:
        C = g["orig_cap"]
        g["_month_cap"] = {}   # month -> this group's GiB migrated that month
        if cap_per_month <= 0:
            g["_months"] = [1]; g["_start"] = 1; g["_done"] = 1
            g["_month_cap"][1] = C
            cap_by_month[1] = cap_by_month.get(1, 0.0) + C
            continue
        start_pos, end_pos = pos, pos + C
        pos = end_pos
        first_m = int(start_pos // cap_per_month) + 1
        last_m  = int(math.ceil(end_pos / cap_per_month)) or 1
        if last_m < first_m:
            last_m = first_m
        months = list(range(first_m, last_m + 1))
        g["_months"] = months; g["_start"] = first_m; g["_done"] = last_m
        for m in months:
            lo, hi = (m - 1) * cap_per_month, m * cap_per_month
            overlap = min(end_pos, hi) - max(start_pos, lo)
            if overlap > 0:
                g["_month_cap"][m] = overlap
                cap_by_month[m] = cap_by_month.get(m, 0.0) + overlap

    total_cap      = sum(g["orig_cap"] for g in groups)
    baseline_azure = sum(g["azure_orig"] for g in groups) * months_factor  # all-Azure, original native cost
    full_everpure  = sum(g["everpure_total"] for g in groups) * months_factor
    max_month = max(cap_by_month) if cap_by_month else 0

    # Fraction of a group migrated by the END of month m (0 before it starts, 1 once
    # done). Everpure cost begins as soon as a group starts migrating and scales with
    # the capacity moved; the not-yet-migrated remainder stays on Azure.
    def _frac(g, m):
        if m < g["_start"]:
            return 0.0
        if m >= g["_done"]:
            return 1.0
        oc = g["orig_cap"]
        if oc <= 0:
            return 1.0
        moved = sum(v for k, v in g["_month_cap"].items() if k <= m)
        return min(1.0, moved / oc)

    schedule = []
    cum_cap = 0.0
    for m in range(1, max_month + 1):
        in_progress = [g for g in groups if m in g["_months"]]   # migrating this month
        cap_add        = cap_by_month.get(m, 0.0)
        cum_cap       += cap_add
        # Prorate by migrated fraction: Everpure on the moved portion, Azure on the rest.
        ever_run  = sum(g["everpure_total"] * _frac(g, m) for g in groups) * months_factor
        azure_run = sum(g["azure_orig"] * (1 - _frac(g, m)) for g in groups) * months_factor
        total_cost = ever_run + azure_run
        cum_arrays = sum(g["arrays"] for g in groups if g["_start"] <= m)   # arrays deployed once a group starts
        schedule.append({
            "month":               m,
            "groups":              [g["group"] for g in in_progress],
            "count":               len(in_progress),
            "cap_migrated":        round(cap_add, 2),
            "cum_cap":             round(cum_cap, 2),
            "cum_pct":             round((cum_cap / total_cap * 100) if total_cap else 0, 1),
            "cum_arrays":          int(round(cum_arrays)),
            "everpure_yr":         round(ever_run, 2),
            "unmigrated_azure_yr": round(azure_run, 2),
            "total_cost_yr":       round(total_cost, 2),
            "savings_yr":          round(baseline_azure - total_cost, 2),
        })

    summary = {
        "included_groups":  len(groups),
        "total_capacity":   round(total_cap, 2),
        "months_to_migrate": max_month,
        "baseline_azure_yr": round(baseline_azure, 2),
        "full_everpure_yr":  round(full_everpure, 2),
        "full_savings_yr":   round(baseline_azure - full_everpure, 2),
    }

    # Yearly cost sums — actual spend integrated month-by-month over each year, with
    # Everpure prorated to migrated capacity (starts when a group begins migrating).
    num_years = max(5, -(-max_month // 12)) if max_month else 0   # ceil, min 5 years
    baseline_year = sum(g["azure_orig"] for g in groups) * 12     # all-Azure per year
    yearly = []
    for Y in range(1, num_years + 1):
        y_start, y_end = (Y - 1) * 12 + 1, Y * 12
        ever = azu = 0.0
        for m in range(y_start, y_end + 1):
            for g in groups:
                f = _frac(g, m)
                ever += g["everpure_total"] * f
                azu  += g["azure_orig"] * (1 - f)
        total = ever + azu
        yearly.append({
            "year":             Y,
            "everpure":         round(ever, 2),
            "unmigrated_azure": round(azu, 2),
            "total_cost":       round(total, 2),
            "baseline_azure":   round(baseline_year, 2),
            "savings":          round(baseline_year - total, 2),
        })
    # Per-group detail (also persisted to S3 by the caller)
    group_detail = [{
        "group":                  g["group"],
        "region":                 g["region"],
        "original_capacity":      round(g["orig_cap"], 2),
        "ec_licensed_capacity": round(g["lic_cap"], 2),
        "azure_native_monthly":   round(g["adj_azure"], 2),
        "everpure_license_monthly": round(g["adj_license"], 2),
        "everpure_infra_monthly": round(g["infra"], 2),
        "everpure_total_monthly": round(g["everpure_total"], 2),
        "savings_monthly":        round(g["savings"], 2),
        "savings_rate":           round(g["rate"], 4),
        "migration_month":        g.get("_start", 1),
        "migration_done_month":   g.get("_done", 1),
        "migration_months":       len(g.get("_months", [1])),
        "month_cap":              {int(k): round(v, 4) for k, v in (g.get("_month_cap") or {}).items()},
    } for g in groups]
    return {"schedule": schedule, "summary": summary, "groups": group_detail, "yearly": yearly}

@app.route("/api/tco/evaluate", methods=["POST"])
@login_required
def tco_evaluate():
    """Run the Python migration evaluation over a generated TCO's group data using
    the review parameters (discounts/margin/min-savings + capacity per month)."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    key  = str(body.get("group_summary_key", "")).strip()
    if not key or not key.endswith("group_summary.csv"):
        return jsonify({"error": "A valid group_summary_key is required."}), 400
    params = body.get("params", {}) or {}

    df = _load_df_from_s3(key)
    if df is None:
        return jsonify({"error": f"group_summary.csv not found in S3: {key}"}), 404
    try:
        result = evaluate_migration(df, params)
    except Exception as exc:
        return jsonify({"error": f"Evaluation failed: {exc}"}), 500

    # Persist the evaluation to S3 alongside the run, recording the non-default
    # parameter values in the same file.
    saved_key = None
    try:
        def _f(x, d=0.0):
            try:
                v = float(x); return v if v == v else d
            except (TypeError, ValueError):
                return d
        defaults = {"min_savings_rate": 0.20, "everpure_discount": 0.0,
                    "partner_margin": 0.0, "azure_native_discount": 0.0,
                    "capacity_per_month": 0.0}
        non_default = {k: params.get(k) for k, dv in defaults.items()
                       if _f(params.get(k, dv)) != dv}

        stamp  = datetime.now().strftime("%Y%m%d%H%M%S")
        prefix = key.rsplit("/", 1)[0] + "/"
        saved_key = f"{prefix}evaluation_{stamp}.csv"

        buf = StringIO()
        buf.write("# TCO migration evaluation\n")
        buf.write(f"# generated,{stamp}\n")
        buf.write("# non-default parameters\n")
        if non_default:
            for k, v in non_default.items():
                buf.write(f"# {k},{v}\n")
        else:
            buf.write("# (all parameters at default)\n")
        buf.write("#\n")
        pd.DataFrame(result["groups"]).to_csv(buf, index=False)

        _s3_client().put_object(
            Bucket=s3_bucket,
            Key=saved_key,
            Body=buf.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )
    except Exception as exc:
        print(f"Warning: could not save evaluation to S3: {exc}")
        saved_key = None

    return jsonify({"ok": True, "saved_key": saved_key, **result})

@app.route("/api/tco/migration-computes", methods=["POST"])
@login_required
def tco_migration_computes():
    """Month-by-month list of which compute instances (VMs) migrate each month.

    Uses the migration schedule (per-group per-month capacity) plus the run's
    parsed_data to attribute compute instances to months. For a group spanning
    multiple months the computes are spread by capacity; volumes with no compute
    name are reported per month as 'unknown hosts' (capacity + volume count),
    also spread proportionally."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    key = str(body.get("group_summary_key", "")).strip()
    params = body.get("params", {}) or {}
    if not key or not key.endswith("group_summary.csv") or "/tco/" not in key:
        return jsonify({"error": "A valid group_summary_key is required."}), 400
    df_sum = _load_df_from_s3(key)
    if df_sum is None:
        return jsonify({"error": f"group_summary.csv not found: {key}"}), 404
    try:
        result = evaluate_migration(df_sum, params)
    except Exception as exc:
        return jsonify({"error": f"Evaluation failed: {exc}"}), 500
    gdetail = result.get("groups", [])

    results_dir = key.split("/tco/")[0]
    df_parsed = _load_df_from_s3(f"{results_dir}/parsed_data.csv")
    cfg = _load_filter_json(_s3_client(), f"{results_dir}/source_data_config.json")
    if df_parsed is None or not cfg:
        return jsonify({"error": "Parsed data / config for this run not found."}), 404
    cols = df_parsed.columns.tolist()
    ci = _cfg_int(cfg, "count_compute")
    di = _cfg_int(cfg, "disk_size")
    has_compute = ci != -99 and 0 <= ci < len(cols)
    if di == -99 or di >= len(cols):
        return jsonify({"error": "disk_size column is not mapped for this dataset."}), 400
    disk_col = cols[di]
    comp_col = cols[ci] if has_compute else None
    gid_col  = "group_id" if "group_id" in df_parsed.columns else None

    df_parsed = df_parsed.copy()
    df_parsed["_cap"] = pd.to_numeric(df_parsed[disk_col], errors="coerce").fillna(0.0)
    if gid_col:
        df_parsed["_gid"] = pd.to_numeric(df_parsed[gid_col], errors="coerce")

    def _blank(v):
        s = ("" if v is None else str(v)).strip().lower()
        return s in ("", "nan", "none", "null", "no_compute", "-", "not_given",
                     "notgiven", "not given", "unknown", "unassigned", "n/a", "na", "none given")

    months = {}
    def _month(m):
        return months.setdefault(m, {"month": m, "compute_count": 0, "capacity": 0.0,
                                     "unknown_capacity": 0.0, "unknown_volumes": 0, "groups": []})

    for g in gdetail:
        gid = str(g["group"])
        mcap = {int(k): float(v) for k, v in (g.get("month_cap") or {}).items() if float(v) > 0}
        if not mcap:
            continue
        msorted = sorted(mcap.keys())
        gtotal = sum(mcap.values())
        try:
            gid_num = int(float(gid))
        except (TypeError, ValueError):
            gid_num = None
        if gid_col and gid_num is not None:
            grp = df_parsed[df_parsed["_gid"] == gid_num]
        elif gid_col:
            grp = df_parsed[df_parsed[gid_col].astype(str) == gid]
        else:
            grp = df_parsed.iloc[0:0]

        named = {}   # name -> [cap, vols]
        unk_cap, unk_vols = 0.0, 0
        if has_compute and comp_col:
            for nm, cap in zip(grp[comp_col].tolist(), grp["_cap"].tolist()):
                if _blank(nm):
                    unk_cap += float(cap); unk_vols += 1
                else:
                    e = named.setdefault(str(nm).strip(), [0.0, 0]); e[0] += float(cap); e[1] += 1
        else:
            unk_cap = float(grp["_cap"].sum()); unk_vols = int(len(grp))

        # cumulative capacity boundary per month (migration is capacity-metered)
        cum, run = [], 0.0
        for m in msorted:
            run += mcap[m]; cum.append((m, run))
        def _month_for(pos):
            for m, c in cum:
                if pos < c:
                    return m
            return msorted[-1]

        # assign named computes by capacity midpoint (largest first → proportional spread)
        per_comp = {m: [] for m in msorted}
        run = 0.0
        for name, (cap, vols) in sorted(named.items(), key=lambda kv: -kv[1][0]):
            per_comp[_month_for(run + cap / 2.0)].append({"name": name, "capacity": round(cap, 2), "volumes": int(vols)})
            run += cap

        # spread unknown-host capacity/volumes proportionally across the months
        per_unk = {m: {"capacity": 0.0, "volumes": 0} for m in msorted}
        if unk_cap > 0 or unk_vols > 0:
            w = {m: mcap[m] / gtotal for m in msorted}
            raw = {m: unk_vols * w[m] for m in msorted}
            floor = {m: int(raw[m]) for m in msorted}
            rem = unk_vols - sum(floor.values())
            for m in sorted(msorted, key=lambda m: -(raw[m] - floor[m]))[:max(0, rem)]:
                floor[m] += 1
            for m in msorted:
                per_unk[m] = {"capacity": round(unk_cap * w[m], 2), "volumes": floor[m]}

        for m in msorted:
            comps = per_comp[m]; unk = per_unk[m]
            if not comps and unk["capacity"] <= 0 and unk["volumes"] <= 0:
                continue
            e = _month(m)
            e["groups"].append({"group": gid, "computes": comps, "unknown": unk})
            e["compute_count"]    += len(comps)
            e["capacity"]         += sum(c["capacity"] for c in comps) + unk["capacity"]
            e["unknown_capacity"] += unk["capacity"]
            e["unknown_volumes"]  += unk["volumes"]

    out = []
    for m in sorted(months.keys()):
        e = months[m]
        e["capacity"] = round(e["capacity"], 2)
        e["unknown_capacity"] = round(e["unknown_capacity"], 2)
        out.append(e)

    # Capacity in the dataset that is NOT part of this migration — parsed volumes
    # whose group was not sized/priced (dropped by the TCO engine) and therefore
    # never enters the migration schedule. Break out the portion with no VM.
    scheduled = set()
    for g in gdetail:
        try:
            scheduled.add(int(float(str(g["group"]))))
        except (TypeError, ValueError):
            pass
    total_parsed = float(df_parsed["_cap"].sum())
    excluded = {"capacity": 0.0, "volumes": 0, "group_count": 0,
                "unknown_capacity": 0.0, "unknown_volumes": 0, "groups": []}
    if gid_col:
        excl = df_parsed[~df_parsed["_gid"].isin(scheduled)]
        if len(excl):
            excluded["capacity"] = round(float(excl["_cap"].sum()), 2)
            excluded["volumes"]  = int(len(excl))
            excluded["group_count"] = int(excl["_gid"].dropna().nunique())
            # Per-group VM (compute) breakdown for the groups NOT in the migration.
            for gid_val, grp in excl.groupby("_gid"):
                if pd.isna(gid_val):
                    continue
                named, ucap, uvol = {}, 0.0, 0
                if has_compute and comp_col:
                    for nm, cap in zip(grp[comp_col].tolist(), grp["_cap"].tolist()):
                        if _blank(nm):
                            ucap += float(cap); uvol += 1
                        else:
                            e = named.setdefault(str(nm).strip(), [0.0, 0]); e[0] += float(cap); e[1] += 1
                else:
                    ucap = float(grp["_cap"].sum()); uvol = int(len(grp))
                comps = [{"name": n, "capacity": round(c, 2), "volumes": int(v)}
                         for n, (c, v) in sorted(named.items(), key=lambda kv: -kv[1][0])]
                excluded["groups"].append({
                    "group": str(int(gid_val)),
                    "capacity": round(float(grp["_cap"].sum()), 2),
                    "compute_count": len(comps),
                    "computes": comps,
                    "unknown": {"capacity": round(ucap, 2), "volumes": int(uvol)},
                })
            excluded["groups"].sort(key=lambda g: -g["capacity"])
            excluded["unknown_capacity"] = round(sum(g["unknown"]["capacity"] for g in excluded["groups"]), 2)
            excluded["unknown_volumes"]  = int(sum(g["unknown"]["volumes"] for g in excluded["groups"]))

    return jsonify({"ok": True, "has_compute": bool(has_compute), "months": out,
                    "total_parsed_capacity": round(total_parsed, 2), "excluded": excluded})

@app.route("/api/tco/consolidate", methods=["POST"])
@login_required
def tco_consolidate():
    """Consolidate negative-savings groups into positive-savings groups.

    For each region, every group whose (commercially-adjusted) savings is negative is
    re-homed into a positive-savings group in the SAME region using an LPT balance on
    EC licensed capacity (each positive group's bin starts loaded with its own
    licensed capacity; the largest negative group goes to the least-loaded bin). The
    negative group's disk rows are rewritten in the parsed data to carry the target
    group's group_id + account/zone/vnet, and the result is saved as a NEW
    parsed_data.csv (with copied config + cost siblings) that the caller then runs
    through /api/results/run-analysis.

    Because that re-homing implies a VNet peering between the source and target VNets
    (intra-region), this also prices the intra-region peering data transfer, assuming
    `peering_ratio_gib_per_tib` GiB/month of peered traffic per TiB of licensed
    capacity moved.

    Body: {group_summary_key, commercials:{everpure_discount, partner_margin,
           azure_discount}, peering_ratio_gib_per_tib}
    """
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    key = str(body.get("group_summary_key", "")).strip()
    if not key or not key.endswith("group_summary.csv") or "/tco/" not in key:
        return jsonify({"error": "A valid group_summary_key is required."}), 400
    comm = body.get("commercials", {}) or {}
    def _f(v, d=0.0):
        try: return float(v)
        except (TypeError, ValueError): return d
    e_disc = _f(comm.get("everpure_discount"))
    margin = _f(comm.get("partner_margin"))
    a_disc = _f(comm.get("azure_discount"))
    ratio = _f(body.get("peering_ratio_gib_per_tib"), 512.0)
    if ratio < 0:
        ratio = 0.0
    # Optionally also consolidate negative groups that have no positive group in
    # their region by merging them together (into the largest-capacity one).
    merge_neg = bool(body.get("merge_negatives_without_positive", False))
    # Negative groups the user chose to leave out of the consolidation entirely.
    excluded = set()
    for gv in (body.get("excluded_groups", []) or []):
        try: excluded.add(int(float(gv)))
        except (TypeError, ValueError): pass

    df_sum = _load_df_from_s3(key)
    if df_sum is None:
        return jsonify({"error": f"group_summary.csv not found: {key}"}), 404
    results_dir = key.split("/tco/")[0]
    df_parsed = _load_df_from_s3(f"{results_dir}/parsed_data.csv")
    cfg = _load_filter_json(_s3_client(), f"{results_dir}/source_data_config.json")
    if df_parsed is None or not cfg:
        return jsonify({"error": "Parsed data / config for this run not found."}), 404

    cols = df_parsed.columns.tolist()
    def _colname(cfg_key):
        idx = _cfg_int(cfg, cfg_key)
        return cols[idx] if 0 <= idx < len(cols) else None
    account_col = _colname("subscription_or_account_id")
    zone_col    = _colname("zone")
    vnet_col    = _colname("vnet_or_vpc")
    if "group_id" not in cols:
        return jsonify({"error": "parsed_data has no group_id column; cannot consolidate."}), 400
    gid_col = "group_id"

    def _num(v):
        try: return float(v)
        except (TypeError, ValueError): return 0.0
    def _blank(v):
        s = ("" if v is None else str(v)).strip().lower()
        return s in ("", "nan", "none", "null", "—", "-", "n/a", "na")
    def _vblank(v):   # VNet-specific: this app stores a missing VNet as the literal "blank"
        return _blank(v) or ("" if v is None else str(v)).strip().lower() == "blank"
    def _zstr(v):
        if _blank(v): return ""
        try:
            fv = float(v)
            if fv == int(fv): return str(int(fv))
        except (TypeError, ValueError):
            pass
        return str(v).strip()

    # ── Per-group info from the summary, with commercially-adjusted savings so the
    #    negative/positive split matches the TCO Review page ──
    groups = {}
    for _, r in df_sum.iterrows():
        desc = r.get("desc")
        try:
            gid = int(float(desc))
        except (TypeError, ValueError):
            continue
        lic0   = _num(r.get("Y1 PSC Lic $"))
        infra  = _num(r.get("Y1 PSC Res $"))
        azure0 = _num(r.get("Y1 Azure Native $"))
        adj_lic  = lic0 * (1 - e_disc) * (1 + margin)
        everpure = adj_lic + infra
        adj_azure = azure0 * (1 - a_disc)
        groups[gid] = {
            "group": gid,
            "region":  str(r.get("Region", "")),
            "account": r.get("Account", ""),
            "zone":    r.get("Availability Zone", ""),
            "vnet":    r.get("VNet", ""),
            "orig_cap": _num(r.get("Original Capacity")),
            "lic_cap":  _num(r.get("Y1 PSC Licensed Capacity")),
            "lic0":    lic0,
            "infra":   infra,
            "savings":  adj_azure - everpure,
        }

    # Raw (pre-discount) Everpure components summed over ALL original groups — the
    # "before" side of the consolidation comparison (recomputed at any commercials).
    before_license_monthly = sum(g["lic0"] for g in groups.values())
    before_infra_monthly   = sum(g["infra"] for g in groups.values())
    before_group_count     = len(groups)

    all_neg_ids = [gid for gid, g in groups.items() if g["savings"] < 0]
    # Candidate negatives = negative groups the user did NOT exclude.
    neg = [g for g in groups.values() if g["savings"] < 0 and g["group"] not in excluded]
    pos = [g for g in groups.values() if g["savings"] > 0]

    # ── LPT balance on licensed capacity, per region (negatives → positives) ──
    pos_by_region = {}
    for p in pos:
        pos_by_region.setdefault(p["region"], []).append(p)
    neg_to_pos = {}
    regions_no_positive = set()
    for region_name in sorted({g["region"] for g in neg}):
        region_pos = pos_by_region.get(region_name, [])
        if not region_pos:
            regions_no_positive.add(region_name)
            continue
        load = {p["group"]: p["lic_cap"] for p in region_pos}
        for g in sorted([n for n in neg if n["region"] == region_name], key=lambda g: -g["lic_cap"]):
            best = min(load, key=lambda k: load[k])
            load[best] += g["lic_cap"]
            neg_to_pos[g["group"]] = best

    # ── Optional: consolidate negatives that have NO positive to map to, but do
    #    have other negatives in the same region — fold them into the largest one ──
    if merge_neg:
        still_no_pos = set()
        for region_name in sorted(regions_no_positive):
            region_neg = [n for n in neg if n["region"] == region_name]
            if len(region_neg) >= 2:
                host = max(region_neg, key=lambda g: g["lic_cap"])
                for g in region_neg:
                    if g["group"] != host["group"]:
                        neg_to_pos[g["group"]] = host["group"]
            else:
                still_no_pos.add(region_name)   # a lone negative has nothing to merge with
        regions_no_positive = still_no_pos

    # ── Target identities pulled from the ORIGINAL parsed data (native encoding) ──
    gid_num = pd.to_numeric(df_parsed[gid_col], errors="coerce")
    tgt_ident = {}
    for pos_gid in set(neg_to_pos.values()):
        sub = df_parsed[gid_num == pos_gid]
        ident = {}
        if len(sub):
            if account_col: ident["account"] = sub[account_col].iloc[0]
            if zone_col:    ident["zone"]    = sub[zone_col].iloc[0]
            if vnet_col:    ident["vnet"]    = sub[vnet_col].iloc[0]
        tgt_ident[pos_gid] = ident

    # ── Apply the mapping to the parsed data (region is never changed) ──
    con = df_parsed.copy()
    con["_gid"] = gid_num
    moved_rows = 0
    for neg_gid, pos_gid in neg_to_pos.items():
        m = con["_gid"] == neg_gid
        cnt = int(m.sum())
        if not cnt:
            continue
        moved_rows += cnt
        con.loc[m, gid_col] = pos_gid
        ident = tgt_ident.get(pos_gid, {})
        if account_col and "account" in ident: con.loc[m, account_col] = ident["account"]
        if zone_col    and "zone"    in ident: con.loc[m, zone_col]    = ident["zone"]
        if vnet_col    and "vnet"    in ident: con.loc[m, vnet_col]    = ident["vnet"]
    con = con.drop(columns=["_gid"])

    # ── Persist the consolidated dataset + copy siblings so run-analysis works ──
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    out_dir = f"{results_dir}/consolidations/consolidate_{stamp}"
    new_parsed_key = f"{out_dir}/parsed_data.csv"
    s3 = _s3_client()
    try:
        buf = StringIO(); con.to_csv(buf, index=False)
        s3.put_object(Bucket=s3_bucket, Key=new_parsed_key,
                      Body=buf.getvalue().encode("utf-8"), ContentType="text/csv")
        for sib in ("source_data_config.json", "azure_managed_disk_costs.csv", "ec_infra_resource_costs.csv"):
            try:
                s3.copy_object(Bucket=s3_bucket,
                               CopySource={"Bucket": s3_bucket, "Key": f"{results_dir}/{sib}"},
                               Key=f"{out_dir}/{sib}")
            except ClientError as exc:
                if exc.response["Error"]["Code"] not in ("404", "NoSuchKey"):
                    raise
    except ClientError as exc:
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": f"Could not save consolidated dataset: {exc}"}), 500

    # ── VNet peering (intra-region, source→target) + data-transfer cost ──
    regions_involved = sorted({groups[n]["region"] for n in neg_to_pos})
    rates = _azure_vnet_peering_rates(regions_involved)

    unknown_names, unk_seq = {}, [0]
    def _vnet_name(g):
        if not _vblank(g["vnet"]):
            return str(g["vnet"]).strip()
        if g["group"] in unknown_names:
            return unknown_names[g["group"]]
        i = unk_seq[0]; unk_seq[0] += 1
        letters, i2 = "", i
        while True:
            letters = chr(ord("A") + (i2 % 26)) + letters
            i2 = i2 // 26 - 1
            if i2 < 0:
                break
        name = f"unknown{letters}"
        unknown_names[g["group"]] = name
        return name

    sources = set(neg_to_pos.keys())
    host_ids = {p for p in neg_to_pos.values() if groups.get(p, {}).get("savings", 0) < 0}

    peers = []
    tot_traffic = tot_cost = 0.0
    for neg_gid, pos_gid in sorted(neg_to_pos.items()):
        src, tgt = groups[neg_gid], groups[pos_gid]
        rate = rates.get(src["region"], {"ingress": 0.01, "egress": 0.01, "per_gb": 0.02,
                                          "source": "default", "currency": "USD"})
        src_lic_tib = src["lic_cap"] / 1024.0
        traffic = src_lic_tib * ratio
        cost = traffic * rate["per_gb"]
        peers.append({
            "region": src["region"],
            "target_group": pos_gid, "target_account": _zstr(tgt["account"]) or str(tgt["account"]),
            "target_zone": _zstr(tgt["zone"]), "target_vnet": _vnet_name(tgt),
            "target_negative": tgt["savings"] < 0,
            "source_group": neg_gid, "source_account": _zstr(src["account"]) or str(src["account"]),
            "source_zone": _zstr(src["zone"]), "source_vnet": _vnet_name(src),
            "source_lic_tib": round(src_lic_tib, 4),
            "peered_traffic_gib": round(traffic, 2),
            "rate_per_gb": rate["per_gb"], "rate_source": rate.get("source", "default"),
            "monthly_cost": round(cost, 2), "annual_cost": round(cost * 12, 2),
            "same_vnet": (not _vblank(src["vnet"]) and not _vblank(tgt["vnet"])
                          and str(src["vnet"]).strip() == str(tgt["vnet"]).strip()),
        })
        tot_traffic += traffic
        tot_cost += cost

    mapping = [{"source_group": n, "target_group": p, "region": groups[n]["region"],
                "source_orig_cap": round(groups[n]["orig_cap"], 2),
                "source_lic_cap":  round(groups[n]["lic_cap"], 2),
                "source_savings":  round(groups[n]["savings"], 2),
                "target_negative": groups[p]["savings"] < 0}
               for n, p in sorted(neg_to_pos.items())]
    # Negatives left unchanged on Azure — either excluded by the user or with no
    # target available (no positive group, and no negative-merge partner).
    unmapped = []
    for gid in all_neg_ids:
        if gid in sources or gid in host_ids:
            continue
        g = groups[gid]
        unmapped.append({"group": gid, "region": g["region"],
                         "orig_cap": round(g["orig_cap"], 2), "lic_cap": round(g["lic_cap"], 2),
                         "savings": round(g["savings"], 2),
                         "reason": "excluded by user" if gid in excluded else "no target in region"})

    result = {
        "ok": True,
        "parsed_data_key": new_parsed_key,
        "out_dir": out_dir,
        "counts": {"negative_groups": len(all_neg_ids), "positive_groups": len(pos),
                   "mapped": len(neg_to_pos),
                   "mapped_to_negative": sum(1 for p in neg_to_pos.values() if groups[p]["savings"] < 0),
                   "negative_hosts": len(host_ids),
                   "excluded_by_user": sum(1 for g in all_neg_ids if g in excluded),
                   "moved_rows": moved_rows, "unmapped": len(unmapped)},
        "merge_negatives_without_positive": merge_neg,
        "mapping": mapping,
        "unmapped_negatives": unmapped,
        "regions_no_positive": sorted(regions_no_positive),
        "peering_ratio_gib_per_tib": ratio,
        "peering_rates": rates,
        "peers": peers,
        "peering_totals": {"traffic_gib": round(tot_traffic, 2),
                           "monthly_cost": round(tot_cost, 2),
                           "annual_cost": round(tot_cost * 12, 2)},
        # "before" = raw (pre-discount) Everpure components over ALL original groups,
        # so the before/after comparison can be recomputed at any commercial settings.
        "before": {"license_monthly": round(before_license_monthly, 4),
                   "infra_monthly": round(before_infra_monthly, 4),
                   "groups": before_group_count,
                   "source_group_summary_key": key},
        "commercials": {"everpure_discount": e_disc, "partner_margin": margin, "azure_discount": a_disc},
    }
    # Persist the consolidation result next to the new parsed data so the
    # Consolidation view can be restored when the run is re-opened later.
    try:
        s3.put_object(Bucket=s3_bucket, Key=f"{out_dir}/consolidation.json",
                      Body=json.dumps(result).encode("utf-8"), ContentType="application/json")
    except Exception as exc:
        print(f"Warning: could not persist consolidation.json: {exc}")
    return jsonify(result)

@app.route("/api/tco/consolidation-detail", methods=["GET"])
@login_required
def tco_consolidation_detail():
    """Return the saved consolidation.json for a consolidated run (mapping, peers,
    peering, before snapshot, etc.) so the Consolidation view can be restored when the
    run is re-opened. 404 if this run is not a consolidation / has no sidecar."""
    global s3_region, s3_bucket
    key = str(request.args.get("group_summary_key", "")).strip()
    if not key or not key.endswith("group_summary.csv") or "/tco/" not in key:
        return jsonify({"error": "A valid group_summary_key is required."}), 400
    results_dir = key.split("/tco/")[0]
    data = _load_filter_json(_s3_client(), f"{results_dir}/consolidation.json")
    if not data:
        return jsonify({"ok": False, "error": "No saved consolidation for this run."}), 404
    return jsonify({"ok": True, "consolidation": data})

@app.route("/api/tco/save-migration-plan", methods=["POST"])
@login_required
def tco_save_migration_plan():
    """Save a named migration plan (an evaluation) as a CSV in a `migration/`
    subfolder under the run's tco/ folder."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    key  = str(body.get("group_summary_key", "")).strip()
    name = str(body.get("name", "")).strip()
    params = body.get("params", {}) or {}
    if not key or not key.endswith("group_summary.csv") or "/tco/" not in key:
        return jsonify({"error": "A valid group_summary_key (in a tco/ folder) is required."}), 400
    if not name:
        return jsonify({"error": "A plan name is required."}), 400
    df = _load_df_from_s3(key)
    if df is None:
        return jsonify({"error": f"group_summary.csv not found in S3: {key}"}), 404
    try:
        result = evaluate_migration(df, params)
    except Exception as exc:
        return jsonify({"error": f"Evaluation failed: {exc}"}), 500

    prec  = params.get("precedence", {}) or {}
    order = params.get("order", {}) or {}
    # Per-group plan rows, annotated with the chosen precedence/order.
    rows = []
    for g in result.get("groups", []):
        gid = str(g.get("group"))
        rows.append({**g,
                     "precedence": prec.get(gid, "middle"),
                     "order": order.get(gid, "")})

    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    safe  = re.sub(r"[^\w.\-]+", "_", name)[:80]
    prefix = key.rsplit("/", 1)[0] + "/migration/"       # .../tco/<id>/migration/
    plan_key = f"{prefix}{safe}_{stamp}.csv"
    summary = result.get("summary", {})
    cap_gib = 0.0
    try:
        cap_gib = float(params.get("capacity_per_month", 0) or 0)
    except (TypeError, ValueError):
        cap_gib = 0.0
    try:
        buf = StringIO()
        buf.write(f"# Migration plan,{name}\n")
        buf.write(f"# generated,{stamp}\n")
        buf.write(f"# capacity_per_month_TiB,{round(cap_gib / 1024, 4)}\n")
        buf.write(f"# capacity_per_month_GiB,{cap_gib}\n")
        for k in ("everpure_discount", "partner_margin", "azure_native_discount", "min_savings_rate"):
            if k in params:
                buf.write(f"# {k},{params.get(k)}\n")
        buf.write(f"# included_groups,{summary.get('included_groups', 0)}\n")
        buf.write(f"# months_to_migrate,{summary.get('months_to_migrate', 0)}\n")
        buf.write("#\n")
        pd.DataFrame(rows).to_csv(buf, index=False)
        _s3_client().put_object(Bucket=s3_bucket, Key=plan_key,
                                Body=buf.getvalue().encode("utf-8"), ContentType="text/csv")
        # print(f"Saved migration plan to {plan_key}")
    except Exception as exc:
        return jsonify({"error": f"Could not save plan: {exc}"}), 500
    return jsonify({"ok": True, "key": plan_key, "name": name})

@app.route("/api/tco/migration-plans", methods=["GET"])
@login_required
def tco_migration_plans():
    """List saved migration plans (CSVs under the run's tco/<id>/migration/ folder)."""
    global s3_region, s3_bucket
    key = str(request.args.get("group_summary_key", "")).strip()
    if not key or not key.endswith("group_summary.csv") or "/tco/" not in key:
        return jsonify({"ok": True, "plans": []})
    prefix = key.rsplit("/", 1)[0] + "/migration/"
    try:
        s3 = _s3_client()
        plans = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                name = k.rsplit("/", 1)[-1]
                if not name.lower().endswith(".csv"):
                    continue
                plans.append({"key": k, "name": name, "size": obj["Size"],
                              "last_modified": obj["LastModified"].isoformat()})
        plans.sort(key=lambda p: p["last_modified"], reverse=True)
        return jsonify({"ok": True, "plans": plans})
    except ClientError as exc:
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

@app.route("/api/tco/migration-plan", methods=["GET"])
@login_required
def tco_migration_plan_detail():
    """Return one saved migration plan's per-group timeline (start/done month) so
    the growth-projection view can fold migration timing into its cost curves."""
    global s3_region, s3_bucket
    user = session.get("username", "")
    key  = str(request.args.get("key", "")).strip()
    if not key or not key.startswith(f"TCO-GUI/{user}/") or "/migration/" not in key or not key.lower().endswith(".csv"):
        return jsonify({"error": "A valid saved-plan key is required."}), 400
    try:
        obj = _s3_client().get_object(Bucket=s3_bucket, Key=key)
        text = obj["Body"].read().decode("utf-8")
        # The plan CSV is prefixed with '#' comment lines (name/params); skip them.
        df = pd.read_csv(StringIO(text), comment="#")
    except ClientError as exc:
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": f"Could not read plan: {exc}"}), 500

    def _int(v, default):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return default
    def _flt(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    # Parse the '# key,value' header comments for the commercial params + capacity
    # so applying a plan can restore the commercials it was built with.
    hdr = {}
    for line in text.splitlines():
        if not line.startswith("#"):
            continue
        body = line[1:].strip()
        if "," in body:
            k, v = body.split(",", 1)
            hdr[k.strip()] = v.strip()
    params = {
        "everpure_discount":     _flt(hdr.get("everpure_discount")),
        "partner_margin":        _flt(hdr.get("partner_margin")),
        "azure_native_discount": _flt(hdr.get("azure_native_discount")),
        "min_savings_rate":      _flt(hdr.get("min_savings_rate")),
        "capacity_per_month_TiB": _flt(hdr.get("capacity_per_month_TiB")),
    }

    groups = []
    for _, r in df.iterrows():
        gid = str(r.get("group"))
        if gid in ("", "nan", "None"):
            continue
        start = _int(r.get("migration_month"), 1)
        done  = _int(r.get("migration_done_month"), start)
        if done < start:
            done = start
        prec = str(r.get("precedence")) if "precedence" in df.columns and str(r.get("precedence")) not in ("nan", "None", "") else "middle"
        order_v = r.get("order") if "order" in df.columns else None
        order = None if order_v in (None, "") or str(order_v) in ("nan", "None") else _int(order_v, None)
        groups.append({"group": gid, "start": start, "done": done,
                       "precedence": prec, "order": order})
    name = key.rsplit("/", 1)[-1]
    return jsonify({"ok": True, "name": name, "params": params, "groups": groups})

@app.route("/api/tco/project", methods=["POST"])
@login_required
def tco_project():
    """Project cost forward as capacity grows, on a monthly/quarterly/yearly cycle.
    Re-runs the sizing engine per period against the run's original inputs and
    saves the resulting time series to the run's tco/ folder."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    key  = str(body.get("group_summary_key", "")).strip()
    if not key or not key.endswith("group_summary.csv") or "/tco/" not in key:
        return jsonify({"error": "A valid group_summary_key (in a tco/ folder) is required."}), 400
    yearly_growth = float(body.get("yearly_growth", 0) or 0)
    frequency = str(body.get("frequency", "yearly")).strip().lower()
    if frequency not in ("monthly", "quarterly", "yearly"):
        frequency = "yearly"

    results_dir = key.split("/tco/")[0]         # .../<run-datetime>/results
    tco_dir     = key.rsplit("/", 1)[0] + "/"   # .../tco/<id>/
    try:
        df_parsed     = _load_df_from_s3(f"{results_dir}/parsed_data.csv")
        df_azure_disk = _load_df_from_s3(f"{results_dir}/azure_managed_disk_costs.csv")
        df_ec_infra   = _load_df_from_s3(f"{results_dir}/ec_infra_resource_costs.csv")
        if df_parsed is None or df_ec_infra is None or df_azure_disk is None:
            return jsonify({"error": "Original run inputs (parsed_data / cost CSVs) not found for this run."}), 404

        s3 = _s3_client()
        cfg_obj = s3.get_object(Bucket=s3_bucket, Key=f"{results_dir}/source_data_config.json")
        source_data_config = json.loads(cfg_obj["Body"].read().decode("utf-8"))

        # Recover the run's analysis params from meta.json (falls back to {})
        params = {}
        try:
            meta_obj = s3.get_object(Bucket=s3_bucket, Key=key.replace("group_summary.csv", "meta.json"))
            params = (json.loads(meta_obj["Body"].read().decode("utf-8")) or {}).get("params", {}) or {}
        except Exception:
            params = {}
        params["source_data_config"] = source_data_config
        params["ec_data"] = _load_ec_list(params.get("skus", ["V10MP2R2", "V20MP2R2"]))

        result = project_growth_over_time(params, df_parsed, df_azure_disk, df_ec_infra,
                                          results_dir, yearly_growth, frequency)
    except Exception as exc:
        return jsonify({"error": f"Projection failed: {exc}"}), 500

    # Save the projected time series to S3 alongside the run
    saved_key = None
    try:
        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        saved_key = f"{tco_dir}projection_{frequency}_{stamp}.csv"
        buf = StringIO()
        buf.write("# TCO growth projection\n")
        buf.write(f"# yearly_growth,{yearly_growth}\n# frequency,{frequency}\n#\n")
        pd.DataFrame(result.get("series", [])).to_csv(buf, index=False)
        _s3_client().put_object(Bucket=s3_bucket, Key=saved_key,
                                Body=buf.getvalue().encode("utf-8"), ContentType="text/csv")
    except Exception as exc:
        print(f"Warning: could not save projection to S3: {exc}")
        saved_key = None

    return jsonify({"ok": True, "saved_key": saved_key, **result})

@app.route("/api/tco/projection", methods=["POST"])
@login_required
def tco_projection_load():
    """Load the growth projection that was saved at generation time (projection.json)
    for a selected run, for display in the TCO Review tab."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    key  = str(body.get("group_summary_key", "")).strip()
    if not key or not key.endswith("group_summary.csv"):
        return jsonify({"error": "A valid group_summary_key is required."}), 400
    proj_key = key.replace("group_summary.csv", "projection.json")
    try:
        obj = _s3_client().get_object(Bucket=s3_bucket, Key=proj_key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return jsonify({"ok": True, "projection": data})
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return jsonify({"ok": True, "projection": None})   # no projection saved for this run
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

# ══════════════════════════════════════════════════════════
#  Results / Parsed Data API
# ══════════════════════════════════════════════════════════
class _BadData(Exception):
    """disk_size/disk_type not mapped — cannot compute Results metrics."""
    pass

def _compute_detail_metrics(df, cfg_data):
    """Compute the Results-page summary (KPIs + region/disk-type/group
    breakdowns) from a parsed dataframe and its source_data_config. Works on any
    row subset, so it also powers the filtered ("Apply") summary. Raises
    _BadData when disk_size or disk_type are unmapped."""
    df = df.copy()
    region_column_value = _cfg_int(cfg_data, "region")
    zone_column_value = _cfg_int(cfg_data, "zone")
    subscription_column_value = _cfg_int(cfg_data, "subscription_or_account_id")
    vnet_column_value = _cfg_int(cfg_data, "vnet_or_vpc")
    disk_type_column_value = _cfg_int(cfg_data, "disk_type")
    disk_size_column_value = _cfg_int(cfg_data, "disk_size")
    disk_status_column_value = _cfg_int(cfg_data, "disk_status")
    count_compute_column_value = _cfg_int(cfg_data, "count_compute")

    col_names = df.columns.tolist()

    no_compute_flag = False
    if count_compute_column_value == -99:
        no_compute_flag = True
    else:
        compute_column_name = col_names[count_compute_column_value]

    no_region_flag = False
    if region_column_value == -99:
        no_region_flag = True
    else:
        region_column_name = col_names[region_column_value]

    no_zone_flag = False
    if zone_column_value == -99:
        no_zone_flag = True
    else:
        zone_column_name = col_names[zone_column_value]
    no_sub_flag = False
    if subscription_column_value == -99:
        no_sub_flag = True
    else:
        subscription_column_name = col_names[subscription_column_value]
    no_vnet_flag = False
    if vnet_column_value == -99:
        no_vnet_flag = True
    else:
        vnet_column_name = col_names[vnet_column_value]
    no_disk_size_flag = False
    if disk_size_column_value == -99:
        no_disk_size_flag = True
    else:
        disk_size_column_name = col_names[disk_size_column_value]
    no_disk_type_flag = False
    if disk_type_column_value == -99:
        no_disk_type_flag = True
    else:
        disk_type_column_name = col_names[disk_type_column_value]

    metrics = {}
    metrics["row_count"] = len(df)
    if no_disk_size_flag or no_disk_type_flag:
        raise _BadData()

    metrics["total_capacity_gib"] = round(float(pd.to_numeric(df[disk_size_column_name], errors="coerce").sum()), 2)
    metrics["capacity_column"] = disk_size_column_name
    df["total_cost"] = pd.to_numeric(df["total_cost"], errors="coerce")
    metrics["total_cost"] = round(float(df["total_cost"].sum()), 2)
    metrics["cost_column"] = "total_cost"
    # Azure cost breakdown: capacity, performance (provisioned IOPS + throughput), and
    # snapshots. The per-disk components are stored at parse time; sum them here so the
    # Results view can show how the Azure cost splits. Guarded for safety.
    def _col_sum(name):
        return round(float(pd.to_numeric(df[name], errors="coerce").fillna(0).sum()), 2)
    metrics["capacity_cost"] = _col_sum("cap_cost") if "cap_cost" in df.columns else None
    if {"iops_cost", "mbps_cost"}.issubset(df.columns):
        metrics["performance_cost"] = round(
            float((pd.to_numeric(df["iops_cost"], errors="coerce").fillna(0)
                   + pd.to_numeric(df["mbps_cost"], errors="coerce").fillna(0)).sum()), 2)
    else:
        metrics["performance_cost"] = None
    metrics["snapshot_cost"] = _col_sum("snap_cost") if "snap_cost" in df.columns else None
    # Parse-time snapshot rate baked into this dataset (fraction). Shown on selection and
    # used to pre-fill the analysis slider. Defaults to main2's historical 0.1.
    try:
        metrics["parsed_snapshot_rate"] = float((cfg_data or {}).get("monthly_snapshot_rate", 0.1))
    except (TypeError, ValueError):
        metrics["parsed_snapshot_rate"] = 0.1
    metrics["num_groups"] = int(df["group_id"].nunique())
    if no_region_flag:
        metrics["region_breakdown"] = []
        metrics["region_count"] = 0
    else:
        region_summary = (
            df.groupby(region_column_name)
            .agg(
                disk_count=pd.NamedAgg(column=region_column_name, aggfunc="count"),
                total_capacity=pd.NamedAgg(column=disk_size_column_name,
                                           aggfunc=lambda x: round(float(pd.to_numeric(x, errors="coerce").sum()), 2)),
                total_cost=pd.NamedAgg(column="total_cost",
                                       aggfunc=lambda x: round(float(pd.to_numeric(x, errors="coerce").sum()), 2))
            )
            .reset_index()
            .rename(columns={region_column_name: "region"})
            .to_dict(orient="records")
        )
        metrics["region_breakdown"] = region_summary
        metrics["region_count"] = len(region_summary)
    dtype_summary = (
        df.groupby(disk_type_column_name)
        .agg(count=pd.NamedAgg(column=disk_type_column_name, aggfunc="count"))
        .reset_index()
        .rename(columns={disk_type_column_name: "disk_type"})
        .sort_values("count", ascending=False)
        .to_dict(orient="records")
    )
    metrics["disk_type_breakdown"] = dtype_summary

    cost_by_type = (
        df.groupby(disk_type_column_name)["total_cost"]
        .sum()
        .round(2)
        .reset_index()
        .rename(columns={disk_type_column_name: "disk_type", "total_cost": "total_cost"})
        .sort_values("total_cost", ascending=False)
        .to_dict(orient="records")
    )
    metrics["cost_by_disk_type"] = cost_by_type

    if "group_id" in df.columns:
        reg_col_gb   = region_column_name  if not no_region_flag   else None
        cap_col_gb   = disk_size_column_name
        cost_col_gb  = "total_cost"
        dtype_col_gb = disk_type_column_name
        zone_col    = zone_column_name if not no_zone_flag   else None
        account_col = subscription_column_name if not no_sub_flag else None
        network_col = vnet_column_name if not no_vnet_flag else None
        compute_col = compute_column_name if not no_compute_flag else None

        group_rows = []
        for gid, gdf in df.groupby("group_id"):
            row = {"group_id": int(gid)}
            row["region"]       = str(gdf[reg_col_gb].iloc[0])   if reg_col_gb   else "—"
            row["zone"]         = str(gdf[zone_col].iloc[0])      if zone_col     else "—"
            row["account"]      = str(gdf[account_col].iloc[0])   if account_col  else "—"
            row["network"]      = str(gdf[network_col].iloc[0])   if network_col  else "—"
            row["volume_count"] = int(len(gdf))
            row["compute_count"] = int(gdf[compute_col].nunique()) if compute_col else 0
            row["total_capacity"] = round(float(pd.to_numeric(gdf[cap_col_gb], errors="coerce").sum()), 2) if cap_col_gb else None
            row["total_cost"]     = round(float(pd.to_numeric(gdf[cost_col_gb], errors="coerce").sum()), 2) if cost_col_gb else None
            if dtype_col_gb:
                row["disk_type_breakdown"] = (
                    gdf.groupby(dtype_col_gb)
                       .agg(
                           count=pd.NamedAgg(column=dtype_col_gb, aggfunc="count"),
                           total_capacity=pd.NamedAgg(
                               column=cap_col_gb if cap_col_gb else dtype_col_gb,
                               aggfunc=lambda x: round(float(pd.to_numeric(x, errors="coerce").sum()), 2)
                           )
                       )
                       .reset_index()
                       .rename(columns={dtype_col_gb: "disk_type"})
                       .sort_values("total_capacity", ascending=False)
                       .to_dict(orient="records")
                )
            else:
                row["disk_type_breakdown"] = []
            group_rows.append(row)
        group_rows.sort(key=lambda x: x["group_id"])
        metrics["group_breakdown"] = group_rows
    else:
        metrics["group_breakdown"] = []

    metrics["columns"] = list(df.columns)
    return metrics

@app.route("/api/results/detail", methods=["POST"])
@login_required
def results_detail():
    """Fetch a specific parsed_data.csv from S3 and return summary metrics."""
    global s3_region, s3_bucket
    body       = request.get_json(force=True) or {}
    object_key = str(body.get("object_key", "")).strip()
    if not object_key:
        return jsonify({"error": "object_key is required."}), 400
    if not object_key.endswith("parsed_data.csv"):
        return jsonify({"error": "Only parsed_data.csv files are supported."}), 400

    try:
        s3  = _s3_client()
        obj = s3.get_object(Bucket=s3_bucket, Key=object_key)
        csv_text = obj["Body"].read().decode("utf-8")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return jsonify({"error": "Parse Data File not found in S3."}), 404
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    try:
        df = pd.read_csv(StringIO(csv_text), on_bad_lines="warn")
    except Exception as exc:
        return jsonify({"error": f"Could not parse CSV: {exc}"}), 400

    # Get data config file to know the column alignment
    s3_path = object_key.rsplit("/", 1)[0]
    # print(s3_path)
    #set key for config file
    cfg_key = f"{s3_path}/source_data_config.json"
    try:
        s3  = _s3_client()
        obj = s3.get_object(Bucket=s3_bucket, Key=cfg_key)
        file_content = obj['Body'].read().decode('utf-8')
        cfg_data = json.loads(file_content)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return jsonify({"error": "Config File not found in S3."}), 404
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    try:
        metrics = _compute_detail_metrics(df, cfg_data)
    except _BadData:
        print("bad data flag")
        return jsonify({"error": "Bad Data."}), 401
    return jsonify({"ok": True, "object_key": object_key, "metrics": metrics})


# ══════════════════════════════════════════════════════════
#  Use-case (searchable-text) filter API
# ══════════════════════════════════════════════════════════
# Search the parsed dataset's "searchable" columns for text that indicates a
# use case / application (e.g. "sql"/"db" -> Database), then either KEEP the
# matching rows (include) or DROP them (exclude), and optionally save the
# filtered inventory as a new parsed_data.csv alongside the original.

def _resolve_searchable_cols(df, source_data_config):
    """Map the config's searchable_columns (original CSV indices) to actual
    column names in the parsed dataframe (which is index-aligned with config)."""
    cols = []
    idxs = (source_data_config or {}).get("searchable_columns", []) or []
    n = len(df.columns)
    for i in idxs:
        try:
            i = int(i)
        except (TypeError, ValueError):
            continue
        if 0 <= i < n:
            cols.append(df.columns[i])
    # de-dup, preserve order
    seen, out = set(), []
    for c in cols:
        if c not in seen:
            seen.add(c); out.append(c)
    return out

def _usecase_match_mask(df, cols, terms):
    """Boolean Series: True where ANY searchable column contains ANY term
    (case-insensitive substring). Also returns per-term match counts."""
    terms = [str(t).strip() for t in (terms or []) if str(t).strip()]
    mask = pd.Series(False, index=df.index)
    per_term = {}
    if not cols or not terms:
        return mask, per_term
    lowered = {c: df[c].astype(str).str.lower() for c in cols}
    for t in terms:
        tl = t.lower()
        tmask = pd.Series(False, index=df.index)
        for c in cols:
            tmask = tmask | lowered[c].str.contains(tl, regex=False, na=False)
        per_term[t] = int(tmask.sum())
        mask = mask | tmask
    return mask, per_term

def _load_filter_json(s3, key):
    """Read a filter.json sidecar if present (returns {} otherwise)."""
    try:
        obj = s3.get_object(Bucket=s3_bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8")) or {}
    except Exception:
        return {}

@app.route("/api/results/filter-preview", methods=["POST"])
@login_required
def results_filter_preview():
    """Preview how a use-case text filter partitions the selected parsed dataset."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    parsed_key = str(body.get("parsed_data_key", "")).strip()
    terms = body.get("terms", []) or []
    mode  = str(body.get("mode", "include")).strip().lower()
    if mode not in ("include", "exclude"):
        mode = "include"
    if not parsed_key or not parsed_key.endswith("parsed_data.csv"):
        return jsonify({"error": "A valid parsed_data.csv key is required."}), 400
    s3_path = parsed_key.rsplit("/", 1)[0]
    s3 = _s3_client()
    try:
        df = pd.read_csv(StringIO(s3.get_object(Bucket=s3_bucket, Key=parsed_key)["Body"].read().decode("utf-8")), on_bad_lines="warn")
    except ClientError as exc:
        return jsonify({"error": "That parsed dataset was not found in the current storage location. "
                                 "Reload the Results list and re-select the file — the selection may be from a "
                                 "different storage location (e.g. MikeS3 vs Local) than the one you're signed in to."}), 404
    except Exception as exc:
        return jsonify({"error": f"Could not read parsed data: {exc}"}), 400
    cfg = _load_filter_json(s3, f"{s3_path}/source_data_config.json")
    cols = _resolve_searchable_cols(df, cfg)
    if not cols:
        return jsonify({"error": "No searchable columns are configured for this dataset. Mark columns as 'searchable' when mapping the upload."}), 400

    mask, per_term = _usecase_match_mask(df, cols, terms)
    matched = int(mask.sum())
    kept_mask = mask if mode == "include" else ~mask
    kept_df = df[kept_mask]

    # Preview: show up to 50 KEPT rows, limited to the searchable columns plus a
    # few identifying ones, so the user can eyeball what the filter aligns to.
    preview_cols = list(cols)
    for extra in ("group_id", "region", "disk_type"):
        if extra in df.columns and extra not in preview_cols:
            preview_cols.append(extra)
    preview_cols = preview_cols[:8]
    sample = kept_df[preview_cols].head(50)
    preview_rows = [[("" if pd.isna(v) else str(v)) for v in row] for row in sample.itertuples(index=False, name=None)]

    return jsonify({
        "ok": True,
        "searchable_columns": cols,
        "total": int(len(df)),
        "matched": matched,
        "kept": int(len(kept_df)),
        "dropped": int(len(df) - len(kept_df)),
        "mode": mode,
        "per_term": per_term,
        "preview_columns": preview_cols,
        "preview_rows": preview_rows,
    })

@app.route("/api/results/filter-detail", methods=["POST"])
@login_required
def results_filter_detail():
    """Apply a use-case filter in place and return the Results summary metrics
    computed over just the filtered rows (does not save anything)."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    parsed_key = str(body.get("parsed_data_key", "")).strip()
    terms = body.get("terms", []) or []
    mode  = str(body.get("mode", "include")).strip().lower()
    if mode not in ("include", "exclude"):
        mode = "include"
    if not parsed_key or not parsed_key.endswith("parsed_data.csv"):
        return jsonify({"error": "A valid parsed_data.csv key is required."}), 400
    s3_path = parsed_key.rsplit("/", 1)[0]
    s3 = _s3_client()
    try:
        df = pd.read_csv(StringIO(s3.get_object(Bucket=s3_bucket, Key=parsed_key)["Body"].read().decode("utf-8")), on_bad_lines="warn")
    except ClientError:
        return jsonify({"error": "That parsed dataset was not found in the current storage location. "
                                 "Reload the Results list and re-select the file — the selection may be from a "
                                 "different storage location (e.g. MikeS3 vs Local) than the one you're signed in to."}), 404
    except Exception as exc:
        return jsonify({"error": f"Could not read parsed data: {exc}"}), 400
    cfg = _load_filter_json(s3, f"{s3_path}/source_data_config.json")
    cols = _resolve_searchable_cols(df, cfg)
    if not cols:
        return jsonify({"error": "No searchable columns are configured for this dataset."}), 400
    mask, _ = _usecase_match_mask(df, cols, terms)
    kept = df[mask] if mode == "include" else df[~mask]
    if len(kept) == 0:
        return jsonify({"error": "The filter leaves 0 rows — nothing to summarize."}), 400
    try:
        metrics = _compute_detail_metrics(kept, cfg)
    except _BadData:
        return jsonify({"error": "Bad Data."}), 401
    return jsonify({"ok": True, "metrics": metrics, "mode": mode, "terms": terms,
                    "kept": int(len(kept)), "total": int(len(df)),
                    "searchable_columns": cols})

@app.route("/api/results/save-filter", methods=["POST"])
@login_required
def results_save_filter():
    """Apply a use-case filter and save the result as a new parsed_data.csv in a
    self-contained folder under .../results/filters/<slug>_<stamp>/ (with copies
    of the sibling config + cost CSVs so it runs like any other dataset)."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    parsed_key = str(body.get("parsed_data_key", "")).strip()
    terms = [str(t).strip() for t in (body.get("terms", []) or []) if str(t).strip()]
    mode  = str(body.get("mode", "include")).strip().lower()
    label = str(body.get("label", "")).strip()
    if mode not in ("include", "exclude"):
        mode = "include"
    if not parsed_key or not parsed_key.endswith("parsed_data.csv"):
        return jsonify({"error": "A valid parsed_data.csv key is required."}), 400
    if not terms:
        return jsonify({"error": "At least one search term is required."}), 400
    if not label:
        label = ("keep " if mode == "include" else "exclude ") + ", ".join(terms)

    src_dir = parsed_key.rsplit("/", 1)[0]
    s3 = _s3_client()
    try:
        df = pd.read_csv(StringIO(s3.get_object(Bucket=s3_bucket, Key=parsed_key)["Body"].read().decode("utf-8")), on_bad_lines="warn")
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return jsonify({"error": "That parsed dataset was not found in the current storage location. "
                                     "Reload the Results list and re-select the file — the selection may be from a "
                                     "different storage location (e.g. MikeS3 vs Local) than the one you're signed in to."}), 404
        return jsonify({"error": f"Could not read parsed data: {exc}"}), 400
    except Exception as exc:
        return jsonify({"error": f"Could not read parsed data: {exc}"}), 400
    cfg = _load_filter_json(s3, f"{src_dir}/source_data_config.json")
    cols = _resolve_searchable_cols(df, cfg)
    if not cols:
        return jsonify({"error": "No searchable columns configured for this dataset."}), 400

    mask, _ = _usecase_match_mask(df, cols, terms)
    kept_mask = mask if mode == "include" else ~mask
    kept_df = df[kept_mask]
    if len(kept_df) == 0:
        return jsonify({"error": "The filter would leave 0 rows — nothing to save."}), 400

    # Keep all filters flat under the ORIGINAL run's results/filters/ folder.
    base_results = src_dir.split("/filters/")[0]
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    slug  = re.sub(r"[^\w.\-]+", "_", label)[:60].strip("_") or "filter"
    out_dir = f"{base_results}/filters/{slug}_{stamp}"
    new_parsed_key = f"{out_dir}/parsed_data.csv"

    try:
        # 1) filtered parsed data
        buf = StringIO(); kept_df.to_csv(buf, index=False)
        s3.put_object(Bucket=s3_bucket, Key=new_parsed_key,
                      Body=buf.getvalue().encode("utf-8"), ContentType="text/csv")
        # 2) copy the sibling config + cost files so the run engine works unchanged
        for sib in ("source_data_config.json", "azure_managed_disk_costs.csv", "ec_infra_resource_costs.csv"):
            try:
                s3.copy_object(Bucket=s3_bucket,
                               CopySource={"Bucket": s3_bucket, "Key": f"{src_dir}/{sib}"},
                               Key=f"{out_dir}/{sib}")
            except ClientError as exc:
                if exc.response["Error"]["Code"] not in ("404", "NoSuchKey"):
                    raise
        # 3) filter metadata sidecar
        meta = {
            "label": label, "mode": mode, "terms": terms,
            "searchable_columns": cols,
            "source_parsed_key": parsed_key,
            "total_rows": int(len(df)), "kept_rows": int(len(kept_df)),
            "created": stamp,
        }
        s3.put_object(Bucket=s3_bucket, Key=f"{out_dir}/filter.json",
                      Body=json.dumps(meta).encode("utf-8"), ContentType="application/json")
        # print(f"Saved filtered dataset ({len(kept_df)}/{len(df)} rows, {mode}: {terms}) to {new_parsed_key}")
    except ClientError as exc:
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": f"Could not save filtered dataset: {exc}"}), 500

    return jsonify({"ok": True, "parsed_data_key": new_parsed_key, "label": label,
                    "kept": int(len(kept_df)), "total": int(len(df))})


# ══════════════════════════════════════════════════════════
#  Run Analysis API
# ══════════════════════════════════════════════════════════

@app.route("/api/results/run-analysis", methods=["POST"])
@login_required
def results_run_analysis():
    """
    Accepts from the frontend:
        csv_key    – S3 key of the selected parsed_data.csv
        config_key – S3 key of the associated source_data_config.json
        params     – GUI parameter overrides (growth, drr, etc.)

    Fetches both files from S3, merges the GUI params over the stored config,
    re-runs the analysis, collects ALL results into a dict, prints it, and
    returns it as JSON.
    """
    global s3_region, s3_bucket

    body       = request.get_json(force=True) or {}
    # print("body",body)
    parsed_key = str(body.get("parsed_data_key", "")).strip()
    # The source data config and cost files live alongside the parsed data file,
    # so derive their keys from its location.
    s3_path = parsed_key.rsplit("/", 1)[0]
    # print(s3_path)
    cfg_key = f"{s3_path}/source_data_config.json"
    azure_disk_key = f"{s3_path}/azure_managed_disk_costs.csv"
    ec_infra_key = f"{s3_path}/ec_infra_resource_costs.csv"
    params     = body.get("params", {})

    # ── Validate inputs ────────────────────────────────────────────────────
    if not parsed_key:
        return jsonify({"error": "parsed data object is required."}), 400
    if not parsed_key.endswith("parsed_data.csv"):
        return jsonify({"error": "parsed data key must point to a parsed_data.csv file."}), 400
    if not cfg_key:
        return jsonify({"error": "data config object is required."}), 400
    if not cfg_key.endswith("source_data_config.json"):
        return jsonify({"error": "data config object  must point to a source_data_config.json file."}), 400

    # print("=" * 60)
    # print("RUN-ANALYSIS REQUEST 1")
    # print(f"  parsed data key    : {parsed_key}")
    # print(f"  data config key    : {cfg_key}")
    # print(f"  params             : {params}")
    # print("=" * 60)

    s3 = _s3_client()

    # If this dataset was produced by a use-case filter, record the filter on the
    # run (scalars flow into meta.json params -> shown on the TCO page).
    fj = _load_filter_json(s3, f"{s3_path}/filter.json")
    if fj:
        params["filter_label"] = str(fj.get("label", ""))
        params["filter_mode"]  = str(fj.get("mode", ""))
        params["filter_terms"] = ", ".join(fj.get("terms", []) or [])

    # ── 1. Fetch parsed_data.csv ───────────────────────────────────────────
    try:
        obj      = s3.get_object(Bucket=s3_bucket, Key=parsed_key)
        parsed_text = obj["Body"].read().decode("utf-8")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return jsonify({"error": f"parsed_data.csv not found in S3: {parsed_key}"}), 404
        return jsonify({"error": f"AWS error fetching CSV: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": f"Error fetching CSV: {exc}"}), 500

    try:
        df_parsed = pd.read_csv(StringIO(parsed_text), on_bad_lines="warn")
        # Backward-compat: datasets parsed before the PSCD/CBS->EC rename carry old
        # column names (e.g. min_pscd_model); normalize so historical runs still analyze.
        df_parsed.columns = [c.replace("pscd", "ec") if isinstance(c, str) else c
                             for c in df_parsed.columns]
        # print(f"Loaded parsed data from S3: {parsed_key} ({df_parsed.shape[0]} rows)")
    except Exception as exc:
        return jsonify({"error": f"Could not parse CSV: {exc}"}), 400
    #print("big hut 5")

    # ── 2. Fetch source_data_config.json ───────────────────────────────────
    try:
        obj          = s3.get_object(Bucket=s3_bucket, Key=cfg_key)
        source_data_config = json.loads(obj["Body"].read().decode("utf-8"))
        # Backward-compat: pre-rename configs carry old 'pscd' keys (e.g. pscd_sku_bias).
        source_data_config = {(k.replace("pscd", "ec") if isinstance(k, str) else k): v
                              for k, v in source_data_config.items()}
        # print(source_data_config, type(source_data_config))
        params["source_data_config"] = source_data_config
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return jsonify({"error": f"source_data_config.json not found in S3: {cfg_key}"}), 404
        return jsonify({"error": f"AWS error fetching config: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": f"Error fetching config: {exc}"}), 500

    # ── 2.a. Snapshot-rate override: adjust the Azure baseline for the analysis ──
    # The per-disk Azure snapshot cost was baked in at parse time using the dataset's
    # parse-time rate (r0). If the analysis specifies a different monthly_snapshot_rate,
    # rescale snap_cost and re-sum total_cost so the Azure baseline reflects the new rate.
    # snap_cost is linear in the factor ((rate/2)+1) (and 0 at rate 0), and
    # total_cost == cap_cost + iops_cost + mbps_cost + snap_cost holds exactly, so the
    # rescale is exact. Affects both engines, which read df_parsed["total_cost"].
    try:
        r0 = float(source_data_config.get("monthly_snapshot_rate", 0.1))
    except (TypeError, ValueError):
        r0 = 0.1
    try:
        new_rate = float(params.get("monthly_snapshot_rate", r0))
    except (TypeError, ValueError):
        new_rate = r0
    _cost_cols = {"cap_cost", "iops_cost", "mbps_cost", "snap_cost", "total_cost"}
    if new_rate != r0 and _cost_cols.issubset(df_parsed.columns):
        for _c in _cost_cols:
            df_parsed[_c] = pd.to_numeric(df_parsed[_c], errors="coerce").fillna(0)
        old_factor = (r0 / 2) + 1                       # >= 1 for r0 >= 0, safe divisor
        new_factor = 0.0 if new_rate == 0 else (new_rate / 2) + 1
        snap_unit = df_parsed["snap_cost"] / old_factor  # snap_cost at factor 1
        df_parsed["snap_cost"] = snap_unit * new_factor
        df_parsed["total_cost"] = (df_parsed["cap_cost"] + df_parsed["iops_cost"]
                                   + df_parsed["mbps_cost"] + df_parsed["snap_cost"])

    # ── 2.b. Fetch azure_manage_disk_cost.csv ───────────────────────────────────
    try:
        obj = s3.get_object(Bucket=s3_bucket, Key=azure_disk_key)
        azure_disk_text = obj["Body"].read().decode("utf-8")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return jsonify({"error": f"azure_managed_disk_cost.csv not found in S3: {azure_disk_key}"}), 404
        return jsonify({"error": f"AWS error fetching CSV: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": f"Error fetching CSV: {exc}"}), 500

    try:
        df_azure_disk = pd.read_csv(StringIO(azure_disk_text), on_bad_lines="warn")
        # print(f"Loaded azure data cost from S3: {azure_disk_key} ({df_azure_disk.shape[0]} rows)")
    except Exception as exc:
        return jsonify({"error": f"Could not parse CSV: {exc}"}), 400

    # ── 2.c. Fetch ec_infra_resource_cost.csv ───────────────────────────────────
    try:
        obj = s3.get_object(Bucket=s3_bucket, Key=ec_infra_key)
        ec_infra_text = obj["Body"].read().decode("utf-8")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return jsonify({"error": f"ec_infra_resource_cost.csv not found in S3: {ec_infra_key}"}), 404
        return jsonify({"error": f"AWS error fetching CSV: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": f"Error fetching CSV: {exc}"}), 500

    try:
        df_ec_infra = pd.read_csv(StringIO(ec_infra_text), on_bad_lines="warn")
        # print(f"Loaded azure data cost from S3: {ec_infra_key} ({df_ec_infra.shape[0]} rows)")
    except Exception as exc:
            return jsonify({"error": f"Could not parse CSV: {exc}"}), 400
    # print("big hut 5")



    # ── 3. Apply GUI parameter overrides ──────────────────────────────────
    # Map frontend param names to the keys main2() reads from config_event.
    param_map = {
        "growth":                  "growth",
        "drr":                     "drr",
        "monthly_snapshot_rate":   "monthly_snapshot_rate",
        "initial_cap_rate":        "initial_cap_rate",
        "ignore_iops_provisioned": "ignore_iops_provisioned",
        "efficiency":              "efficiency",
        "default_sku_model":       "ec_sku_bias",
    }
    method = str(params.get("method", "dedicated")).strip().lower()
    # print(f"analysis method: {method}")

    # Self-heal: a non-MikeS3 backend that was selected before config seeding (or
    # whose seeding failed) may lack the global engine config files. Seed any
    # missing ones from MikeS3 now so the analysis can proceed.
    _ensure_backend_configs(_session_storage())

    try:
        if method == "azure_native":
            # ── Azure Native path ──────────────────────────────────────────────
            # Read the ECAN config from S3 (same _config prefix as ec_config.json)
            # and run the simplified capacity + throughput cost model. No EC SKU
            # sizing or infrastructure pricing is used.
            ecan_config = _load_ecan_config()
            params["ecan_config"] = ecan_config
            if not ecan_config:
                return jsonify({"error": f"ecan_config.json was not found in this storage location "
                                         f"({ECAN_CONFIG_KEY}). The Azure Native pricing config is missing "
                                         f"for this backend."}), 400
            rst = tco_by_group_azure_native(params, df_parsed, ecan_config, s3_path)
        else:
            param_models = params.get("skus", ["V10MP2R2", "V20MP2R2"])
            # print(param_models)
            # 4. Load EC Config Data
            ec_data = _load_ec_list(param_models)
            params["ec_data"] = ec_data
            # print("ec_data", ec_data, type(ec_data))
            if not isinstance(ec_data, dict) or not ec_data.get("models"):
                return jsonify({"error": f"ec_config.json was not found (or has no models) in this storage "
                                         f"location ({EC_CONFIG_KEY}). The EC sizing config is missing for "
                                         f"this backend."}), 400
            rst = tco_by_group_y1(params, df_parsed, df_azure_disk, df_ec_infra, s3_path)
    except Exception as exc:
        import traceback as _tb
        print("Analysis failed:\n" + _tb.format_exc())
        return jsonify({"error": f"Analysis failed: {exc}"}), 500

    # ── Growth projection at generation time ────────────────────────────────
    # Using the growth rate + frequency + years set on the Results window, project
    # cost over time and save it alongside the run so the TCO Review tab can show
    # it without recomputing.
    try:
        run_prefix = rst.get("prefix") if isinstance(rst, dict) else None
        if run_prefix:
            frequency = str(params.get("frequency", "yearly")).strip().lower()
            if frequency not in ("monthly", "quarterly", "yearly"):
                frequency = "yearly"
            yearly_growth = float(params.get("growth", 0) or 0)
            proj = project_growth_over_time(params, df_parsed, df_azure_disk, df_ec_infra,
                                            s3_path, yearly_growth, frequency)
            _s3_client().put_object(
                Bucket=s3_bucket,
                Key=f"{run_prefix}projection.json",
                Body=json.dumps(proj).encode("utf-8"),
                ContentType="application/json",
            )
            # print(f"Saved growth projection ({frequency}, {len(proj.get('series', []))} periods) to {run_prefix}projection.json")
    except Exception as exc:
        print(f"Warning: could not compute/save growth projection: {exc}")

    applied_overrides = {}
    for ui_key, cfg_key_name in param_map.items():
        if ui_key in params:
            source_data_config[cfg_key_name] = params[ui_key]
            applied_overrides[cfg_key_name] = params[ui_key]

    # print("Applied config overrides:", applied_overrides)

    # ── 4. Run the analysis ────────────────────────────────────────────────
    #try:
    #    raw_results = main2(config_event, df)
    #except Exception as exc:
    #    return jsonify({"error": f"Analysis failed: {exc}"}), 500

    # ── 5. Collect ALL results into a serialisable dict ────────────────────

    #results_dict = {
    #    "csv_key":         parsed_key,
    #    "config_key":      cfg_key,
    #    "params_received": params,
    #    "overrides_applied": applied_overrides,
    #    "status":          raw_results.get("status"),
    #    "num_groups":      raw_results.get("num_groups"),
    #    "tot_capacity_gib": raw_results.get("tot_capacity"),
    #    "tot_costs":       round(float(raw_results.get("tot_costs", 0) or 0), 2),
    #    "regions":         [],
    #}

    # regions is returned as a single-column DataFrame; flatten to list
    #regions_val = raw_results.get("regions")
    #if regions_val is not None:
    #    try:
    #        if hasattr(regions_val, "iloc"):
    #            results_dict["regions"] = regions_val.iloc[:, 0].tolist()
    #        else:
    #            results_dict["regions"] = list(regions_val)
    #    except Exception:
    #        results_dict["regions"] = []

    # ── 6. Print the complete results dict ────────────────────────────────
    #print("=" * 60)
    #print("RUN-ANALYSIS RESULTS")
    #print("=" * 60)
    #for key, value in results_dict.items():
    #    print(f"  {key}: {value}")
    #print("=" * 60)

    return jsonify({"ok": True, "results": "happy"})


# ══════════════════════════════════════════════════════════
#  Auth API
# ══════════════════════════════════════════════════════════

@app.route("/api/auth/login", methods=["POST"])
def api_login():
    # The storage location is a deployment setting (see /api/storage/config) and
    # must be configured first — the before_request gate blocks this route until
    # it is. Login now only checks the user credentials.
    body     = request.get_json(force=True) or {}
    username = str(body.get("username", "")).strip()
    password = str(body.get("password", ""))
    if not username or not password:
        return jsonify({"error": "Username and password are required."}), 400
    if VALID_USERS.get(username) != password:
        return jsonify({"error": "Invalid username or password."}), 401
    session["logged_in"] = True
    session["username"]  = username
    session["date_time_str"] = datetime.now().strftime("%Y%m%d%H%M%S")
    return jsonify({"ok": True, "username": username})

@app.route("/api/auth/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({"ok": True})

@app.route("/api/auth/status", methods=["GET"])
def api_auth_status():
    return jsonify({
        "logged_in":    bool(session.get("logged_in")),
        "username":     session.get("username", ""),
        "storage_kind": (_load_storage_config() or {}).get("kind", ""),
    })

# ══════════════════════════════════════════════════════════
#  Storage location — a deployment-wide setting saved in a local file
# ══════════════════════════════════════════════════════════

@app.route("/api/storage/config", methods=["GET"])
def storage_config_get():
    """Report whether a storage location is configured, and (safely) what it is —
    the offering and its location. Never returns AWS secrets."""
    cfg = _load_storage_config()
    if not cfg:
        return jsonify({"configured": False})
    return jsonify({"configured": True, **_storage_offering_location(cfg)})

@app.route("/api/storage/config", methods=["POST"])
def storage_config_set():
    """Choose the deployment's storage location, validate it with a write test,
    and persist it to a local file so it isn't asked again."""
    body = request.get_json(force=True) or {}
    kind = str(body.get("kind", "")).strip().lower()
    if kind not in ("mikes3", "others3", "local"):
        return jsonify({"error": "Choose a storage location: MikeS3, Other S3, or Local Storage."}), 400
    if kind == "mikes3":
        storage = {"kind": "mikes3"}
    elif kind == "others3":
        bucket     = str(body.get("bucket", "")).strip()
        access_key = str(body.get("access_key", "")).strip()
        secret_key = str(body.get("secret_key", "")).strip()
        if not bucket or not access_key or not secret_key:
            return jsonify({"error": "Other S3 requires an S3 bucket name, an AWS access key, and an AWS secret key."}), 400
        storage = {"kind": "others3", "bucket": bucket, "access_key": access_key,
                   "secret_key": secret_key, "region": str(body.get("region", "")).strip() or "us-east-1"}
    else:  # local
        drive = str(body.get("drive", "")).strip()
        try:
            root = _local_root_for_drive(drive)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        storage = {"kind": "local", "drive": drive.rstrip(":\\/").upper(), "root": root}

    ok, msg = _storage_write_test(storage)
    if not ok:
        return jsonify({"error": f"Could not write to the selected storage location. {msg}"}), 400
    _ensure_backend_configs(storage)     # seed engine configs for a fresh backend
    _save_storage_config(storage)
    return jsonify({"ok": True, "configured": True, **_storage_offering_location(storage)})

@app.route("/api/storage/config", methods=["DELETE"])
def storage_config_clear():
    """Clear the deployment storage setting; sign out and force re-selection."""
    _clear_storage_config()
    session.clear()
    return jsonify({"ok": True, "configured": False})

# ══════════════════════════════════════════════════════════
#  JSON Data tab API
# ══════════════════════════════════════════════════════════

@app.route("/api/jsondata/get", methods=["GET"])
@login_required
def jsondata_get():
    user   = session["username"]
    stored = _json_data_store.get(user)
    if stored:
        return jsonify({"ok": True, "data": stored, "source": "saved"})
    return jsonify({"ok": True, "data": None, "source": "none"})

@app.route("/api/jsondata/set", methods=["POST"])
@login_required
def jsondata_set():
    global config_data
    global config_data_flag
    if not session.get("active_customer"):
        return jsonify({"error": "A customer must be selected before saving JSON data. Please go to the Customers tab and select or create a customer."}), 400
    body = request.get_json(force=True) or {}
    raw  = body.get("raw", "")
    try:
        parsed = json.loads(raw)
        session["json_config"] = parsed
        # print(parsed)
        #config_data = parsed
        #config_data_flag = True

    except json.JSONDecodeError as exc:
        return jsonify({"error": f"Invalid JSON: {exc}"}), 400
    user = session["username"]
    _json_data_store[user] = parsed
    return jsonify({"ok": True, "message": "JSON data saved successfully."})

@app.route("/api/jsondata/upload", methods=["POST"])
@login_required
def jsondata_upload():
    if not session.get("active_customer"):
        return jsonify({"error": "A customer must be selected before uploading JSON data. Please go to the Customers tab and select or create a customer."}), 400
    if "file" not in request.files:
        return jsonify({"error": "No file provided."}), 400
    f = request.files["file"]
    if not f.filename.lower().endswith(".json"):
        return jsonify({"error": "Only .json files are accepted here."}), 400
    try:
        parsed = json.loads(f.read().decode("utf-8"))
    except Exception as exc:
        return jsonify({"error": f"Could not parse file: {exc}"}), 400
    user = session["username"]
    _json_data_store[user] = parsed
    return jsonify({"ok": True, "message": f'File "{f.filename}" loaded successfully.'})

# ══════════════════════════════════════════════════════════
#  Customer Management API
# ══════════════════════════════════════════════════════════

def _load_df_from_s3(object_key):
    """Load a CSV from S3 and return a DataFrame, or None on any error."""
    try:
        s3_client = _s3_client()
        obj = s3_client.get_object(Bucket=s3_bucket, Key=object_key)
        csv_bytes = obj["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(csv_bytes))
        # print(f"Loaded cached CSV from S3: {object_key} ({df.shape[0]} rows)")
        return df
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            return None
        print(f"S3 ClientError loading {object_key}: {exc}")
        return None
    except Exception as exc:
        print(f"Error loading {object_key} from S3: {exc}")
        return None

def _load_customer_list():
    global s3_region
    global s3_bucket
    """Read customer list JSON from S3. Returns list of customer name strings."""
    try:
        s3   = _s3_client()
        obj  = s3.get_object(Bucket=s3_bucket, Key=CUSTOMER_LIST_S3_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        if isinstance(data, list):
            return data
        return data.get("customers", [])
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            return []          # file doesn't exist yet — treat as empty
        raise
    except Exception:
        return []

def _load_ec_list(models):
    global s3_region
    global s3_bucket
    included_models = {"models": {}}
    # print("models",models)
    """Read EC Config JSON from S3. Returns list of customer name strings."""
    try:
        s3   = _s3_client()
        obj  = s3.get_object(Bucket=s3_bucket, Key=EC_CONFIG_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        # print("ec_config",data,type(data))
        for mod in models:
            # print("mod", mod)
            if mod in data.get("models", {}).keys():
                included_models["models"][mod] = data.get("models", []).get(mod,{})
        # print(included_models)
        return included_models
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            return []          # file doesn't exist yet — treat as empty
        raise
    except Exception:
        return []

def _load_ecan_config():
    """Read the Azure Native (ECAN) config JSON from S3, stored alongside the
    EC config under the _config prefix. Returns the dict keyed by SKU
    (e.g. {"V20AZN": {...}}), or {} on any error."""
    global s3_region
    global s3_bucket
    try:
        s3   = _s3_client()
        obj  = s3.get_object(Bucket=s3_bucket, Key=ECAN_CONFIG_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        # print("ecan_config", data, type(data))
        return data if isinstance(data, dict) else {}
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            print(f"ecan_config.json not found at {ECAN_CONFIG_KEY}")
            return {}
        raise
    except Exception as exc:
        print(f"Error loading ecan_config.json: {exc}")
        return {}

def _save_customer_list(customers):
    """Write the customer list back to S3 as JSON."""
    global s3_region
    global s3_bucket
    s3      = _s3_client()
    payload = json.dumps({"customers": sorted(set(customers))}, indent=2).encode("utf-8")
    s3.put_object(
        Bucket=s3_bucket,
        Key=CUSTOMER_LIST_S3_KEY,
        Body=payload,
        ContentType="application/json",
    )

@app.route("/api/customers", methods=["GET"])
@login_required
def customers_list():
    try:
        customers = _load_customer_list()
        return jsonify({"ok": True, "customers": customers})
    except NoCredentialsError:
        return jsonify({"error": "AWS credentials are not configured on the server."}), 500
    except ClientError as exc:
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

@app.route("/api/customers/add", methods=["POST"])
@login_required
def customers_add():
    body = request.get_json(force=True) or {}
    name = str(body.get("name", "")).strip()
    if not name:
        return jsonify({"error": "Customer name is required."}), 400
    if len(name) > 80:
        return jsonify({"error": "Customer name must be 80 characters or fewer."}), 400
    # Basic safe-character check so the name can be used in S3 key paths
    import re
    if not re.match(r'^[\w\-. ]+$', name):
        return jsonify({"error": "Customer name may only contain letters, numbers, spaces, hyphens, underscores, and periods."}), 400
    try:
        customers = _load_customer_list()
        if name in customers:
            #session["customers"] = name
            return jsonify({"ok": True, "customers": sorted(customers), "message": f'"{name}" already exists.'}), 200
        customers.append(name)
        _save_customer_list(customers)
        #session["customers"] = name
        return jsonify({"ok": True, "customers": sorted(set(customers)), "message": f'Customer "{name}" added.'})
    except NoCredentialsError:
        return jsonify({"error": "AWS credentials are not configured on the server."}), 500
    except ClientError as exc:
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

@app.route("/api/customers/delete", methods=["POST"])
@login_required
def customers_delete():
    """Remove a customer from the list and delete ALL of its data in S3
    (every object under TCO-GUI/<username>/<customer>/)."""
    global s3_region, s3_bucket
    body = request.get_json(force=True) or {}
    name = str(body.get("name", "")).strip()
    if not name:
        return jsonify({"error": "Customer name is required."}), 400
    active_username = session.get("username", "unknown")
    prefix = f"TCO-GUI/{active_username}/{name}/"
    deleted = 0
    try:
        s3 = _s3_client()
        # Delete every object under the customer's prefix (batched; 1000 max per call)
        paginator = s3.get_paginator("list_objects_v2")
        batch = []
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                batch.append({"Key": obj["Key"]})
                if len(batch) == 1000:
                    s3.delete_objects(Bucket=s3_bucket, Delete={"Objects": batch})
                    deleted += len(batch)
                    batch = []
        if batch:
            s3.delete_objects(Bucket=s3_bucket, Delete={"Objects": batch})
            deleted += len(batch)

        # Remove the customer from the global list
        customers = [c for c in _load_customer_list() if c != name]
        _save_customer_list(customers)

        # Clear the active selection if it pointed at this customer
        if session.get("active_customer") == name:
            session["active_customer"] = ""

        return jsonify({
            "ok": True,
            "customers": sorted(set(customers)),
            "deleted_objects": deleted,
            "message": f'Customer "{name}" and {deleted} data object(s) removed.',
        })
    except NoCredentialsError:
        return jsonify({"error": "AWS credentials are not configured on the server."}), 500
    except ClientError as exc:
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

@app.route("/api/customers/select", methods=["POST"])
@login_required
def customers_select():
    """Persist the currently active customer in the session."""
    body = request.get_json(force=True) or {}
    name = str(body.get("name", "")).strip()
    if not name:
        return jsonify({"error": "Customer name is required."}), 400
    session["active_customer"] = name
    return jsonify({"ok": True, "active_customer": name})

@app.route("/api/customers/active", methods=["GET"])
@login_required
def customers_active():
    return jsonify({
        "ok": True,
        "active_customer": session.get("active_customer", ""),
        "active_scenario": session.get("active_scenario", "default"),
    })

@app.route("/api/session/clear", methods=["POST"])
@login_required
def session_clear():
    """Clear all session data except login credentials."""
    username = session.get("username")
    logged_in = session.get("logged_in")
    session.clear()
    session["username"]  = username
    session["logged_in"] = logged_in
    return jsonify({"ok": True, "message": "Session data cleared (login preserved)."})

@app.route("/api/customers/scenario", methods=["GET"])
@login_required
def customers_scenario_get():
    return jsonify({
        "ok": True,
        "active_scenario": session.get("active_scenario", "default"),
    })

@app.route("/api/customers/scenario", methods=["POST"])
@login_required
def customers_scenario_set():
    body     = request.get_json(force=True) or {}
    scenario = str(body.get("scenario", "")).strip() or "default"
    if len(scenario) > 80:
        return jsonify({"error": "Scenario name must be 80 characters or fewer."}), 400
    import re
    if not re.match(r'^[\w\-. ]+$', scenario):
        return jsonify({"error": "Scenario name may only contain letters, numbers, spaces, hyphens, underscores, and periods."}), 400
    session["active_scenario"] = scenario
    return jsonify({"ok": True, "active_scenario": scenario})

# ══════════════════════════════════════════════════════════
#  S3 Upload API
# ══════════════════════════════════════════════════════════

@app.route("/api/s3/presign", methods=["POST"])
@login_required
def s3_presign():
    global s3_region
    global s3_bucket
    # print("in s3_presign")
    # Require an active customer before allowing uploads
    active_customer = session.get("active_customer", "")
    if not active_customer:
        return jsonify({"error": "A customer must be selected before uploading files. Please go to the Customers tab and select or create a customer."}), 400
    # Column mapping is now chosen AFTER upload (see /api/mapping/*), so no
    # json_config is required here.

    body         = request.get_json(force=True) or {}
    filename     = str(body.get("filename", "")).strip()
    content_type = str(body.get("content_type", "application/octet-stream")).strip()
    file_size    = int(body.get("size", 0))
    if not filename:
        return jsonify({"error": "filename is required"}), 400
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext.lower() not in ALLOWED_EXTENSIONS:
        return jsonify({
            "error":   f"File type '.{ext}' is not allowed.",
            "allowed": sorted(ALLOWED_EXTENSIONS),
        }), 400
    if file_size > MAX_UPLOAD_BYTES:
        mb = MAX_UPLOAD_BYTES // (1024 * 1024)
        return jsonify({"error": f"File exceeds the {mb} MB limit."}), 400
    safe_name        = "".join(c for c in filename if c.isalnum() or c in "._- ")[:120]
    active_scenario  = session.get("active_scenario", "default") or "default"
    active_username  = session.get("username", "unknown")
    # Stamp each upload with the current date/time so the customer data lands in
    # its own dated folder (rather than reusing the login-time stamp).
    date_time_str    = datetime.now().strftime("%Y%m%d%H%M%S")
    session["date_time_str"] = date_time_str
    upload_prefix    = f"TCO-GUI/{active_username}/{active_customer}/{active_scenario}/{date_time_str}/data/"
    results_prefix = f"TCO-GUI/{active_username}/{active_customer}/{active_scenario}/{date_time_str}/results/"
    session["upload_prefix"] = upload_prefix
    session["results_prefix"] = results_prefix
    object_key       = f"{upload_prefix}{safe_name}"
    # Local storage has no presigned URL — the browser uploads through the server
    # (POST /api/s3/local-upload) instead of PUTting directly to S3.
    if _session_storage().get("kind") == "local":
        return jsonify({
            "mode": "local",
            "object_key": object_key,
            "upload_prefix": upload_prefix,
        })
    try:
        s3         = _s3_client()
        upload_url = s3.generate_presigned_url(
            "put_object",
            Params={
                "Bucket":      s3_bucket,
                "Key":         object_key,
                "ContentType": content_type,
            },
            ExpiresIn=PRESIGN_EXPIRY,
        )
    except NoCredentialsError:
        return jsonify({"error": "AWS credentials are not configured on the server."}), 500
    except ClientError as exc:
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": f"Could not generate upload URL: {str(exc)}"}), 500
    public_url = f"https://{s3_bucket}.s3.{s3_region}.amazonaws.com/{object_key}"
    return jsonify({
        "mode": "s3",
        "upload_url": upload_url,
        "object_key": object_key,
        "public_url": public_url,
        "expires_in": PRESIGN_EXPIRY,
        "upload_prefix": upload_prefix,
    })

@app.route("/api/s3/local-upload", methods=["POST"])
@login_required
def s3_local_upload():
    """Receive a browser upload and write it via the session storage backend.
    Used for Local Storage (and any backend without browser-direct uploads)."""
    global s3_bucket
    object_key = str(request.form.get("object_key", "")).strip()
    up = session.get("upload_prefix", "")
    if not object_key or not up or not object_key.startswith(up):
        return jsonify({"error": "Invalid object key for this upload session."}), 400
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "No file was provided."}), 400
    try:
        _s3_client().put_object(Bucket=s3_bucket, Key=object_key,
                                Body=f.read(),
                                ContentType=f.mimetype or "text/csv")
    except Exception as exc:
        return jsonify({"error": f"Could not write file to storage: {exc}"}), 500
    return jsonify({"ok": True, "object_key": object_key})

@app.route("/api/s3/confirm", methods=["POST"])
@login_required
def s3_confirm():
    global s3_region
    global s3_bucket
    # print("in s3_confirm")
    # Mapping is chosen after upload; no json_config gate here.
    body       = request.get_json(force=True) or {}
    object_key = str(body.get("object_key", "")).strip()
    if not object_key:
        return jsonify({"error": "object_key is required"}), 400
    try:
        s3   = _s3_client()
        head = s3.head_object(Bucket=s3_bucket, Key=object_key)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return jsonify({"error": "Object not found in S3 — upload may have failed."}), 404
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    return jsonify({
        "object_key":    object_key,
        "size":          head.get("ContentLength", 0),
        "content_type":  head.get("ContentType", ""),
        "last_modified": head.get("LastModified", "").isoformat() if head.get("LastModified") else "",
        "etag":          head.get("ETag", "").strip('"'),
    })

@app.route("/api/s3/list-uploads", methods=["GET"])
@login_required
def s3_list_uploads():
    global s3_region
    global s3_bucket
    """Return all files under the active customer's upload prefix."""
    active_customer  = session.get("active_customer", "none")
    active_scenario  = session.get("active_scenario", "default")
    active_username  = session.get("username", "unknown")
    prefix           = f"TCO-GUI/{active_username}/{active_customer}/{active_scenario}/"
    try:
        s3       = _s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        pages    = paginator.paginate(Bucket=s3_bucket, Prefix=prefix)
        files    = []
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key == prefix:          # skip the "folder" placeholder
                    continue
                files.append({
                    "key":           key,
                    "filename":      key.split("/")[-1],
                    "size":          obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                })
        # Parsing is now an explicit step (POST /api/mapping/parse) after the
        # column mapping is chosen — listing uploads no longer runs the engine.
        return jsonify({
            "ok":              True,
            "prefix":          prefix,
            "customer":        active_customer,
            "scenario":        active_scenario,
            "count":           len(files),
            "files":           files,
            "analysis":        None,
        })
    except NoCredentialsError:
        return jsonify({"error": "AWS credentials are not configured on the server."}), 501
    except ClientError as exc:
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 502
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503

@app.route("/api/s3/uploaded-files", methods=["GET"])
@login_required
def s3_uploaded_files():
    """List the raw uploaded data files (under .../<datetime>/data/) for the
    active customer/scenario, so the S3 Upload page can show and manage them."""
    global s3_bucket
    user = session.get("username", "unknown")
    cust = session.get("active_customer", "")
    scen = session.get("active_scenario", "default")
    if not cust:
        return jsonify({"ok": True, "customer": "", "scenario": scen, "files": []})
    prefix = f"TCO-GUI/{user}/{cust}/{scen}/"
    try:
        s3 = _s3_client()
        files = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if "/data/" not in key or key.endswith("/data/"):
                    continue
                parts = key.split("/")
                files.append({
                    "key": key,
                    "filename": key.split("/")[-1],
                    "run_datetime": parts[4] if len(parts) > 4 else "",
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                })
        files.sort(key=lambda f: f["last_modified"], reverse=True)
        return jsonify({"ok": True, "customer": cust, "scenario": scen, "files": files})
    except ClientError as exc:
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

def _valid_upload_key(key):
    user = session.get("username", "unknown")
    return bool(key and key.startswith(f"TCO-GUI/{user}/") and "/data/" in key and not key.endswith("/data/"))

@app.route("/api/s3/upload-download", methods=["GET"])
@login_required
def s3_upload_download():
    """Stream an uploaded data file back to the browser (works for S3 and local)."""
    global s3_bucket
    key = str(request.args.get("key", "")).strip()
    if not _valid_upload_key(key):
        return jsonify({"error": "Invalid key."}), 400
    try:
        obj = _s3_client().get_object(Bucket=s3_bucket, Key=key)
        body = obj["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return jsonify({"error": "File not found."}), 404
        return jsonify({"error": f"AWS error: {exc.response['Error']['Message']}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    fname = key.split("/")[-1] or "download"
    return Response(body, mimetype="application/octet-stream",
                    headers={"Content-Disposition": f'attachment; filename="{fname}"'})

@app.route("/api/s3/upload-delete", methods=["POST"])
@login_required
def s3_upload_delete():
    """Delete one uploaded data file for the active user."""
    global s3_bucket
    body = request.get_json(force=True) or {}
    key = str(body.get("key", "")).strip()
    if not _valid_upload_key(key):
        return jsonify({"error": "Invalid key."}), 400
    try:
        _s3_client().delete_object(Bucket=s3_bucket, Key=key)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({"ok": True, "key": key})

def read_file_s3(prefix):
    global s3_region
    global s3_bucket
    s3_client = _s3_client()
    response  = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
    if 'Contents' in response:
        filtered_objects = [obj for obj in response['Contents'] if obj['Key'].lower().endswith('.csv')]
        # print("len filtered objs", len(filtered_objects))
    else:
        return {"status": 207, "data": {}, "msg": "No objects found"}
    if len(filtered_objects) == 0:
        return {"status": 208, "data": {}, "msg": "No objects ending with .csv found"}
    df_all = pd.concat([
        pd.read_csv(
            StringIO(s3_client.get_object(Bucket=s3_bucket, Key=csv_obj.get('Key'))['Body'].read().decode('utf-8')),
            on_bad_lines='warn'
        ) for csv_obj in filtered_objects
    ])
    # print(df_all.shape)
    config_data = session["json_config"]
    results = main2(config_data,df_all)
    return results

# ══════════════════════════════════════════════════════════
#  Column mapping (upload -> auto-map -> editable -> parse)
# ══════════════════════════════════════════════════════════
# Canonical engine fields + the header aliases used to auto-map an uploaded
# file's columns. `key` is exactly what main2()/source_data_config.json expect;
# `aliases` are matched case-insensitively against the file's headers.
DATA_FIELDS_CATALOG = [
    {"key": "region", "label": "Region", "aliases": ["region", "location", "azureregion", "azure region", "datacenter", "az region"]},
    {"key": "zone", "label": "Availability Zone", "aliases": ["zone", "availability zone", "availabilityzone", "az", "azurezone", "azure zone"]},
    {"key": "subscription_or_account_id", "label": "Subscription / Account", "aliases": ["subscription", "subscription id", "subscriptionid", "account", "account id", "accountid", "account name", "accountname"]},
    {"key": "vnet_or_vpc", "label": "VNet / VPC", "aliases": ["vnet", "vnet name", "vnetname", "vpc", "network", "virtual network", "resource group", "resourcegroup"]},
    {"key": "disk_type", "label": "Disk Type / SKU", "required": True, "aliases": ["disk type", "disktype", "sku", "storage type", "tier", "disk sku", "type"]},
    {"key": "disk_size", "label": "Disk Size (GiB)", "required": True, "aliases": ["disk size", "disksize", "allocated (gb)", "allocated gb", "allocated", "size", "size (gb)", "capacity", "provisioned (gb)", "provisioned size", "disk size (gib)", "gib", "gb"]},
    {"key": "mbps", "label": "Throughput (MBps)", "aliases": ["mbps", "throughput", "bandwidth", "disk bw", "diskbw", "provisioned_bw", "provisioned bw", "throughput (mbps)", "bw"]},
    {"key": "iops", "label": "IOPS", "aliases": ["iops", "disk iops", "diskiops", "provisioned iops", "provisioned_iops", "disk iops (read+write)"]},
    {"key": "disk_status", "label": "Disk Status", "aliases": ["disk status", "disk state", "diskstate", "state", "status"]},
    {"key": "host_type", "label": "OS / Host Type", "aliases": ["os type", "ostype", "os", "operating system", "host type", "hosttype"]},
    {"key": "root_flag", "label": "OS/Root Disk Flag", "aliases": ["root", "os disk", "osdisk", "is os disk", "boot", "root flag"]},
    {"key": "count_compute", "label": "Compute Count (VMs)", "aliases": ["vm count", "vmcount", "count", "compute count", "number of vms", "num vms", "vms", "vm_name", "vm name", "vmname", "hostname", "host name", "machine name", "server name", "compute name"]},
    {"key": "disk_usage", "label": "Disk Usage / Used (GiB)", "aliases": ["used (gb)", "used gb", "used", "disk usage", "consumed (gb)", "used capacity", "used (gib)"]},
]
MAPPING_TEMPLATES_KEY = "TCO-GUI/_config/mapping_templates.json"
# Learned/edited header aliases per field, layered onto the constant catalog.
DATA_FIELDS_ALIASES_KEY = "TCO-GUI/_config/data_fields_aliases.json"

def _load_field_overrides():
    """Return {fieldKey: [aliases...]} of learned/edited aliases from S3, or {}."""
    try:
        obj = _s3_client().get_object(Bucket=s3_bucket, Key=DATA_FIELDS_ALIASES_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return {}
        raise
    except Exception:
        return {}

def _save_field_overrides(overrides):
    _s3_client().put_object(Bucket=s3_bucket, Key=DATA_FIELDS_ALIASES_KEY,
                            Body=json.dumps(overrides).encode("utf-8"),
                            ContentType="application/json")

def _effective_catalog():
    """The field catalog with each field's aliases replaced by the saved override
    when present (so learned/edited aliases drive auto-mapping)."""
    overrides = _load_field_overrides()
    out = []
    for f in DATA_FIELDS_CATALOG:
        aliases = overrides[f["key"]] if isinstance(overrides.get(f["key"]), list) else list(f["aliases"])
        # de-dupe, lowercased, keep order
        seen, al = set(), []
        for a in aliases:
            a = str(a).strip().lower()
            if a and a not in seen:
                seen.add(a); al.append(a)
        out.append({"key": f["key"], "label": f["label"], "required": bool(f.get("required")), "aliases": al})
    return out

def _upload_headers(upload_prefix):
    """Return the column headers of the uploaded CSV(s) under `upload_prefix`,
    exactly as pandas/main2 will see them (positional order)."""
    s3 = _s3_client()
    resp = s3.list_objects_v2(Bucket=s3_bucket, Prefix=upload_prefix)
    csvs = [o["Key"] for o in resp.get("Contents", []) if o["Key"].lower().endswith(".csv")]
    if not csvs:
        return None
    text = s3.get_object(Bucket=s3_bucket, Key=sorted(csvs)[0])["Body"].read().decode("utf-8")
    df = pd.read_csv(StringIO(text), nrows=0, on_bad_lines="warn")
    return [str(c) for c in df.columns.tolist()]

def _upload_sample_df(upload_prefix, scan_rows=2000):
    """Read up to `scan_rows` rows of the uploaded CSV as raw strings (so the
    mapping grid can show real sample values per column)."""
    s3 = _s3_client()
    resp = s3.list_objects_v2(Bucket=s3_bucket, Prefix=upload_prefix)
    csvs = [o["Key"] for o in resp.get("Contents", []) if o["Key"].lower().endswith(".csv")]
    if not csvs:
        return None
    text = s3.get_object(Bucket=s3_bucket, Key=sorted(csvs)[0])["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(text), nrows=scan_rows, dtype=str,
                       keep_default_na=False, on_bad_lines="warn")

def _column_samples(df, per_col=3):
    """Up to `per_col` unique, non-blank sample values per column (by index)."""
    out = {}
    blanks = {"", "nan", "none", "null", "na", "n/a"}
    for i, col in enumerate(df.columns):
        seen = []
        for v in df[col].tolist():
            s = ("" if v is None else str(v)).strip()
            if not s or s.lower() in blanks or s in seen:
                continue
            seen.append(s)
            if len(seen) >= per_col:
                break
        out[i] = seen
    return out

_BLANKS = {"", "nan", "none", "null", "na", "n/a"}

def _preview_rows(df, n=3):
    """Return up to `n` whole rows for a header-aligned data preview, preferring
    rows with the fewest blank cells so the sample shows real values. Rows keep
    their original order; each cell is a string."""
    if df is None or len(df) == 0:
        return []
    rows = df.values.tolist()
    def blank_count(row):
        c = 0
        for v in row:
            if ("" if v is None else str(v)).strip().lower() in _BLANKS:
                c += 1
        return c
    order = sorted(range(len(rows)), key=lambda i: (blank_count(rows[i]), i))
    pick = sorted(order[:n])
    return [[("" if v is None else str(v)) for v in rows[i]] for i in pick]

def _auto_map(headers, catalog=None):
    """Auto-map headers -> field keys by alias (first field wins per header; each
    field claims at most one column). Returns (columns, fieldIndexMap)."""
    catalog = catalog or _effective_catalog()
    alias_to_field = {}
    for f in catalog:
        for a in f["aliases"]:
            a = a.strip().lower()
            if a and a not in alias_to_field:
                alias_to_field[a] = f["key"]
    field_index = {f["key"]: -99 for f in catalog}
    columns = []
    for i, h in enumerate(headers):
        key = alias_to_field.get(str(h).strip().lower())
        # honour 1:1 — a field only claims its first matching column
        if key and field_index.get(key, -99) == -99:
            field_index[key] = i
            columns.append({"index": i, "header": h, "fieldKey": key, "matched": True})
        else:
            columns.append({"index": i, "header": h, "fieldKey": "", "matched": False})
    return columns, field_index

def _analysis_summary(results):
    """Serialize a main2() result dict into the summary the results modal shows."""
    if not isinstance(results, dict):
        return None
    if results.get("status") == 200:
        regions_val = results.get("regions")
        try:
            regions_list = regions_val.iloc[:, 0].tolist() if hasattr(regions_val, "iloc") else list(regions_val)
        except Exception:
            regions_list = []
        return {
            "status": 200,
            "num_groups": str(results.get("num_groups", 0)),
            "tot_capacity": str(results.get("tot_capacity", 0)),
            "tot_costs": round(float(results.get("tot_costs", 0) or 0), 2),
            "regions": regions_list,
        }
    return {"status": results.get("status"), "msg": results.get("msg", "No CSV data found.")}

@app.route("/api/mapping/fields", methods=["GET"])
@login_required
def mapping_fields():
    return jsonify({"ok": True, "fields": _effective_catalog()})

@app.route("/api/mapping/fields", methods=["PUT"])
@login_required
def mapping_fields_put():
    """Edit the saved header aliases per field (the mappings editor). Body:
    {fields: [{key, aliases:[...]}]} — stores an override alias list per field."""
    body = request.get_json(force=True) or {}
    incoming = body.get("fields")
    if not isinstance(incoming, list):
        return jsonify({"error": "A 'fields' array is required."}), 400
    valid_keys = {f["key"] for f in DATA_FIELDS_CATALOG}
    overrides = {}
    for f in incoming:
        key = str((f or {}).get("key", ""))
        if key not in valid_keys:
            continue
        aliases = (f or {}).get("aliases", [])
        seen, al = set(), []
        for a in (aliases if isinstance(aliases, list) else []):
            a = str(a).strip().lower()
            if a and a not in seen:
                seen.add(a); al.append(a)
        overrides[key] = al
    try:
        _save_field_overrides(overrides)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({"ok": True, "fields": _effective_catalog()})

@app.route("/api/mapping/analyze", methods=["POST"])
@login_required
def mapping_analyze():
    """Read the uploaded file's headers and propose a column->field mapping."""
    body = request.get_json(force=True) or {}
    upload_prefix = (session.get("upload_prefix") or str(body.get("upload_prefix", "")).strip())
    if not upload_prefix:
        return jsonify({"error": "No uploaded file found. Upload a file first."}), 400
    try:
        df = _upload_sample_df(upload_prefix)
    except Exception as exc:
        return jsonify({"error": f"Could not read uploaded file: {exc}"}), 500
    if df is None:
        return jsonify({"error": "No .csv file found for this upload."}), 404
    headers = [str(c) for c in df.columns.tolist()]
    catalog = _effective_catalog()
    columns, field_index = _auto_map(headers, catalog)
    # A small header-aligned data preview (up to 3 rows, preferring rows with the
    # fewest blanks) shown above the mapping grid.
    preview_rows = _preview_rows(df, 3)
    return jsonify({"ok": True, "headers": headers, "columns": columns,
                    "fieldIndexMap": field_index, "fields": catalog,
                    "preview_rows": preview_rows})

@app.route("/api/mapping/parse", methods=["POST"])
@login_required
def mapping_parse():
    """Build source_data_config from the finalized mapping and run the engine
    (writes parsed_data.csv + source_data_config.json to the run's results/ prefix)."""
    body = request.get_json(force=True) or {}
    incoming = body.get("fieldIndexMap")
    if not isinstance(incoming, dict):
        return jsonify({"error": "A fieldIndexMap object is required."}), 400
    upload_prefix = (session.get("upload_prefix") or str(body.get("upload_prefix", "")).strip())
    if not upload_prefix:
        return jsonify({"error": "No uploaded file found. Upload a file first."}), 400
    catalog = _effective_catalog()
    # Coerce every catalog field to an int index (-99 = unmapped / "Don't Use").
    field_index = {}
    for f in catalog:
        v = incoming.get(f["key"], -99)
        try:
            field_index[f["key"]] = int(v)
        except (TypeError, ValueError):
            field_index[f["key"]] = -99
    # Required fields (from the catalog) must be mapped.
    labels = {f["key"]: f["label"] for f in catalog}
    required = [f["key"] for f in catalog if f.get("required")]
    missing = [k for k in required if field_index.get(k, -99) == -99]
    if missing:
        return jsonify({"error": "Map required field(s) before parsing: " +
                        ", ".join(labels[k] for k in missing) + "."}), 400
    # Ensure results_prefix is set (main2 writes there); derive if needed.
    results_prefix = session.get("results_prefix")
    if not results_prefix:
        results_prefix = upload_prefix.rsplit("/data/", 1)[0] + "/results/"
        session["results_prefix"] = results_prefix
    session["upload_prefix"] = upload_prefix
    # Learn the manual/auto column mappings: add each mapped column's header as an
    # alias for its field so the same file auto-maps next time.
    try:
        headers = _upload_headers(upload_prefix) or []
        overrides = _load_field_overrides()
        base = {f["key"]: list(f["aliases"]) for f in catalog}
        changed = False
        for key, idx in field_index.items():
            if isinstance(idx, int) and 0 <= idx < len(headers):
                alias = str(headers[idx]).strip().lower()
                cur = overrides.get(key, base.get(key, []))
                if alias and alias not in [str(a).strip().lower() for a in cur]:
                    overrides[key] = cur + [alias]
                    changed = True
        if changed:
            _save_field_overrides(overrides)
    except Exception as exc:
        print(f"Warning: could not persist learned aliases: {exc}")
    # Build the config main2 consumes (becomes source_data_config.json verbatim).
    config = dict(field_index)
    config["cloud"] = "azure"
    config["name"] = str(body.get("name", "")).strip() or session.get("active_customer", "dataset")
    # Broad status allowlist so a mapped disk_status column doesn't over-filter.
    config["valid_disk_status"] = ["attached", "unattached", "reserved", "activesas",
                                   "readytoupload", "in-use", "available"]
    config["price_all_data_flag"] = 1
    # Zone fallback: when no zone column is mapped the engine assigns a fixed zone;
    # its default list is empty (which breaks), so supply a single zone (id 1).
    config["fixed_zone_count"] = 1
    config["fixed_zone_list"] = [1]
    config["default_zone_id"] = 1
    # Columns flagged "searchable" (persisted for a future search feature; not used
    # by the engine yet). Stored as the mapped column indices.
    searchable = body.get("searchable")
    if isinstance(searchable, list):
        idxs = []
        for v in searchable:
            try:
                idxs.append(int(v))
            except (TypeError, ValueError):
                pass
        config["searchable_columns"] = sorted(set(idxs))
    # Monthly snapshot rate chosen on the Parse Data page (fraction, 0..1). Baked into
    # the per-disk Azure cost at parse time (main2 -> calc_true_cost_azure) and persisted
    # in source_data_config.json so it is shown on selection and used as the analysis
    # default. Defaults to 0.1 (10%), matching main2's historical default.
    try:
        snap_rate = float(body.get("monthly_snapshot_rate", 0.1))
    except (TypeError, ValueError):
        snap_rate = 0.1
    config["monthly_snapshot_rate"] = max(0.0, min(1.0, snap_rate))
    session["json_config"] = config
    try:
        results = read_file_s3(upload_prefix)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify({"ok": True, "analysis": _analysis_summary(results),
                    "customer": session.get("active_customer", ""),
                    "scenario": session.get("active_scenario", "default")})

def _load_mapping_templates():
    try:
        obj = _s3_client().get_object(Bucket=s3_bucket, Key=MAPPING_TEMPLATES_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data if isinstance(data, list) else []
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return []
        raise
    except Exception:
        return []

def _save_mapping_templates(templates):
    _s3_client().put_object(Bucket=s3_bucket, Key=MAPPING_TEMPLATES_KEY,
                            Body=json.dumps(templates).encode("utf-8"),
                            ContentType="application/json")

@app.route("/api/mapping/templates", methods=["GET"])
@login_required
def mapping_templates_get():
    return jsonify({"ok": True, "templates": _load_mapping_templates()})

@app.route("/api/mapping/templates", methods=["POST"])
@login_required
def mapping_templates_post():
    body = request.get_json(force=True) or {}
    name = str(body.get("name", "")).strip()
    incoming = body.get("fieldIndexMap")
    if not name:
        return jsonify({"error": "Template name is required."}), 400
    if not isinstance(incoming, dict):
        return jsonify({"error": "A fieldIndexMap object is required."}), 400
    field_index = {}
    for f in DATA_FIELDS_CATALOG:
        try:
            field_index[f["key"]] = int(incoming.get(f["key"], -99))
        except (TypeError, ValueError):
            field_index[f["key"]] = -99
    templates = _load_mapping_templates()
    tpl = {"id": f"tpl-{uuid.uuid4()}", "name": name, "fieldIndexMap": field_index,
           "createdAt": datetime.now().strftime("%Y%m%d%H%M%S")}
    templates.append(tpl)
    try:
        _save_mapping_templates(templates)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({"ok": True, "template": tpl, "templates": templates})

@app.route("/api/mapping/templates/<tpl_id>", methods=["DELETE"])
@login_required
def mapping_templates_delete(tpl_id):
    templates = [t for t in _load_mapping_templates() if t.get("id") != tpl_id]
    try:
        _save_mapping_templates(templates)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({"ok": True, "templates": templates})

# ══════════════════════════════════════════════════════════
#  Workload Builder — model library + synthetic-workload build/parse
# ══════════════════════════════════════════════════════════
# Built-in models ship in the image (tools/workload_models.json); user-created
# models persist to a separate config in storage so they survive restarts.
WORKLOAD_MODELS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tools", "workload_models.json")
USER_MODELS_KEY = "TCO-GUI/_config/user_models.json"

# East US retail prices for INFORMATIONAL model-card estimates only (pulled
# 2026-07-20). The authoritative TCO still comes from Run Analysis (live pricing).
_PREMIUM_V1_TIERS = [(4, 5.28), (8, 5.28), (16, 5.28), (32, 5.28), (64, 10.207),
                     (128, 19.71), (256, 38.01), (512, 73.22), (1024, 135.17),
                     (2048, 259.05), (4096, 495.57), (8192, 979.0), (16384, 1937.0), (32767, 3844.0)]
_PV2_CAP, _PV2_IOPS, _PV2_MBPS = 0.0803, 0.00511, 0.04015
_PV2_FREE_IOPS, _PV2_FREE_MBPS = 3000, 125
_VM_MONTHLY = {"Standard_E8bds_v5": 487.64, "Standard_E16bds_v5": 975.28,
               "Standard_E32bds_v5": 1950.56, "Standard_E48bds_v5": 2925.84,
               "Standard_D4ds_v5": 164.98, "Standard_D8ds_v5": 329.96, "Standard_D16ds_v5": 659.92}

def _disk_monthly(dtype, cap, iops, mbps):
    t = (dtype or "").lower()
    cap = float(cap or 0)
    if "premiumv2" in t:
        return (cap * _PV2_CAP + max(0, (iops or 0) - _PV2_FREE_IOPS) * _PV2_IOPS
                + max(0, (mbps or 0) - _PV2_FREE_MBPS) * _PV2_MBPS)
    if "ultra" in t:
        return cap * 0.12 + (iops or 0) * 0.00042 + (mbps or 0) * 0.0033
    if "standardssd" in t:
        return cap * 0.075
    if "premium" in t:
        for mx, price in _PREMIUM_V1_TIERS:
            if cap <= mx:
                return price
        return _PREMIUM_V1_TIERS[-1][1]
    if "standard" in t:
        return cap * 0.04
    return 0.0

def _is_provisioned(dtype):
    t = (dtype or "").lower()
    return ("premiumv2" in t) or ("ultra" in t)

def _model_capacity_gib(model):
    return sum(float(d.get("capacityGB", 0)) for d in model.get("drive_config", []))

def _model_est_monthly(model):
    storage = sum(_disk_monthly(d.get("drive_type"), d.get("capacityGB"), d.get("iops"), d.get("mbps"))
                  for d in model.get("drive_config", []))
    vm = (model.get("suggested_compute", {}) or {}).get("vm_size", "")
    compute = _VM_MONTHLY.get(vm, 0.0)
    return {"storage": round(storage, 2), "compute": round(compute, 2), "total": round(storage + compute, 2)}

def _model_view(key, model, source):
    cap = _model_capacity_gib(model)
    return {
        "key": key, "source": source, "label": model.get("label", key),
        "family": model.get("family", "sql" if key.startswith("sql") else "common_server"),
        "vm_size": (model.get("suggested_compute", {}) or {}).get("vm_size", ""),
        "vm_note": (model.get("suggested_compute", {}) or {}).get("note", ""),
        "performance": model.get("totals", {}),
        "capacity_gib": round(cap, 1), "capacity_tib": round(cap / 1024.0, 3),
        "est_monthly": _model_est_monthly(model),
        "min_ec_sku": model.get("suggested_minimum_ec_sku", ""),
        "drive_config": model.get("drive_config", []),
    }

def _load_builtin_models():
    try:
        with open(WORKLOAD_MODELS_FILE, encoding="utf-8") as f:
            return json.load(f).get("models", {})
    except Exception as exc:
        print(f"Could not load built-in models: {exc}")
        return {}

def _load_user_models():
    try:
        obj = _s3_client().get_object(Bucket=s3_bucket, Key=USER_MODELS_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return {}
        raise
    except Exception:
        return {}

def _save_user_models(models):
    _s3_client().put_object(Bucket=s3_bucket, Key=USER_MODELS_KEY,
                            Body=json.dumps(models, indent=2).encode("utf-8"),
                            ContentType="application/json")

@app.route("/api/models", methods=["GET"])
@login_required
def models_get():
    builtin = _load_builtin_models()
    user = _load_user_models()
    out = [_model_view(k, v, "builtin") for k, v in builtin.items()]
    out += [_model_view(k, v, "user") for k, v in user.items()]
    return jsonify({"ok": True, "models": out})

@app.route("/api/models/user", methods=["POST"])
@login_required
def models_user_post():
    """Save a user-created model to the separate user_models config. Only a name
    and a storage layout (drive_config) are required; VM/performance are optional."""
    body = request.get_json(force=True) or {}
    name = str(body.get("name", "")).strip()
    if not name:
        return jsonify({"error": "A model name is required."}), 400
    key = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").lower() or "model"
    drives = body.get("drive_config") or []
    if not isinstance(drives, list) or not drives:
        return jsonify({"error": "At least one drive is required."}), 400
    clean = []
    for i, d in enumerate(drives):
        dt = str(d.get("drive_type", "")).strip()
        if not dt:
            return jsonify({"error": f"Drive {i + 1}: a disk type is required."}), 400
        try:
            cap = float(d.get("capacityGB"))
        except (TypeError, ValueError):
            return jsonify({"error": f"Drive {i + 1}: a capacity (GiB) is required."}), 400
        cd = {"drive_type": dt, "root": bool(d.get("root", False)), "capacityGB": cap}
        if d.get("role"):
            cd["role"] = str(d.get("role"))
        if _is_provisioned(dt):
            if d.get("iops") not in (None, ""):
                cd["iops"] = int(float(d["iops"]))
            if d.get("mbps") not in (None, ""):
                cd["mbps"] = int(float(d["mbps"]))
        clean.append(cd)
    model = {"label": str(body.get("label", name)).strip() or name,
             "family": "user", "user_created": True, "drive_config": clean}
    if isinstance(body.get("performance"), dict):
        model["totals"] = body["performance"]
    if str(body.get("vm_size", "")).strip():
        model["suggested_compute"] = {"vm_size": str(body["vm_size"]).strip()}
    if str(body.get("min_ec_sku", "")).strip():
        model["suggested_minimum_ec_sku"] = str(body["min_ec_sku"]).strip()
    users = _load_user_models()
    users[key] = model
    try:
        _save_user_models(users)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({"ok": True, "key": key, "model": _model_view(key, model, "user")})

@app.route("/api/models/user/<key>", methods=["DELETE"])
@login_required
def models_user_delete(key):
    users = _load_user_models()
    if key not in users:
        return jsonify({"error": "User model not found."}), 404
    del users[key]
    try:
        _save_user_models(users)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({"ok": True})

# Column order the synthetic inventory is emitted in (matches tools/generate_inventory.py)
# and the fixed field-index mapping main2 consumes for it.
_WL_COLUMNS = ["vm_name", "description", "region", "zone", "subscription", "vnet",
               "diskType", "capacity", "iops", "mbps", "status", "osType", "root", "minimum_ec_sku"]
_WL_FIELD_INDEX = {"count_compute": 0, "region": 2, "zone": 3, "subscription_or_account_id": 4,
                   "vnet_or_vpc": 5, "disk_type": 6, "disk_size": 7, "iops": 8, "mbps": 9,
                   "disk_status": 10, "host_type": 11, "root_flag": 12, "disk_usage": -99}

@app.route("/api/workload/save", methods=["POST"])
@login_required
def workload_save():
    """Build a synthetic inventory CSV from selected models, persist it under the
    chosen customer, then parse it (main2) so it is immediately available for
    Run Analysis — mirroring the upload -> map -> parse flow with a generated file."""
    import csv as _csv
    body = request.get_json(force=True) or {}
    customer = str(body.get("customer", "")).strip()
    if not customer:
        return jsonify({"error": "Associate the workload with a customer first."}), 400
    if not re.match(r'^[\w\-. ]+$', customer):
        return jsonify({"error": "Customer name may only contain letters, numbers, spaces, hyphens, underscores, periods."}), 400
    scenario = str(body.get("scenario", "")).strip() or "default"
    wl_name = str(body.get("workload_name", "")).strip() or f"workload-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    items = body.get("items") or []
    if not isinstance(items, list) or not items:
        return jsonify({"error": "Add at least one model to the workload."}), 400
    region = str(body.get("region", "")).strip() or "eastus"

    models = dict(_load_builtin_models())
    models.update(_load_user_models())   # user models can override by key

    # Resolve each item to a concrete instance count.
    resolved = []
    for it in items:
        mkey = str(it.get("model_key", "")).strip()
        model = models.get(mkey)
        if not model:
            return jsonify({"error": f"Unknown model '{mkey}'."}), 400
        cap = _model_capacity_gib(model)
        mode = str(it.get("mode", "count")).lower()
        if mode == "capacity":
            try:
                tib = float(it.get("capacity_tib"))
            except (TypeError, ValueError):
                return jsonify({"error": f"'{mkey}': a target capacity (TiB) is required."}), 400
            count = int((tib * 1024.0) // cap) if cap > 0 else 0   # round DOWN
        else:
            try:
                count = int(it.get("count"))
            except (TypeError, ValueError):
                return jsonify({"error": f"'{mkey}': a count is required."}), 400
        if count < 1:
            continue
        resolved.append({"key": mkey, "model": model, "count": count,
                         "capacity_per_gib": cap, "actual_capacity_gib": round(count * cap, 1),
                         "region": str(it.get("region", "")).strip() or region,
                         "zone": str(it.get("zone", "0")).strip() or "0",
                         "vnet": str(it.get("vnet", "")).strip() or "vnet1"})
    if not resolved:
        return jsonify({"error": "The workload is empty (every item rounded down to 0 instances)."}), 400

    # Build the CSV rows.
    rows = []
    for idx, r in enumerate(resolved):
        mkey, model, count = r["key"], r["model"], r["count"]
        os_type = "Windows" if str(mkey).lower().startswith("sql") else "Linux"
        min_sku = model.get("suggested_minimum_ec_sku", "none") or "none"
        # Full model key + the cart-item index keep instance names unique even when
        # the same model is added more than once (e.g. to different zones/vnets);
        # grouping in the analysis is by region/zone/subscription/vnet columns.
        prefix = re.sub(r"[^A-Za-z0-9]+", "_", mkey).strip("_") or "vm"
        for i in range(count):
            vm_name = f"{prefix}-{idx:02d}-{i:04d}"
            for d in model.get("drive_config", []):
                prov = _is_provisioned(d.get("drive_type"))
                rows.append([
                    vm_name, mkey, r["region"], r["zone"], customer, r["vnet"],
                    d.get("drive_type", ""), d.get("capacityGB", ""),
                    (d.get("iops", "") if prov else ""), (d.get("mbps", "") if prov else ""),
                    "Attached", os_type, ("True" if d.get("root") else "False"), min_sku,
                ])
    buf = StringIO()
    w = _csv.writer(buf)
    w.writerow(_WL_COLUMNS)
    w.writerows(rows)
    csv_text = buf.getvalue()

    # Ensure the customer exists, and make it the active customer/scenario.
    try:
        customers = _load_customer_list()
        if customer not in customers:
            customers.append(customer)
            _save_customer_list(customers)
    except Exception:
        pass
    session["active_customer"] = customer
    session["active_scenario"] = scenario

    # Establish the dated upload/results prefixes (same scheme as presign).
    username = session.get("username", "unknown")
    dt = datetime.now().strftime("%Y%m%d%H%M%S")
    session["date_time_str"] = dt
    upload_prefix = f"TCO-GUI/{username}/{customer}/{scenario}/{dt}/data/"
    results_prefix = f"TCO-GUI/{username}/{customer}/{scenario}/{dt}/results/"
    session["upload_prefix"] = upload_prefix
    session["results_prefix"] = results_prefix
    safe_name = "".join(c for c in wl_name if c.isalnum() or c in "._- ")[:100] or "workload"
    object_key = f"{upload_prefix}{safe_name}.csv"
    try:
        _s3_client().put_object(Bucket=s3_bucket, Key=object_key,
                                Body=csv_text.encode("utf-8"), ContentType="text/csv")
    except Exception as exc:
        return jsonify({"error": f"Could not store the generated workload: {exc}"}), 500

    # Fixed column mapping for the generated CSV, then parse (main2) so the dataset
    # is available in Results.
    config = dict(_WL_FIELD_INDEX)
    config["cloud"] = "azure"
    config["name"] = wl_name
    config["valid_disk_status"] = ["attached", "unattached", "reserved", "activesas",
                                   "readytoupload", "in-use", "available"]
    config["price_all_data_flag"] = 1
    config["fixed_zone_count"] = 1
    config["fixed_zone_list"] = [1]
    config["default_zone_id"] = 1
    session["json_config"] = config
    try:
        results = read_file_s3(upload_prefix)
    except Exception as exc:
        return jsonify({"error": f"Workload stored but parsing failed: {exc}"}), 400

    # Persist the workload definition (recipe) so it can be reloaded as a starting
    # point for a new workload — stored customer-agnostically.
    try:
        _upsert_workload(wl_name, region, _sanitize_wl_items(items), customer)
    except Exception as exc:
        print(f"Warning: could not save workload definition: {exc}")

    total_vms = sum(r["count"] for r in resolved)
    total_cap = round(sum(r["actual_capacity_gib"] for r in resolved), 1)
    return jsonify({
        "ok": True, "customer": customer, "scenario": scenario, "workload_name": wl_name,
        "total_vms": total_vms, "total_disks": len(rows), "total_capacity_gib": total_cap,
        "resolved": [{"model": r["key"], "count": r["count"],
                      "region": r["region"], "zone": r["zone"], "vnet": r["vnet"],
                      "actual_capacity_gib": r["actual_capacity_gib"],
                      "actual_capacity_tib": round(r["actual_capacity_gib"] / 1024.0, 2)} for r in resolved],
        "analysis": _analysis_summary(results),
    })

# ── Saved workload definitions (reusable recipes, customer-agnostic) ──
SAVED_WORKLOADS_KEY = "TCO-GUI/_config/saved_workloads.json"

def _load_saved_workloads():
    try:
        obj = _s3_client().get_object(Bucket=s3_bucket, Key=SAVED_WORKLOADS_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return {}
        raise
    except Exception:
        return {}

def _save_saved_workloads(d):
    _s3_client().put_object(Bucket=s3_bucket, Key=SAVED_WORKLOADS_KEY,
                            Body=json.dumps(d, indent=2).encode("utf-8"),
                            ContentType="application/json")

def _sanitize_wl_items(items):
    out = []
    for it in (items or []):
        mk = str(it.get("model_key", "")).strip()
        if not mk:
            continue
        place = {"region": str(it.get("region", "")).strip() or "eastus",
                 "zone": str(it.get("zone", "0")).strip() or "0",
                 "vnet": str(it.get("vnet", "")).strip() or "vnet1"}
        mode = str(it.get("mode", "count")).lower()
        if mode == "capacity":
            try:
                out.append({"model_key": mk, "mode": "capacity", "capacity_tib": float(it.get("capacity_tib")), **place})
            except (TypeError, ValueError):
                continue
        else:
            try:
                out.append({"model_key": mk, "mode": "count", "count": int(it.get("count")), **place})
            except (TypeError, ValueError):
                continue
    return out

def _upsert_workload(name, region, items, customer=None):
    d = _load_saved_workloads()
    wid = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").lower() or "workload"
    now = datetime.now().isoformat()
    existing = d.get(wid, {})
    wl = {"id": wid, "name": name, "region": region or "eastus", "items": items,
          "created": existing.get("created", now), "updated": now,
          "last_customer": customer or existing.get("last_customer", "")}
    d[wid] = wl
    _save_saved_workloads(d)
    return wl

@app.route("/api/workloads", methods=["GET"])
@login_required
def workloads_list():
    d = _load_saved_workloads()
    items = sorted(d.values(), key=lambda w: w.get("updated", ""), reverse=True)
    return jsonify({"ok": True, "workloads": items})

@app.route("/api/workloads", methods=["POST"])
@login_required
def workloads_save_def():
    """Save a workload definition (recipe) without parsing — a reusable, customer-agnostic template."""
    body = request.get_json(force=True) or {}
    name = str(body.get("name", "")).strip()
    if not name:
        return jsonify({"error": "A workload name is required to save it."}), 400
    items = _sanitize_wl_items(body.get("items"))
    if not items:
        return jsonify({"error": "Add at least one model before saving the workload."}), 400
    region = str(body.get("region", "")).strip() or "eastus"
    try:
        wl = _upsert_workload(name, region, items, body.get("customer"))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({"ok": True, "workload": wl})

@app.route("/api/workloads/<wid>", methods=["DELETE"])
@login_required
def workloads_delete(wid):
    d = _load_saved_workloads()
    if wid not in d:
        return jsonify({"error": "Saved workload not found."}), 404
    del d[wid]
    try:
        _save_saved_workloads(d)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({"ok": True})

def _clean_drive_config(drives):
    """Validate/normalize an incoming drive_config list (import / user model)."""
    clean = []
    for d in (drives or []):
        dt = str(d.get("drive_type", "")).strip()
        if not dt:
            continue
        try:
            cap = float(d.get("capacityGB"))
        except (TypeError, ValueError):
            continue
        cd = {"drive_type": dt, "root": bool(d.get("root", False)), "capacityGB": cap}
        if d.get("role"):
            cd["role"] = str(d.get("role"))
        if _is_provisioned(dt):
            if d.get("iops") not in (None, ""):
                cd["iops"] = int(float(d["iops"]))
            if d.get("mbps") not in (None, ""):
                cd["mbps"] = int(float(d["mbps"]))
        clean.append(cd)
    return clean

@app.route("/api/workload/import", methods=["POST"])
@login_required
def workload_import():
    """Import a workload config file: register any custom models it carries (under
    their original keys, without overriding built-ins), and return the resolved
    recipe so the builder can load it. Makes a config portable across deployments."""
    body = request.get_json(force=True) or {}
    items = _sanitize_wl_items(body.get("items"))
    if not items:
        return jsonify({"error": "This config file has no workload items."}), 400
    name = str(body.get("name", "")).strip()
    region = str(body.get("region", "")).strip() or "eastus"
    models = body.get("models") if isinstance(body.get("models"), dict) else {}
    builtin = _load_builtin_models()
    users = _load_user_models()
    registered = []
    for key, m in models.items():
        k = re.sub(r"[^A-Za-z0-9]+", "_", str(key)).strip("_").lower()
        if not k or k in builtin:            # never override a built-in model
            continue
        drives = _clean_drive_config((m or {}).get("drive_config"))
        if not drives:
            continue
        md = {"label": str((m or {}).get("label", k)).strip() or k,
              "family": "user", "user_created": True, "drive_config": drives}
        if str((m or {}).get("vm_size", "")).strip():
            md["suggested_compute"] = {"vm_size": str(m["vm_size"]).strip()}
        if str((m or {}).get("min_ec_sku", "")).strip():
            md["suggested_minimum_ec_sku"] = str(m["min_ec_sku"]).strip()
        users[k] = md
        registered.append(k)
    if registered:
        try:
            _save_user_models(users)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500
    known = set(builtin) | set(users)
    missing = sorted({it["model_key"] for it in items if it["model_key"] not in known})
    return jsonify({"ok": True, "name": name, "region": region, "items": items,
                    "registered": registered, "missing": missing})

# ══════════════════════════════════════════════════════════
#  Backup / restore — export or import the entire data set
# ══════════════════════════════════════════════════════════
# Everything the app persists lives under the TCO-GUI/ prefix in the active
# storage backend (customer list, EC/EC-AN config, user models, saved workloads,
# mapping templates, and every customer's uploads / parsed data / TCO runs).
BACKUP_PREFIX = "TCO-GUI/"

@app.route("/api/backup/export", methods=["GET"])
@login_required
def backup_export():
    """Download the entire data set (everything under TCO-GUI/) as a single ZIP."""
    import zipfile
    s3 = _s3_client()
    buf = BytesIO()
    count = 0
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=BACKUP_PREFIX):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                try:
                    data = s3.get_object(Bucket=s3_bucket, Key=key)["Body"].read()
                except Exception as exc:
                    print(f"backup: skipped {key}: {exc}")
                    continue
                z.writestr(key, data)
                count += 1
        z.writestr("_backup_manifest.json", json.dumps({
            "app": "everpure-azure-tco", "created": datetime.now().isoformat(),
            "object_count": count,
            "storage": _storage_offering_location(_session_storage()),
        }, indent=2))
    buf.seek(0)
    fname = f"everpure-backup-{datetime.now().strftime('%Y%m%d%H%M%S')}.zip"
    return Response(buf.getvalue(), mimetype="application/zip",
                    headers={"Content-Disposition": f'attachment; filename="{fname}"'})

@app.route("/api/backup/import", methods=["POST"])
def backup_import():
    """Restore/merge a backup ZIP into the active storage backend. Adds every
    object from the archive on top of what is already loaded (same key = replaced).
    Available from the login page (pre-auth) — writes only under TCO-GUI/."""
    import zipfile
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "No backup file was provided."}), 400
    try:
        z = zipfile.ZipFile(BytesIO(f.read()))
    except Exception as exc:
        return jsonify({"error": f"That is not a valid backup .zip ({exc})."}), 400
    s3 = _s3_client()
    imported, skipped = 0, 0
    for name in z.namelist():
        if name.endswith("/") or name == "_backup_manifest.json":
            continue
        # Only restore app data, and refuse path traversal.
        if not name.startswith(BACKUP_PREFIX) or ".." in name.split("/"):
            skipped += 1
            continue
        try:
            s3.put_object(Bucket=s3_bucket, Key=name, Body=z.read(name), ContentType=_guess_ct(name))
            imported += 1
        except Exception as exc:
            print(f"import: could not write {name}: {exc}")
            skipped += 1
    if imported == 0:
        return jsonify({"error": "No importable objects found (a backup .zip contains "
                                 "TCO-GUI/… entries)."}), 400
    return jsonify({"ok": True, "imported": imported, "skipped": skipped})

def get_ec_size_n_cost_data(regions, customer, directory, ec):

    file_name_ec_cost = "ec_infra_resource_costs.csv"
    output_bucket_prefix = session["results_prefix"]
    cached_key = f"{output_bucket_prefix}{file_name_ec_cost}"
    cached_df = _load_df_from_s3(cached_key)
    if cached_df is not None:
        # print(f"Using cached ec_infra_resource_costs.csv from S3.")
        return cached_df
    else:
        # print("ec infra cost file not found - generating")
        hours_per_month = 730
        all_data = []

        # Generate Dataframe for EC component Azure retail costs. Each region is
        # independent (only reads shared, read-only config), so process regions
        # concurrently on a thread pool — the per-region body below builds its own
        # local `data` list which is merged into all_data.
        def _one_region(region_name):
            data = []
            hours_per_month = 730   # local default (reassigned below; must be defined for the osDisk branch)

            # Controllers
            cost_type = "Controller"
            service = "Virtual Machines"
            ec_models = list(ec)
            # print(ec_models)
            for model in ec_models:
                service_name = service
                # product = "Azure Premium SSD v2"
                # print(ec[model])
                vm = ec[model].get("VM")
                vm_enc = urllib.parse.quote(vm)
                service_enc = urllib.parse.quote(service_name)
                type_list = ["Consumption", "Reservation"]
                for type in type_list:
                    type_filter = f"%20and%20type%20eq%20%27{type}%27"
                    arm_sku_name_filter = f"%20and%20armSkuName%20eq%20%27{vm_enc}%27"
                    arm_region_name_filter = f"%20and%20armRegionName%20eq%20%27{region_name}%27"
                    service_name_filter = f"serviceName%20eq%20%27{service_enc}%27"
                    url_2 = f"https://prices.azure.com/api/retail/prices?$filter={service_name_filter}{arm_region_name_filter}{arm_sku_name_filter}{type_filter}"
                    filter_out_field = [
                        {"field": "productName", "value": "Windows"},
                        {"field": "meterName", "value": "Spot"},
                        {"field": "meterName", "value": "Low"},
                    ]
                    not_done_api = True
                    while not_done_api:
                        response = requests.get(url_2)
                        #print(response.url)
                        if response.status_code == 200:
                            not_done_api = False
                        elif response.status_code == 429:
                            time.sleep(1)
                        else:
                            raise f"api error {response.status_code}"
                    vm_list = json.loads(response.content).get("Items", [])

                    for st in vm_list:
                        keep = True
                        for flt_out in filter_out_field:
                            if flt_out.get("value") in st.get(flt_out["field"]):
                                keep = False
                        if keep:
                            retail_price = st.get("retailPrice")
                            type_c = st.get("type")
                            if type_c == 'Reservation':
                                term = st.get('reservationTerm')
                                if "1" in term:
                                    term_len = 12
                                elif "3" in term:
                                    term_len = 36
                                else:
                                    raise f"reservationTerm error {term}"
                                month_rate = retail_price / term_len
                            else:  # Hourly consumption based
                                term = "onDemand"
                                hours_per_month = 730
                                term_len = 1 / hours_per_month
                                month_rate = retail_price / term_len

                            v = {
                                "service": service,
                                "type": type,
                                "armSkuName": vm,
                                "productName": st.get("productName"),
                                "meterName": st.get("meterName"),
                                "unitOfMeasure": st.get("unitOfMeasure", None),
                                "armRegionName": region_name,
                                "term": term,
                                "monthRate": month_rate,
                                "retailPrice": float(retail_price),
                                'tierMinimumUnits': None,
                                "pscSku": ec[model].get("pscSku"),
                                "costType": cost_type
                            }
                            data.append(v)
                            # print(st)
            # OS Disk
            service = 'Storage'
            product_names = ['Premium SSD Managed Disks', 'Azure Premium SSD v2']
            meter_name = 'P10 LRS Disk'
            os_disk_meter_name = 'P10 LRS Disk'
            prem_v2_meter_list = [
                "Premium LRS Provisioned Capacity",
                "Premium LRS Provisioned IOPS",
                "Premium LRS Provisioned Throughput (MBps)"
            ]
            for pn in product_names:
                service_name = service
                if "v2" in pn:
                    pn_enc = urllib.parse.quote(pn)
                    product_filter = f"%20and%20productName%20eq%20%27{pn_enc}%27"
                else:
                    pn1_enc = urllib.parse.quote(pn)
                    pn2_enc = urllib.parse.quote(meter_name)
                    product_filter = f"%20and%20productName%20eq%20%27{pn1_enc}%27%20and%20meterName%20eq%20%27{pn2_enc}%27"
                service_enc = urllib.parse.quote(service_name)

                arm_region_name_filter = f"%20and%20armRegionName%20eq%20%27{region_name}%27"
                service_name_filter = f"serviceName%20eq%20%27{service_enc}%27"
                url_2 = f"https://prices.azure.com/api/retail/prices?$filter={service_name_filter}{arm_region_name_filter}{product_filter}"
                # filter_out_field = [
                #    {"field": "productName", "value": "Windows"},
                #    {"field": "meterName", "value": "Spot"},
                #    {"field": "meterName", "value": "Low"},
                # ]
                not_done_api = True
                while not_done_api:
                    response = requests.get(url_2)
                    #print(response.url)
                    if response.status_code == 200:
                        not_done_api = False
                    elif response.status_code == 429:
                        time.sleep(1)
                    else:
                        raise f"api error {response.status_code}"
                drive_list = json.loads(response.content).get("Items", [])

                for st in drive_list:
                    if "v2" in pn and st.get('meterName') in prem_v2_meter_list and float(st.get('retailPrice')) > 0:
                        cost_type = "raidDisk"
                        # print(st)
                        retail_price = st.get('retailPrice')
                        month_rate = retail_price * hours_per_month
                        v = {
                            "service": service,
                            "type": None,
                            "armSkuName": None,
                            "productName": pn,
                            "meterName": st.get("meterName"),
                            "unitOfMeasure": st.get("unitOfMeasure", None),
                            "armRegionName": region_name,
                            "term": None,
                            "monthRate": month_rate,
                            "retailPrice": float(retail_price),
                            'tierMinimumUnits': st.get('tierMinimumUnits'),
                            "pscSku": None,
                            "costType": cost_type
                        }
                        data.append(v)
                    elif st.get('meterName') == os_disk_meter_name:
                        cost_type = "osDisk"
                        retail_price = st.get('retailPrice')
                        month_rate = retail_price
                        v = {
                            "service": service,
                            "type": None,
                            "armSkuName": None,
                            "productName": pn,
                            "meterName": st.get("meterName"),
                            "unitOfMeasure": st.get("unitOfMeasure", None),
                            "armRegionName": region_name,
                            "term": None,
                            "monthRate": month_rate,
                            "retailPrice": float(retail_price),
                            "pscSku": None,
                            "costType": cost_type
                        }
                        data.append(v)
            return data

        with ThreadPoolExecutor(max_workers=AZURE_PRICE_WORKERS) as _ex:
            for _rows in _ex.map(_one_region, regions):
                all_data.extend(_rows)
        # for d in data:
        #    print(d)
        df_ec_pricing = pd.DataFrame(all_data)
        file_name_ec_cost = "ec_infra_resource_costs.csv"
        output_bucket_prefix = session["results_prefix"]
        object_prefix = f"{output_bucket_prefix}"
        # Use the put_object API to upload the CSV data
        status_up = upload_df_to_s3(df_ec_pricing, file_name_ec_cost, object_prefix)
        # print(f"ec_infra_resource_file upload status {status_up}")

        return df_ec_pricing


# Per-parse cache for Azure retail-price lookups. calc_true_cost_azure is applied
# per disk row; without caching it re-scans the entire pricing frame for each of a
# dataset's (up to tens of thousands of) rows. The same (region, meter/sku) combo
# recurs constantly, so caching the unique-price result collapses ~4 full-frame
# scans/row down to one scan per distinct combo. Cleared at the start of each parse.
_price_lookup_cache = {}


def _cup(azure_pricing, conds):
    """Cached unique retailPrice lookup for a set of {column: value} equality
    filters. Returns the same numpy array as azure_pricing.loc[mask, 'retailPrice']
    .unique() — results are identical to the pre-cache code, only faster."""
    key = tuple(sorted((k, str(v)) for k, v in conds.items()))
    cached = _price_lookup_cache.get(key)
    if cached is not None:
        return cached
    mask = None
    for col, val in conds.items():
        m = azure_pricing[col] == val
        mask = m if mask is None else (mask & m)
    res = azure_pricing.loc[mask, "retailPrice"].unique()
    _price_lookup_cache[key] = res
    return res


def calc_true_cost_azure(row, azure_pricing):
    global iops
    global mbps
    global region
    global disk_type
    global monthly_snapshot_rate
    global disk_size
    global efficiency
    hours_per_month = 730
    # print(type(azure_pricing))
    sku_col_name = "skuName"
    meter_col_name = "meterName"
    region_col_name = "region"
    retail_price_col_name = "retailPrice"
    lrs_snapshot_sku_name = 'Snapshots LRS'
    product_name = 'productName'
    ssd_product_name = "Standard SSD Managed Disks"
    hdd_product_name = "Standard HDD Managed Disks"

    disk_size_val = int(row[disk_size]) if pd.notna(row[disk_size]) else 0

    disk_type_name = row[disk_type]
    region_name = row[region]
    # print(f"reg name {region_name}")
    # print(azure_pricing.shape, region_col_name)

    mode = "LRS"
    if "ZRS" in disk_type_name.upper():
        mode = "ZRS"
    if "ultra" in disk_type_name.lower():
        base_2_sizes = [2 ** i for i in range(2, 11)]
        next2 = base_2_sizes[-1]
        while next2 < 65536:
            next2 = next2 + 1024
            base_2_sizes.append(next2)
        if pd.isna(row[iops]):
            iops_val = 1
        else:
            iops_val = int(row[iops])
        if pd.isna(row[mbps]):
            mbps_val = 1
        else:
            mbps_val = int(row[mbps])
        lrs_snapshots = _cup(azure_pricing, {region_col_name: region_name,
                                             product_name: hdd_product_name,
                                             sku_col_name: lrs_snapshot_sku_name})
        lrs_snapshot_price = 0.018
        if len(lrs_snapshots) == 1:
            lrs_snapshot_price = lrs_snapshots[0]
        else:
            #print(f"found multiple or no LRS snapshot prices {lrs_snapshots}")
            if len(lrs_snapshots) > 1:
                lrs_snapshot_price = lrs_snapshots[0]
        #   print(disk_type_name)
        # disk_type_name_str = "ultra"
        # size_class = "Ultra LRS"
        cap_meter = "Ultra LRS Provisioned Capacity"
        iops_meter = "Ultra LRS Provisioned IOPS"
        bw_meter = "Ultra LRS Provisioned Throughput (MBps)"
        capacity_prices = _cup(azure_pricing, {region_col_name: region_name, meter_col_name: cap_meter})
        eligible_sizes = [num for num in base_2_sizes if num >= disk_size_val]
        # print(eligible_sizes)
        # If the filtered list is not empty, return the maximum value
        #if eligible_sizes:
        #    ultra_paid_size = min(eligible_sizes)
        #else:
        #    ultra_paid_size = max(base_2_sizes)
        #   print(f"capacity prices {capacity_prices}")
        if len(capacity_prices) == 1:
            capacity_price = float(capacity_prices[0]) * disk_size_val * hours_per_month
        else:
            #print(f"multiple or no capacity prices {capacity_prices} for {region_name} - {cap_meter}")
            capacity_price = 0
        iops_prices = _cup(azure_pricing, {region_col_name: region_name, meter_col_name: iops_meter})
        #    print(f"iops prices {iops_prices}")
        iops_price = 0
        if len(iops_prices) == 1:
            iops_price = float(iops_prices[0]) * (iops_val - 0) * hours_per_month
        else:
            print(f"multiple or no iops prices {iops_prices} for {region_name} - {iops_meter}")
        bw_prices = _cup(azure_pricing, {region_col_name: region_name, meter_col_name: bw_meter})
        #    print(disk_type_name)
        #    print(f"capacity prices {capacity_prices}")
        #    print(f"iops prices {iops_prices}")
        #    print(f"bw prices {bw_prices}")
        bw_price = 0
        if len(bw_prices) == 1:
            bw_price = float(bw_prices[0]) * (mbps_val - 0) * hours_per_month
        else:
            print(f"multiple or no bw prices {bw_prices} for {region_name} - {bw_meter}")
        # 04/06 Option to ingore all snaphot costs if change rate is zero
        if float(monthly_snapshot_rate) == 0:
            snapshot_price = 0
        else:

            snapshot_price = float(efficiency) * disk_size_val * ((float(monthly_snapshot_rate) / 2) + 1) * lrs_snapshot_price
        total_cost = iops_price + bw_price + capacity_price + snapshot_price
        return pd.Series(
            {"total_cost": total_cost, "cap_cost": capacity_price, "iops_cost": iops_price, "mbps_cost": bw_price,
             "snap_cost": snapshot_price, "mode": mode, "paid_capacity": disk_size_val, iops: iops_val,
             mbps: mbps_val})
    elif "standardssd" in disk_type_name.lower():
        iops_val = 500
        mbps_val = 100

        lrs_snapshots = _cup(azure_pricing, {region_col_name: region_name,
                                             product_name: hdd_product_name,
                                             sku_col_name: lrs_snapshot_sku_name})
        lrs_snapshot_price = 0.018
        if len(lrs_snapshots) == 1:
            lrs_snapshot_price = lrs_snapshots[0]
        else:
            #print(f"found multiple or no LRS snapshot prices {lrs_snapshots}")
            if len(lrs_snapshots) > 1:
                lrs_snapshot_price = lrs_snapshots[0]
        disk_type_name_str = "standardssd"
        size = disk_size_val
        paid_cap = 0
        if size <= 4:
            size_class = f"E1 {mode}"
            paid_cap = 4
        elif size <= 8:
            size_class = f"E2 {mode}"
            paid_cap = 8
        elif size <= 16:
            size_class = f"E3 {mode}"
            paid_cap = 16
        elif size <= 32:
            size_class = f"E4 {mode}"
            paid_cap = 32
        elif size <= 64:
            size_class = f"E6 {mode}"
            paid_cap = 64
        elif size <= 128:
            size_class = f"E10 {mode}"
            paid_cap = 128
        elif size <= 256:
            size_class = f"E15 {mode}"
            paid_cap = 256
        elif size <= 512:
            size_class = f"E20 {mode}"
            paid_cap = 512
        elif size <= 1024:
            size_class = f"E30 {mode}"
            paid_cap = 1024
        elif size <= 2048:
            size_class = f"E40 {mode}"
            paid_cap = 2048
        elif size <= 4096:
            size_class = f"E50 {mode}"
            paid_cap = 4096
        elif size <= 8192:
            iops_val = 2000
            mbps_val = 400
            size_class = f"E60 {mode}"
            paid_cap = 8192
        elif size <= 16384:
            iops_val = 4000
            mbps_val = 600
            size_class = f"E70 {mode}"
            paid_cap = 16384
        elif size <= 32768:
            iops_val = 6000
            mbps_val = 750
            size_class = f"E80 {mode}"
            paid_cap = 32768
        else:
            #print(f"error in size value {size} standard ssd")
            return pd.Series(
                {"total_cost": 0, "cap_cost": 0, "iops_cost": 0, "mbps_cost": 0, "snap_cost": 0, "mode": "remove",
                 "paid_capacity": 0, iops: iops_val, mbps: mbps_val})
        capacity_prices = _cup(azure_pricing, {region_col_name: region_name, sku_col_name: size_class})
        if len(capacity_prices) == 1:
            capacity_price = capacity_prices[0]
        else:
            #print(f"multiple or no capacity prices {capacity_prices} for {region_name} - {size_class}")
            capacity_price = 0
        iops_price = 0
        bw_price = 0
        # 04/06 MR Option to ingore all snaphot costs if change rate is zero
        if float(monthly_snapshot_rate) == 0:
            snapshot_price = 0
        else:
            snapshot_price = float(efficiency) * disk_size_val * ((float(monthly_snapshot_rate) / 2) + 1) * lrs_snapshot_price
        total_cost = iops_price + bw_price + capacity_price + snapshot_price
        # print(disk_type_name)
        #    print(f"capacity prices {capacity_prices}")

        return pd.Series(
            {"total_cost": total_cost, "cap_cost": capacity_price, "iops_cost": iops_price, "mbps_cost": bw_price,
             "snap_cost": snapshot_price, "mode": mode, "paid_capacity": paid_cap, iops: iops_val, mbps: mbps_val})
    elif "standard" in disk_type_name.lower():
        iops_val = 500
        mbps_val = 60
        lrs_snapshots = _cup(azure_pricing, {region_col_name: region_name,
                                             product_name: hdd_product_name,
                                             sku_col_name: lrs_snapshot_sku_name})
        lrs_snapshot_price = 0.018
        if len(lrs_snapshots) == 1:
            lrs_snapshot_price = lrs_snapshots[0]
        else:
            #print(f"found multiple or no LRS snapshot prices {lrs_snapshots}")
            if len(lrs_snapshots) > 1:
                lrs_snapshot_price = lrs_snapshots[0]
        # disk_type_name_str = "standard"
        size = disk_size_val
        paid_cap = 0
        if size <= 32:
            size_class = f"S4 {mode}"
            paid_cap = 32
        elif size <= 64:
            size_class = f"S6 {mode}"
            paid_cap = 64
        elif size <= 128:
            size_class = f"S10 {mode}"
            paid_cap = 128
        elif size <= 256:
            size_class = f"S15 {mode}"
            paid_cap = 256
        elif size <= 512:
            size_class = f"S20 {mode}"
            paid_cap = 512
        elif size <= 1024:
            size_class = f"S30 {mode}"
            paid_cap = 1024
        elif size <= 2048:
            size_class = f"S40 {mode}"
            paid_cap = 2048
        elif size <= 4096:
            size_class = f"S50 {mode}"
            paid_cap = 4096
        elif size <= 8192:
            iops_val = 1300
            mbps_val = 300
            size_class = f"S60 {mode}"
            paid_cap = 8192
        elif size <= 16384:
            iops_val = 2000
            mbps_val = 500
            size_class = f"S70 {mode}"
            paid_cap = 16384
        elif size <= 32768:
            iops_val = 2000
            mbps_val = 500
            size_class = f"S80 {mode}"
            paid_cap = 32768
        else:
            #print(f"error in size value {size} standard")
            return pd.Series(
                {"total_cost": 0, "cap_cost": 0, "iops_cost": 0, "mbps_cost": 0, "snap_cost": 0, "mode": "remove",
                 "paid_capacity": 0, iops: iops_val, mbps: mbps_val})
        capacity_prices = _cup(azure_pricing, {region_col_name: region_name, sku_col_name: size_class})
        if len(capacity_prices) == 1:
            capacity_price = capacity_prices[0]
        else:
            print(f"multiple or no capacity prices {capacity_prices} for {region_name} - {size_class}")
            capacity_price = 0
        iops_price = 0
        bw_price = 0
        # 04/06 MR Option to ingore all snaphot costs if change rate is zero
        if float(monthly_snapshot_rate) == 0:
            snapshot_price = 0
        else:
            snapshot_price = float(efficiency) * disk_size_val * ((float(monthly_snapshot_rate) / 2) + 1) * lrs_snapshot_price
        total_cost = iops_price + bw_price + capacity_price + snapshot_price
        # print(disk_type_name)
        #    print(f"capacity prices {capacity_prices}")

        return pd.Series(
            {"total_cost": total_cost, "cap_cost": capacity_price, "iops_cost": iops_price, "mbps_cost": bw_price,
             "snap_cost": snapshot_price, "mode": mode, "paid_capacity": paid_cap, iops: iops_val, mbps: mbps_val})
    elif "premiumv2" in disk_type_name.lower():
        #print("in premv2 costing")
        if pd.isna(row[iops]) or row[iops] < 3000:
            iops_val = 3000
        else:
            iops_val = int(row[iops])
        if pd.isna(row[mbps]) or row[mbps] < 125:
            mbps_val = 125
        else:
            mbps_val = int(row[mbps])
        lrs_snapshots = _cup(azure_pricing, {region_col_name: region_name,
                                             product_name: hdd_product_name,
                                             sku_col_name: lrs_snapshot_sku_name})
        lrs_snapshot_price = 0.018
        if len(lrs_snapshots) == 1:
            lrs_snapshot_price = lrs_snapshots[0]
        else:
            print(f"found multiple or no LRS snapshot prices {lrs_snapshots}")
            if len(lrs_snapshots) > 1:
                lrs_snapshot_price = lrs_snapshots[0]
        # disk_type_name_str = "premiumv2"
        # size_class = "Premium LRS"
        cap_meter = "Premium LRS Provisioned Capacity"
        iops_meter = "Premium LRS Provisioned IOPS"
        bw_meter = "Premium LRS Provisioned Throughput (MBps)"
        capacity_prices = _cup(azure_pricing, {region_col_name: region_name, meter_col_name: cap_meter})
        if len(capacity_prices) == 1:
            capacity_price = float(capacity_prices[0]) * disk_size_val * hours_per_month
        else:
            print(f"multiple or no capacity prices {capacity_prices} for {region_name} - {cap_meter}")
            capacity_price = 0
        iops_prices = _cup(azure_pricing, {region_col_name: region_name, meter_col_name: iops_meter})
        iops_price = 0
        if len(iops_prices) == 1:
            if iops_val > 3000:
                #print(f" in iops {float(iops_prices[0]) } {(iops_val - 3000)} {hours_per_month}")
                iops_price = float(iops_prices[0]) * (iops_val - 3000) * hours_per_month
        else:
            print(f"multiple or no iops prices {iops_prices} for {region_name} - {iops_meter}")
        bw_prices = _cup(azure_pricing, {region_col_name: region_name, meter_col_name: bw_meter})
        bw_price = 0
        if len(bw_prices) == 1:
            if mbps_val > 125:
                bw_price = float(bw_prices[0]) * (mbps_val - 125) * hours_per_month
        else:
            print(f"multiple or no bw prices {bw_prices} for {region_name} - {bw_meter}")
        # 04/06 MR Option to ingore all snaphot costs if change rate is zero
        if float(monthly_snapshot_rate) == 0:
            snapshot_price = 0
        else:
            snapshot_price = float(efficiency) * disk_size_val * (float((monthly_snapshot_rate) / 2) + 1) * lrs_snapshot_price
        total_cost = iops_price + bw_price + capacity_price + snapshot_price
        #print(disk_type_name)
        #print(f"capacity prices {capacity_price} {capacity_prices} {disk_size_val}")
        #print(f"iops prices {iops_price} {iops_prices} {iops_val}")
        #print(f"bw prices {bw_price} {bw_prices} {mbps_val}")
        return pd.Series(
            {"total_cost": total_cost, "cap_cost": capacity_price, "iops_cost": iops_price, "mbps_cost": bw_price,
             "snap_cost": snapshot_price, "mode": mode, "paid_capacity": disk_size_val, iops: iops_val, mbps: mbps_val})
    elif "premium" in disk_type_name.lower():
        iops_val = 120
        mbps_val = 25

        lrs_snapshots = _cup(azure_pricing, {region_col_name: region_name,
                                             product_name: hdd_product_name,
                                             sku_col_name: lrs_snapshot_sku_name})
        lrs_snapshot_price = 0.018
        if len(lrs_snapshots) == 1:
            lrs_snapshot_price = lrs_snapshots[0]
        else:
            print(f"found multiple or no LRS snapshot prices {lrs_snapshots}")
            if len(lrs_snapshots) > 1:
                lrs_snapshot_price = lrs_snapshots[0]
        # print(f"snapshot prices {lrs_snapshot_price}")
        disk_type_name_str = "premium"
        size = disk_size_val
        paid_cap = 0
        if size <= 4:
            size_class = f"P1 {mode}"
            paid_cap = 4
        elif size <= 8:
            size_class = f"P2 {mode}"
            paid_cap = 8
        elif size <= 16:
            size_class = f"P3 {mode}"
            paid_cap = 16
        elif size <= 32:
            size_class = f"P4 {mode}"
            paid_cap = 32
        elif size <= 64:
            iops_val = 240
            mbps_val = 50
            size_class = f"P6 {mode}"
            paid_cap = 64
        elif size <= 128:
            iops_val = 500
            mbps_val = 100
            size_class = f"P10 {mode}"
            paid_cap = 128
        elif size <= 256:
            iops_val = 1100
            mbps_val = 125
            size_class = f"P15 {mode}"
            paid_cap = 256
        elif size <= 512:
            iops_val = 2300
            mbps_val = 150
            size_class = f"P20 {mode}"
            paid_cap = 512
        elif size <= 1024:
            iops_val = 5000
            mbps_val = 200
            size_class = f"P30 {mode}"
            paid_cap = 1024
        elif size <= 2048:
            iops_val = 7500
            mbps_val = 250
            size_class = f"P40 {mode}"
            paid_cap = 2048
        elif size <= 4096:
            iops_val = 7500
            mbps_val = 250
            size_class = f"P50 {mode}"
            paid_cap = 4096
        elif size <= 8192:
            iops_val = 16000
            mbps_val = 500
            size_class = f"P60 {mode}"
            paid_cap = 8192
        elif size <= 16384:
            iops_val = 18000
            mbps_val = 750
            size_class = f"P70 {mode}"
            paid_cap = 16384
        elif size <= 32768:
            iops_val = 20000
            mbps_val = 900
            size_class = f"P80 {mode}"
            paid_cap = 32768
        else:
            print(f"error in size value {size} premium")
            return pd.Series(
                {"total_cost": 0, "cap_cost": 0, "iops_cost": 0, "mbps_cost": 0, "snap_cost": 0, "mode": "remove",
                 "paid_capacity": 0, iops: iops_val, mbps: mbps_val})
        #    print(f"size {size_class}")
        capacity_prices = _cup(azure_pricing, {region_col_name: region_name, sku_col_name: size_class})
        if len(capacity_prices) == 1:
            capacity_price = capacity_prices[0]
        else:
            print(f"multiple or no capacity prices {capacity_prices} for {region_name} - {size_class}")
            capacity_price = 0
        iops_price = 0
        bw_price = 0
        #    print(f"efficiency {float(efficiency)} disk_size_val {disk_size_val} {float(monthly_snapshot_rate)}")
        # 04/06 MR Option to ingore all snaphot costs if change rate is zero
        if float(monthly_snapshot_rate) == 0:
            snapshot_price = 0
        else:
            snapshot_size = float(efficiency) * disk_size_val * ((float(monthly_snapshot_rate) / 2) + 1)
            #print(f"snapshot size {snapshot_size} disk size {disk_size_val}")
            snapshot_price = float(efficiency) * disk_size_val * (float(monthly_snapshot_rate / 2) + 1) * lrs_snapshot_price
        total_cost = iops_price + bw_price + capacity_price + snapshot_price
        #    print(disk_type_name)
        #    print(f"snapshot price {snapshot_price}")
        #    print(f"capacity prices {capacity_prices}")

        return pd.Series(
            {"total_cost": total_cost, "cap_cost": capacity_price, "iops_cost": iops_price, "mbps_cost": bw_price,
             "snap_cost": snapshot_price, "mode": mode, "paid_capacity": paid_cap, iops: iops_val, mbps: mbps_val})
    else:
        mode = "remove"
        return pd.Series({"total_cost": 0, "cap_cost": 0, "iops_cost": 0, "mbps_cost": 0, "snap_cost": 0, "mode": mode,
                          "paid_capacity": 0, iops: 0, mbps: 0})


def _azure_vnet_peering_rates(regions):
    """Look up intra-region VNet peering data-transfer rates ($/GB, both directions)
    from the Azure retail prices API.

    Consolidation only ever re-homes groups within the same region, so the peer that
    would be created is an *intra-region* VNet peering; Azure charges an ingress and an
    egress data-transfer rate on it. Azure publishes intra-region peering as a flat
    per-GB rate under productName "Virtual Network Peering" (meterName "Intra-Region
    Ingress"/"Intra-Region Egress"); the standard public-region entry is carried under
    armRegionName "Global" (individual regions do not each republish it). We fetch that
    once and apply it to every region, honouring a region-specific meter if one exists.
    Returns {region: {"ingress":.., "egress":.., "per_gb":.., "currency":.., "source":..}}
    and falls back to a typical default when the API is unreachable."""
    DEFAULT = 0.01  # $/GB each direction — typical intra-region VNet peering
    clean = sorted({str(r) for r in regions
                    if r is not None and str(r).strip() not in ("", "no_region", "—", "nan")})

    g_ingress = g_egress = None            # the "Global" (standard public region) rate
    currency = "USD"
    per_region = {}                        # region -> {"ingress":.., "egress":..}
    got_api = False
    try:
        next_url = ("https://prices.azure.com/api/retail/prices?$filter="
                    "serviceName%20eq%20%27Virtual%20Network%27%20and%20"
                    "productName%20eq%20%27Virtual%20Network%20Peering%27")
        for _page in range(5):
            resp = requests.get(next_url, timeout=20)
            if resp.status_code == 429:
                time.sleep(1); continue
            if resp.status_code != 200:
                break
            j = json.loads(resp.content)
            for it in j.get("Items", []):
                mn = str(it.get("meterName", "")).lower()
                if "intra-region" not in mn:
                    continue
                price = it.get("retailPrice")
                if price is None:
                    continue
                got_api = True
                currency = it.get("currencyCode", currency) or currency
                reg = str(it.get("armRegionName", ""))
                if reg in ("", "Global"):
                    if "ingress" in mn:
                        g_ingress = float(price) if g_ingress is None else min(g_ingress, float(price))
                    elif "egress" in mn:
                        g_egress = float(price) if g_egress is None else min(g_egress, float(price))
                else:
                    d = per_region.setdefault(reg, {})
                    if "ingress" in mn:
                        d["ingress"] = float(price)
                    elif "egress" in mn:
                        d["egress"] = float(price)
            next_url = j.get("NextPageLink")
            if not next_url:
                break
    except Exception as exc:
        print(f"Warning: VNet peering price lookup failed: {exc}")

    if g_ingress is None:
        g_ingress = DEFAULT
    if g_egress is None:
        g_egress = DEFAULT

    out = {}
    for region_name in clean:
        ov = per_region.get(region_name)
        ri = ov.get("ingress", g_ingress) if ov else g_ingress
        re_ = ov.get("egress", g_egress) if ov else g_egress
        out[region_name] = {
            "ingress": round(float(ri), 6),
            "egress": round(float(re_), 6),
            "per_gb": round(float(ri) + float(re_), 6),
            "currency": currency,
            "source": "azure_api" if (got_api or ov) else "default",
        }
    return out


def gen_price_list_azure(regions, customer, directory):

    file_name_amd_cost = "azure_managed_disk_costs.csv"
    output_bucket_prefix = session["results_prefix"]
    cached_key = f"{output_bucket_prefix}{file_name_amd_cost}"
    cached_df = _load_df_from_s3(cached_key)
    if cached_df is not None:
        # print(f"Using cached azure_managed_disk_costs.csv from S3.")
        return cached_df
    else:
        # print("amd cost file not found - generating")
        #print(regions, customer, directory, "in gen_price_list_azure")
        bad_words = [
            "Reservation",
            "Transactions",
            "Confidential",
            "Burst",
            "Mount",
            "Operations"
        ]
        # regions = ["eastus2", "eastus", "westus"]
        storage_lists = []
        price_list = []
        service = "Storage"

        product_list = [
            "Azure Premium SSD v2",
            "Standard SSD Managed Disks",
            "Premium SSD Managed Disks",
            "Standard HDD Managed Disks",
            "Ultra Disks"
        ]
        #print("saidy",regions, product_list)
        # Each (region, product) is an independent Azure retail-price query — fan them
        # out across a thread pool (network I/O, so the GIL is released during each
        # request). Results are collected in the main thread, preserving behaviour.
        def _fetch_region_product(rp):
            region_name, product = rp
            product_enc = urllib.parse.quote(product)
            url_2 = f"https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27{service}%27%20and%20armRegionName%20eq%20%27{region_name}%27%20and%20productName%20eq%20%27{product_enc}%27"
            for _try in range(30):
                response = requests.get(url_2, timeout=30)
                if response.status_code == 200:
                    break
                if response.status_code == 429:
                    time.sleep(1); continue
                raise RuntimeError(f"api error {response.status_code}")
            rows = []
            for st in json.loads(response.content).get("Items", []):
                rt_price = st.get("retailPrice", None)
                ut_price = st.get("unitPrice", None)
                sku_name = st.get("skuName", None)
                type_c = st.get("type", None)
                if rt_price and ut_price and sku_name and (not type_c == 'Reservation'):
                    rows.append({
                        "productName": product,
                        "skuName": sku_name,
                        "meterName": st.get("meterName", None),
                        "retailPrice": rt_price,
                        "unitPrice": ut_price,
                        "unitOfMeasure": st.get("unitOfMeasure", None),
                        "region": region_name,
                    })
            return rows

        tasks = [(region_name, product) for region_name in regions for product in product_list]
        with ThreadPoolExecutor(max_workers=AZURE_PRICE_WORKERS) as _ex:
            for rows in _ex.map(_fetch_region_product, tasks):
                price_list.extend(rows)
        df_price = pd.DataFrame(price_list)
        pattern = '|'.join(bad_words)
        # print("ron", pattern)
        try:
            mask = df_price["skuName"].str.contains(pattern, case=False, na=False, regex=True)
        except:
            #print("wren", price_list)
            raise "vallhalla"

        # 3. Filter the DataFrame using the mask
        filtered_df = df_price[~mask]
        # df_filtered = df_csv[df_csv[compute].isin(vm_inclusion_list)]
        del df_price
        df_price = filtered_df
        mask = df_price["meterName"].str.contains(pattern, case=False, na=False, regex=True)

        # 3. Filter the DataFrame using the mask
        filtered_df = df_price[~mask]
        # df_filtered = df_csv[df_csv[compute].isin(vm_inclusion_list)]
        del df_price
        df_price = filtered_df
        meter_list = df_price["meterName"].unique()
        # print(f"num items {len(meter_list)} and items {meter_list}")

        file_name_amd_cost = "azure_managed_disk_costs.csv"
        output_bucket_prefix = session["results_prefix"]
        object_prefix = f"{output_bucket_prefix}"
        # Use the put_object API to upload the CSV data
        status_up = upload_df_to_s3(df_price, file_name_amd_cost, object_prefix)
        # print(f"ec_infra_resource_file upload status {status_up}")

        return df_price


def parse_network_names(row):
    global parse_network_name_list
    for name in parse_network_name_list:
        #print(name, row[other2_column_name])
        if name.lower() in row[other2_column_name].lower():
            return name
    return row[other2_column_name]


def add_in_region_replication(row):
    global in_region_mapping
    global zone
    global region
    global default_zone_id
    for mapped_item in in_region_mapping:
        # print("add_in",mapped_item)
        map_reg = mapped_item.get("region", None)
        map_zone_source = mapped_item.get("source", None)
        map_zone_target = mapped_item.get("target", None)
        # print("add_in 2", map_reg, map_zone_source, map_zone_target)
        if (map_reg == "all" or row[zone] == map_zone_source) and (map_zone_source == "any" or row[region] == map_reg):
            # print("add_in 3")
            if map_zone_target == "any":
                # print("add_in 4")
                return pd.Series({"rep_region": row[region], "rep_zone": str(default_zone_id)})
            else:
                # print("add_in 5")
                return pd.Series({"rep_region": row[region], "rep_zone": str(map_zone_target)})
    return pd.Series({"rep_region": None, "rep_zone": None})


def calc_true_az(row):
    global az_mapping
    global zone
    global a_name
    if az_mapping.get(row[a_name], {}).get(row[zone], None) is not None:
        # print(az_mapping[row[a_name]][row[zone]])
        return pd.Series({"true_az": az_mapping[row[a_name]][row[zone]], "a_name": az_mapping[row[a_name]]["new_name"]})
        # return az_mapping[row[a_name]][row[zone]], az_mapping[row[a_name]]["new_name"]
    else:
        print(f"did not find {row[a_name]} {row[zone]} in mapping")
        return pd.Series({"true_az": row[zone], "a_name": row[a_name]})
        # return row[zone],row[a_name]



def convert_disk_usage_to_root_flag(row):
    global disk_usaage_string
    global disk_usage
    global os_disk_device_list
    # print(f"convert {disk_usage}")
    # print(f"convert {row[disk_usage]}  {os_disk_device_list}")
    if disk_usage_string:
        # print("t1")
        if row[disk_usage] == disk_usage_string:
            # print("t2")
            return False
        else:
            # print("t3")
            # print("disk_uagage check1", row[disk_usage], row[disk_usage])
            return True
    elif row[disk_usage]:
        # print("t5")
        if not row[disk_usage] in os_disk_device_list:
            # print("t6")
            return False
    else:
        # print("disk_uagage check2", row[disk_usage], row[disk_usage])
        return True

def main2(event,df_all):
    #use_price_file_flag = event.get("use_price_file_flag",0)
    #ec_price_file = event.get("ec_price_file")
    #azure_price_file = event.get("azure_price_file")
    region_column_value = _cfg_int(event, "region")
    zone_column_value = _cfg_int(event, "zone")
    subscription_column_value = _cfg_int(event, "subscription_or_account_id")
    vnet_column_value = _cfg_int(event, "vnet_or_vpc")
    disk_type_column_value = _cfg_int(event, "disk_type")
    disk_size_column_value = _cfg_int(event, "disk_size")
    mbps_column_value = _cfg_int(event, "mbps")
    iops_column_value = _cfg_int(event, "iops")
    disk_status_column_value = _cfg_int(event, "disk_status")
    host_type_column_value = _cfg_int(event, "host_type")
    root_flag_column_value = _cfg_int(event, "root_flag")
    count_compute_column_value = _cfg_int(event, "count_compute")
    disk_usage_column_value = _cfg_int(event, "disk_usage")
    ignore_iops_provisioned_flag = event.get("ignore_iops_provisioned",False)
    # 04/06/26 MR added flag to skip BW affect
    ignore_bw_provisioned_flag = event.get("ignore_bw_provisioned", False)

    # 04/13 MR - adding check for max number of volumes
    max_volumes_per_array_val = event.get("max_volumes_per_array",-99)
    if max_volumes_per_array_val > 0:
        max_volumes_per_array_flag = True
        max_volumes_per_array = int(max_volumes_per_array_val)
    else:
        max_volumes_per_array_flag = False

    customer = session.get("active_customer", "")
    output_bucket_prefix = session["results_prefix"]
    #source_bucket_prefix = event.get('s3_source_prefix',fr"{customer}/data/")
    list_of_regions = []
    supported_region_list = event.get("region_list",[])
    if len(supported_region_list) > 0:
        supported_region_list_flag = True
        list_of_regions = []
        tmp_list_of_regions = list(supported_region_list)
        for t in tmp_list_of_regions:
            if supported_region_list[t] >= 200:
                list_of_regions.append(t)
    else:
        supported_region_list_flag = False
    # print(list_of_regions)
    per_vm_v10_bw_limit = event.get("per_vm_v20_bw_limit",1024)
    per_vm_v20_bw_limit = event.get("per_vm_v20_bw_limit", 1800)
    ec_config = event.get("ec_config",{
        "V10MP2R2":
            {
                "pscSku": "V10MP2R2",
                "VM": "Standard_E16bds_v5",
                "NumControllers": 2,
                "NvramIOPS": 5000,
                "NvramBW": 150,
                "VdIOPS": 4211,
                "VdBW": 125,
                "vd_count": 14,
                "nvram_num": 2,
                "nvram_size": 64,
                "disk_sizes": [256, 512, 1024, 2048, 3072],
                "size_down": 0,
                "iops_over_provision_rate": 2.5,
                "usable_limit_iops": 89000,
                "index": 100,
                "raw_cap_matrix": {
                    "2304": 256,
                    "4290": 512,
                    "9205": 1024,
                    "19036": 2048,
                    "28866": 3072,
                    "39137": 4096,
                    "50196": 5120,
                    "61255": 6144,
                    "72314": 7168
                }
            },
        "V20MP2R2":
            {
                "pscSku": "V20MP2R2",
                "VM": "Standard_E32bds_v5",
                "NumControllers": 2,
                "NvramIOPS": 15000,
                "NvramBW": 450,
                "VdIOPS": 8422,
                "VdBW": 178,
                "vd_count": 14,
                "nvram_num": 2,
                "nvram_size": 64,
                "disk_sizes": [256, 512, 1024, 2048, 3072, 4096, 5120, 6144, 7168, 8192, 9216],
                "size_down": 0,
                "iops_over_provision_rate": 2.5,
                "usable_limit_iops": 175000,
                "index": 200,
                "raw_cap_matrix": {
                    "4290": 512,
                    "9205": 1024,
                    "19036": 2048,
                    "28866": 3072,
                    "39137": 4096,
                    "50196": 5120,
                    "61255": 6144,
                    "72314": 7168,
                    "83374":8192,
                    "94433":9216,
                    "105492":10240,
                    "116551":11264,
                    "127610":12288,
                    "138670":13312,
                    "149729":14336,
                    "160788":15360,
                    "171847":16384,
                    "182906":17408,
                    "193966":18432,
                    "205025":19456,
                    "216084":20480,
                    "227133":21504,
                    "238202":22528
                }
            },
        "V50MP2R2":
            {
                "pscSku": "V50MP2R2",
                "VM": "Standard_D128ds_v6",
                "NumControllers": 2,
                "NvramIOPS": 32000,
                "NvramBW": 1200,
                "VdIOPS": 25398,
                "VdBW": 992,
                "vd_count": 14,
                "nvram_num": 2,
                "nvram_size": 64,
                "disk_sizes": [256, 512, 1024, 2048, 3072, 4096, 5120, 6144, 7168, 8192, 9216, 10240, 11264, 12288],
                "size_down": 0,
                "iops_over_provision_rate": 2.5,
                "usable_limit_iops": 270000,
                "index": 500,
                "raw_cap_matrix": {
                    "4290": 512,
                    "9205": 1024,
                    "19036": 2048,
                    "28866": 3072,
                    "39137": 4096,
                    "50196": 5120,
                    "61255": 6144,
                    "72314": 7168,
                    "83374":8192,
                    "94433":9216,
                    "105492":10240,
                    "116551":11264,
                    "127610":12288,
                    "138670":13312,
                    "149729":14336,
                    "160788":15360,
                    "171847":16384,
                    "182906":17408,
                    "193966":18432,
                    "205025":19456,
                    "216084":20480,
                    "227133":21504,
                    "238202":22528
                }
            }
    })
    initial_iops_full_rate = event.get("initial_iops_full_rate", 0.9)
    initial_cap_rate = event.get("initial_cap_rate", 0.80)
    ec_license_cost_per_gib = float(event.get("ec_license_cost_per_gib", "0.06"))
    directory = event.get("directory")

    customer = event.get("name")
    data_reduction_ratio = float(event.get("drr",4))
    win_threshold = _cfg_int(event, "win_threshold", 0)
    lin_threshold = _cfg_int(event, "lin_threshold", 0)

    use_vm_inclusion_list = event.get("use_vm_inclusion_list", 0)
    mid_tier_ratio_limit_V10 = event.get("mid_tier_ratio_limit_V10", 0.75)
    mid_tier_capacity_limit_V10_GiB = event.get("mid_tier_capacity_limit_V10_GiB", 51050)
    top_tier_ratio_limit_V20 = event.get("top_tier_ratio_limit_V20", 0.75)
    top_tier_capacity_limit_V20_GiB = event.get("top_tier_capacity_limit_V20_GiB", 102400)
    ec_sku_bias = event.get("ec_sku_bias", "none")
    if use_vm_inclusion_list:
        vm_inclusion_list = event.get("vm_inclusion_list", [])
    global efficiency
    efficiency = float(event.get("efficiency",0.66))
    growth = float(event.get("growth",0.2))
    all_flag = event.get("price_all_data_flag")
    count_compute_flag = False if event.get("count_compute", -99) == -99 else True
    vm_inclusion_list = []
    if use_vm_inclusion_list and count_compute_flag:
        vm_inclusion_list = event.get("vm_inclusion_list", [])
    else:
        use_vm_inclusion_list = False
    # print(f"vm list {vm_inclusion_list} {use_vm_inclusion_list}")
    # print(count_compute_flag)
    # print(f"all flag {all_flag}")
    cloud = event.get("cloud")
    aws_pricing_file = event.get("aws_pricing_file", -99)
    az_mapping_file = event.get("az_mapping_file", -99)
    # print(az_mapping_file)
    global monthly_snapshot_rate
    monthly_snapshot_rate = float(event.get("monthly_snapshot_rate", 0.1))
    subscription_or_account_id = subscription_column_value
    #vnet_or_vpc = vnet_column_value
    use_file_name_for_subscription_or_account_flag = event.get("use_file_name_for_subscription_or_account_flag")
    # print(f"account flag {use_file_name_for_subscription_or_account_flag}")
    reserve = event.get("reserve", "Reserved 3 Years")
    # non_os_disk_usage_string = event.get("non_os_disk_usage_string",0)
    # bumps = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768]

    # Determine if you are going to parse VNET/VPC names for a pattern
    parse_network_name_flag = False
    global parse_network_name_list
    parse_network_name_list = event.get("parse_network_name_strings", [])
    parse_network_name_column = event.get("parsed_network_name", -99)
    skip_non_parsed_network_names = event.get("skip_non_parsed_network_names", False)
    if len(parse_network_name_list) > 0 and parse_network_name_column != -99:
        parse_network_name_flag = True
        # print("parse", parse_network_name_column, parse_network_name_list, skip_non_parsed_network_names,
              # parse_network_name_flag)
    else:
        pass
        # print("parse", parse_network_name_column, parse_network_name_flag)

    # Determine if we are using a subscription or account id in the processed data
    if subscription_or_account_id == -99 and not use_file_name_for_subscription_or_account_flag:
        use_file_name_flag = False
        use_other_column_flag = False
        # print("option 1")
    elif subscription_or_account_id == -99:  # The use file name flag is set
        use_file_name_flag = True
        use_other_column_flag = False
        # print("option 2")
    else:  # THe use column name
        use_file_name_flag = False
        use_other_column_flag = True
        # print("option 3")

    global df_aws_pricing


    if az_mapping_file != -99:
        az_mapping_flag = True
        out_directory = fr"{directory}\results2"

        os.chdir(out_directory)
        # print(az_mapping_file)
        global az_mapping
        try:
            with open(az_mapping_file, "r") as file:
                az_mapping = json.load(file)
            # print("found mapping file")
        except:
            print(os.listdir('.'))
            print("did not find mapping file")
            az_mapping_flag = False
        with open(az_mapping_file, "r") as file:
            az_mapping = json.load(file)
        # print("found mapping file")

    else:
        az_mapping_flag = False

    out_directory = fr"{directory}\results2"
    try:
        os.mkdir(out_directory)
    except:
        pass

    #os.chdir(directory)
    #csv_files = []
    first = True
    global region
    region = None
    global zone
    zone = None
    # other = col_names[event.get("other")]
    global disk_type
    disk_type = None
    global disk_size
    disk_size = None
    global mbps
    mbps = None
    global iops
    iops = None
    disk_status = None
    host_type = None
    root_flag = None
    global disk_usage
    disk_usage = None
    global disk_usage_string
    global other
    other = None
    global other2_column_name
    global os_disk_device_list
    other2_column_name = None
    disk_usage_string = event.get("non_os_disk_usage_string", 0)
    os_disk_device_list = event.get("os_disk_device_list", [])
    # print(f"weivel {disk_usage_string} {os_disk_device_list}")
    no_device_flag = True   # default: no OS-disk device list provided
    if disk_usage_string:
        no_device_flag = True
        if len(os_disk_device_list) > 0:
            no_device_flag = False
    compute = None
    global a_name
    a_name = "AccountName"
    global in_region_mapping
    global cross_region_mapping
    global default_zone_id
    default_zone_id = event.get("default_zone_id", 1)

    # replication mapping
    in_region_mapping_flag = event.get("in_region_mapping_flag", 0)
    # print("rep flag", in_region_mapping_flag)
    if in_region_mapping_flag:
        in_region_mapping_file = event.get("in_region_mapping_file", None)
        # print("rep flag", in_region_mapping_file)
        with open(in_region_mapping_file, 'r') as file:
            in_region_mapping = json.load(file).get("mapping", [])
            # print(in_region_mapping)

    cross_region_mapping_flag = event.get("cross_region_mapping_flag", 0)
    if cross_region_mapping_flag:
        cross_region_mapping_file = event.get("cross_region_mapping_file", None)
        with open(cross_region_mapping_file, 'r') as file:
            cross_region_mapping = json.load(file).get("mapping", [])

    # Check Inputs
    bad_data = False
    if disk_type_column_value == -99:
        bad_data = True
        # print("hi 01")
        raise "bad data"
    if disk_size_column_value == -99:
        bad_data = True
        # print("hi 02")
        raise "bad data"
    no_region_flag = False
    if region_column_value == -99:
        no_region_flag = True
    no_zone_flag = False
    if zone_column_value == -99:
        no_zone_flag = True
    no_other_flag = False
    if subscription_column_value == -99:
        no_other_flag = True
    no_other2_flag = False
    if not parse_network_name_flag:
        if vnet_column_value == -99:
            no_other2_flag = True
            # print("no other2 name flag",no_other2_flag)
    no_mbps_flag = False
    if mbps_column_value == -99:
        no_mbps_flag = True
    no_iops_flag = False
    if iops_column_value == -99:
        no_iops_flag = True
    no_disk_status_flag = False
    if disk_status_column_value == -99:
        no_disk_status_flag = True
        default_disk_status = "in-use"
        valid_disk_status = [default_disk_status]
        # print(f"disk status types 0 {default_disk_status, valid_disk_status}")
        #print(event.get("valid_disk_status", []))
    else:
        if len(event.get("valid_disk_status", [])) < 1:
            no_disk_status_flag = True
            default_disk_status = "in-use"
            valid_disk_status = [default_disk_status]
            # print(f"disk status types 1 {default_disk_status, valid_disk_status}")
        else:
            valid_disk_status = [item.lower() for item in event.get("valid_disk_status")]
            default_disk_status = valid_disk_status[0]
            # print(f"disk status types 2 {default_disk_status, valid_disk_status}")

    fixed_zone_flag = False
    if zone_column_value == -99:
        fixed_zone_count = event.get("fixed_zone_count", 0)
        fixed_zone_list = event.get("fixed_zone_list", [])
        if fixed_zone_count == len(fixed_zone_list):
            fixed_zone_flag = True
    if fixed_zone_flag:
        pass
        # print("fixed zone flag was set")
    no_host_type_flag = False
    if host_type_column_value == -99:
        no_host_type_flag = True
    no_root_flag_flag = False
    if root_flag_column_value == -99:
        no_root_flag_flag = True
    no_disk_usage_flag = False
    if disk_usage_column_value == -99:
        no_disk_usage_flag = True

    df_csv = None

    #found_file = False
    #for filename in os.listdir('.'):  # '.' represents the current directory
    #    if (filename.endswith('.csv') or filename.endswith('.CSV')) and os.path.isfile(filename):
    #        found_file = True
    #        name, extension = os.path.splitext(filename)
    #        print(name)
    #        csv_files.append(filename)
    #        raw_data = filename
    #
    #        if first:
    # print("made it to first parse")

    df_csv = df_all
    #if use_file_name_flag:
    #    df_csv[a_name] = name
    #    # print("option 4")
    #else:
    if True:
        df_csv[a_name] = "all"
        # print("option 5")
    # print(df_csv.shape)
    col_names = df_csv.columns.tolist()
    # Validate every mapped column index is within range for THIS file. Reusing a
    # config from a file with more columns otherwise fails later with a cryptic
    # "list index out of range". -99 means "not mapped" and is always allowed.
    _mapped_cols = {
        "region": region_column_value, "zone": zone_column_value,
        "subscription_or_account_id": subscription_column_value,
        "vnet_or_vpc": vnet_column_value, "disk_type": disk_type_column_value,
        "disk_size": disk_size_column_value, "mbps": mbps_column_value,
        "iops": iops_column_value, "disk_status": disk_status_column_value,
        "host_type": host_type_column_value, "root_flag": root_flag_column_value,
        "count_compute": count_compute_column_value, "disk_usage": disk_usage_column_value,
    }
    _bad_cols = {f: v for f, v in _mapped_cols.items() if v != -99 and not (0 <= v < len(col_names))}
    if _bad_cols:
        detail = "; ".join(f"'{f}' -> index {v}" for f, v in sorted(_bad_cols.items()))
        raise ValueError(
            f"Column mapping error: {detail}. This file has {len(col_names)} column(s) "
            f"(valid indices 0-{len(col_names) - 1}). Update the JSON Data column mapping to match this file.")
    # print(col_names)
    # print(type(col_names), col_names)
    # df_csv.columns)
    # map_column_name
    second_list = []
    if no_region_flag:
        region = "no_region"
        if cloud == "aws":
            df_csv[region] = "US East(N. Virginia)"
        else:  # cloud is azure
            df_csv[region] = "eastus"
    else:
        # print(col_names, region_column_value)
        region = col_names[region_column_value]
    if no_zone_flag:
        zone = "no_zone"
        df_csv[zone] = default_zone_id
    else:
        zone = col_names[zone_column_value]

    if no_other_flag:
        other = "no_other"
        df_csv[other] = "all"
        # print("oops oh")
    else:
        other = col_names[subscription_column_value]
        # print(f"found other {other}")
    if no_other2_flag:
        other2_column_name = "no_other2"
        df_csv[other2_column_name] = "all"
        # print("other2",other2_column_name)
    else:
        if parse_network_name_flag:
            other2_column_name = col_names[parse_network_name_column]
            # print("other2 column name",other2_column_name)
        else:
            other2_column_name = col_names[vnet_column_value]
    disk_type = col_names[disk_type_column_value]
    # print("disk_type_value", disk_type, disk_type_column_value)
    disk_size = col_names[disk_size_column_value]
    # Guard against a wrong column mapping: disk_size MUST point at a numeric
    # (capacity) column. Mapping it to a text column (e.g. "OS Type"/"SKU") would
    # otherwise fail deep in cost calc with a cryptic "cannot convert NaN to
    # integer". Coerce here and raise a clear, actionable error instead.
    _ds_numeric = pd.to_numeric(df_csv[disk_size], errors="coerce")
    if int(_ds_numeric.notna().sum()) == 0:
        raise ValueError(
            f"Column mapping error: 'disk_size' is mapped to column '{disk_size}' "
            f"(index {disk_size_column_value}), which contains no numeric values. "
            f"Map 'disk_size' to the disk capacity column in the JSON Data configuration.")
    # A legitimate capacity column may still have blank cells — treat those as 0 so
    # per-row conversions downstream don't crash.
    df_csv[disk_size] = _ds_numeric.fillna(0)
    if no_mbps_flag:
        mbps = "no_mbps"
        df_csv[mbps] = 0
        # print("mbps1", mbps)
    else:
        mbps = col_names[mbps_column_value]
        # print("mbps2",mbps)
    if no_iops_flag:
        iops = "no_iops"
        df_csv[iops] = 0
    else:
        iops = col_names[iops_column_value]

    if no_disk_status_flag:
        disk_status = "no_status"
        df_csv[disk_status] = default_disk_status
        # print("no disk status so set column")
    else:
        disk_status = col_names[disk_status_column_value]
    # print(f"hope alive {default_disk_status}")
    if no_disk_usage_flag:
        # print("abo hope1")
        disk_usage = "no_usage"
        df_csv[disk_usage] = "device_name"
    else:
        disk_usage = col_names[disk_usage_column_value]
    # print("hope alive2")
    if no_host_type_flag:
        host_type = "no_host_type"
        df_csv[host_type] = "not_used"
    else:
        host_type = col_names[host_type_column_value]
    # print("hope alive3")
    if no_root_flag_flag:
        root_flag = "no_root_flag"
    else:
        root_flag = col_names[root_flag_column_value]
    # print("hope alive4")
    if count_compute_flag:
        compute = col_names[count_compute_column_value]
    else:
        compute = "no_vms_given"

    # print("Baby needs some new shews")
    df_csv[mbps] = df_csv[mbps].fillna(125)
    rows_index_with_bad_data = df_csv[df_csv[disk_size].isnull() | df_csv[disk_type].isnull()].index
    # print(df_csv.shape)
    # print(f"bad rows: {len(rows_index_with_bad_data)}")
    df_csv.drop(rows_index_with_bad_data, inplace=True)
    # print(df_csv.shape)
    df_csv[disk_usage] = df_csv[disk_usage].fillna(disk_usage_string)
    if use_vm_inclusion_list:
        # print("processing VM inclusion list")

        # 1. Create the regex pattern

        pattern = '|'.join(vm_inclusion_list)
        mask = df_csv[compute].str.contains(pattern, case=False, na=False, regex=True)

        # 3. Filter the DataFrame using the mask
        filtered_df = df_csv[mask]
        # df_filtered = df_csv[df_csv[compute].isin(vm_inclusion_list)]
        del df_csv
        df_csv = filtered_df
        vm_list = df_csv[compute].unique()
        # print(f"num VMs {len(vm_list)} and VMs {vm_list}")
    else:
        pass
        # print("not doing VM includsion list")

    if in_region_mapping_flag:
        df_csv["rep_region"] = None
        df_csv["rep_zone"] = None
        df_csv[["rep_region", "rep_zone"]] = df_csv.apply(add_in_region_replication, axis=1)
        # file_name2 = fr"{customer}_tmp_data.csv"
        # os.chdir(out_directory)
        # df_csv.to_csv(file_name2, encoding='utf-8', index=False)
        # return
        # disk_capacity_used = df_csv.loc[
        #    (df_csv["rep_region"] == "eastus2"), disk_size].sum()
        # disk_zone = df_csv.loc[
        #    (df_csv["rep_region"] == "eastus2"), "rep_zone"].unique()
        # disk_capacity_used2 = df_csv.loc[
        #    (df_csv["rep_zone"] == "1"), disk_size].sum()
        # disk_zone2 = df_csv.loc[
        #    (df_csv["rep_zone"] == "1"), "rep_zone"].unique()

        # print("zz",disk_capacity_used,disk_zone,disk_capacity_used2,disk_zone2)
        # return

    if not no_other2_flag:
        df_csv[other2_column_name] = df_csv[other2_column_name].fillna("blank")

    # If we are parsing network names, set the parsed values
    if parse_network_name_flag:
        df_csv[other2_column_name] = df_csv.apply(parse_network_names, axis=1)

    if not no_other_flag:
        df_csv[other] = df_csv[other].fillna("blank")
    if not count_compute_flag:
        df_csv[compute] = "not_given"
        df_csv["vm_perf_tier"] = 0
        if not "min_ec_model" in df_csv.columns:
            df_csv["min_ec_model"] = "V10MP2R2"
    # print(df_csv.shape)
    # df_csv[disk_type] = df_csv[disk_type].fillna("bad")
    # print(df_csv.shape)
    df_csv[iops] = pd.to_numeric(df_csv[iops], errors='coerce')
    # print(df_csv.shape)
    df_csv[mbps] = pd.to_numeric(df_csv[mbps], errors='coerce')
    # print(df_csv.shape)
    df_csv[disk_size] = pd.to_numeric(df_csv[disk_size], errors='coerce')
    # print(df_csv.shape)
    # print(df_csv.info())
    region_list_pricing = df_csv[region].unique()
    azure_pricing = gen_price_list_azure(region_list_pricing, customer, directory)
    df_ec_pricing = get_ec_size_n_cost_data(region_list_pricing, customer, directory, ec_config)
    ec_mods = list(ec_config)
    for mod in ec_mods:
        group_ec_capacity_matrix_str = ec_config[mod]["raw_cap_matrix"].keys()
        group_ec_capacity_matrix_list = [int(x) for x in group_ec_capacity_matrix_str]
        ec_config[mod]["usable_limit_capacity"] = max(group_ec_capacity_matrix_list)
    min_ec_model = ec_config[ec_mods[0]]["pscSku"]
    # print(f"min sku model {min_ec_model}")
    if cloud == "azure":
        df_csv["total_cost"] = 0
        df_csv["cap_cost"] = 0
        df_csv["iops_cost"] = 0
        df_csv["mbps_cost"] = 0
        df_csv["snap_cost"] = 0
        df_csv["paid_capacity"] = 0
        df_csv["vm_perf_tier"] = 0
        if not "min_ec_model" in df_csv.columns:
            df_csv["min_ec_model"] = min_ec_model
        df_csv["mode"] = "LRS"
        df_csv["group_id"] = 0
        _price_lookup_cache.clear()   # fresh cache per parse (pricing is refetched each run)
        df_csv[["total_cost",
                "cap_cost",
                "iops_cost",
                "mbps_cost",
                "snap_cost",
                "mode",
                "paid_capacity",
                iops,
                mbps]] = df_csv.apply(calc_true_cost_azure, axis=1, azure_pricing=azure_pricing, )

    if az_mapping_flag:
        df_csv[["true_az", a_name]] = df_csv.apply(calc_true_az, axis=1)

    # print(df_csv.shape)
    # print(df_csv.info())
    # print(f"disk usage {no_disk_usage_flag} {no_root_flag_flag} {no_device_flag}")
    if no_disk_usage_flag and no_root_flag_flag and no_device_flag:  # Neither field is set
        df_csv[root_flag] = False
        # print("tree 01")
    elif no_disk_usage_flag and no_device_flag:  # Only Root flag is set
        df_csv[root_flag] = df_csv[root_flag].fillna(False)  # no other action required
        # print("tree 02")
    elif no_root_flag_flag:  # Only disk usage flag or device flag
        df_csv[root_flag] = df_csv.apply(convert_disk_usage_to_root_flag, axis=1)
        # print("tree 03")
    # true_count = len(df_csv.loc[(df_csv[root_flag] == True)])
    # ("true count",true_count)
    # print(df_csv.shape)
    if cloud == "azure":
        # print(f"made it to auzre")
        # df_csv['lost_capacity'] = df_csv.apply(calc_lost_capacity, axis=1)
        # df_csv['paid_capacity'] = df_csv.apply(calc_paid_capacity, axis=1)
        # num_paid = len(df_csv[(df_csv['paid_capacity'] > 0)])
        # print(f"num_paid = {num_paid}")
        # df_csv['iops_metered'] = df_csv.apply(calc_overage_iops, axis=1)
        # df_csv['mbps_metered'] = df_csv.apply(calc_overage_mbps, axis=1)
        # df_csv['HA'] = "No"
        df_csv[zone] = df_csv[zone].fillna(int(0))
        df_csv[disk_size] = df_csv[disk_size].fillna(0)
    df_csv[zone] = df_csv[zone].astype(str)

    # print(f"region {region} zone {zone} other {other} disk_type {disk_type} disk_size {disk_size} mbps {mbps} iops {iops} disk_status {disk_status}")

    # print(df_csv.shape)
    group_id = 0
    group_name = {}
    if use_file_name_flag:
        pass
        # print("no change in account info")
        # account_name_list = df_csv[a_name].unique()
    elif use_other_column_flag:
        # print(now)
        df_csv[a_name] = df_csv[other]
        # account_name_list = df_csv[other].unique()
    else:
        df_csv[a_name] = "blank"
        # account_name_list = ["none"]
    account_name_list = df_csv[a_name].unique()
    # print(f"account list {account_name_list}")
    compute_count = 0
    # Precompute every VM's usage aggregates in a SINGLE pass. Previously the group
    # loops ran a full df.apply(calc_compute_usage) once per VM (O(VMs x rows) — the
    # dominant cost for large inventories); now the loop just reads vm_data_all[cp].
    df_csv[compute] = df_csv[compute].fillna("not_given")
    vm_data_all = {}
    if count_compute_flag:
        df_csv.apply(calc_compute_usage_all, axis=1, vm_data=vm_data_all,
                     compute_column_name=compute, iops_column_name=iops, bw_column_name=mbps,
                     disk_size_column_name=disk_size, disk_type_column_name=disk_type,
                     min_sku_value=min_ec_model)
    for an in account_name_list:
        # print(f"name {an}")
        if supported_region_list_flag:
            region_lists = df_csv.loc[
                (df_csv[a_name] == an) &
                (df_csv[region].isin(list_of_regions)), region].unique()
        else:
            region_lists = df_csv.loc[(df_csv[a_name] == an), region].unique()
        # print(region_lists)
        # print(len(region_lists))
        for reg in region_lists:
            # print(f'region: {reg}')
            # If only looking at specific values in specified networks
            if skip_non_parsed_network_names:
                network_list = parse_network_name_list
            else:
                network_list = df_csv.loc[
                    (df_csv[a_name] == an) &
                    (df_csv[region] == reg), other2_column_name].unique()
            # print(f" networks {network_list}")
            for nt in network_list:
                # print(f'region: {reg}')
                if az_mapping_flag:
                    zone = "true_az"
                az_list = df_csv.loc[
                    (df_csv[a_name] == an) &
                    (df_csv[other2_column_name] == nt) &
                    (df_csv[region] == reg), zone].unique()
                # print(az_list)

                for az in az_list:
                    group_id = group_id + 1
                    # print(f"group id incremented {group_id}")
                    group_name[str(group_id)] = {'customer': customer, 'region': reg, 'zone': az, 'name': an, 'network': nt,
                                                 "true_cost_total": 0, "replication": 0}
                    if fixed_zone_flag:
                        group_name[str(group_id)] = {'customer': customer, 'region': reg, 'zone': fixed_zone_list[0],
                                                     'name': an,
                                                     'network': nt, "true_cost_total": 0, "replication": 0}
                        fixed_zone_groups = [str(group_id)]
                        if len(fixed_zone_list) > 1:
                            group_id = group_id + 1
                            group_name[str(group_id)] = {'customer': customer, 'region': reg, 'zone': fixed_zone_list[1],
                                                         'name': an,
                                                         'network': nt, "true_cost_total": 0, "replication": 0}
                            fixed_zone_groups.append(str(group_id))
                            if len(fixed_zone_list) > 2:
                                group_id = group_id + 1
                                group_name[str(group_id)] = {'customer': customer, 'region': reg, 'zone': fixed_zone_list[2],
                                                             'name': an,
                                                             'network': nt, "true_cost_total": 0, "replication": 0}
                                fixed_zone_groups.append(str(group_id))

                    disk_type_list = df_csv.loc[
                        (df_csv[region] == reg) &
                        (df_csv[a_name] == an) &
                        (df_csv[other2_column_name] == nt) &
                        (df_csv[zone] == az), disk_type].unique()
                    # print(disk_type_list)
                    for dt in disk_type_list:

                        if cloud == "aws":
                            pass
                            # print("not processing aws data")
                        # Data Frame Query Azure unique
                        elif cloud == "azure":

                            # Identify rows where the value is NOT a string
                            non_strings = df_csv[df_csv[disk_status].apply(lambda x: not isinstance(x, str))]

                            # See the values
                            # print(f"where no string exist {disk_status}  {non_strings[disk_status]}")

                            group_condition = (df_csv[region] == reg) & \
                                              (df_csv[zone] == az) & \
                                              (df_csv[disk_type] == dt) & \
                                              (df_csv[a_name] == an) & \
                                              (df_csv[other2_column_name] == nt) & \
                                              ((df_csv[root_flag] == False) |
                                               ((df_csv[root_flag] == True) & (
                                                   df_csv[host_type].str.contains("Windows", case=False)) &
                                                (df_csv[disk_size] > win_threshold)) |
                                               ((df_csv[root_flag] == True) & (
                                                   df_csv[host_type].str.contains("Linux", case=False)) &
                                                (df_csv[disk_size] > lin_threshold))) & \
                                              (df_csv[disk_status].str.lower().isin(valid_disk_status))
                            # print(f"matched condition {len(df_csv[group_condition])} group id {group_id}")
                            # print("checking shape for condition")
                            # print(df_csv.shape)
                            # print(group_condition.shape)
                            df_csv['group_id'] = df_csv['group_id'].mask(group_condition, group_id)
                            # print(f" groups {df_csv['group_id'].unique()} ")
                            top_compute = {}
                            df_csv[compute] = df_csv[compute].fillna("not_given")
                            if count_compute_flag:
                                # df_csv[compute] = df_csv[compute].fillna("not_given")
                                unique_compute_instances_list = df_csv.loc[
                                    (df_csv['group_id'] == group_id), compute].unique()
                                unique_compute_instances = len(unique_compute_instances_list)
                                for cp in unique_compute_instances_list:
                                    if not cp == "not_given":
                                        vm_data = vm_data_all   # precomputed once, above (was a per-VM full-frame apply)

                                        vm_perf_tier = 0
                                        if (vm_data[cp].get("non_perf_tot_iops") > 999 or vm_data[cp].get(
                                                "non_perf_tot_bw") > 124):
                                            vm_perf_tier = 1
                                        if (vm_data[cp].get("non_perf_num_vols") == 1 and vm_data[cp].get(
                                                "perf_num_vols") >= 1) or vm_data[cp].get("perf_num_vols") >= 2:
                                            vm_perf_tier = 2
                                            if vm_data[cp].get("perf_tot_iops") > 999 or vm_data[cp].get(
                                                    "perf_tot_bw") > 124:
                                                vm_perf_tier = 3
                                                # 04/06 MR Moved Prem V2 as a tier 4 instead of tier 5 trigger
                                                if (vm_data[cp].get("perf_tot_iops") / vm_data[cp].get(
                                                        "perf_num_vols") > 3000) or (
                                                        vm_data[cp].get("perf_tot_bw") / vm_data[cp].get(
                                                    "perf_num_vols") > 150) or (
                                                        any("V2" in s for s in vm_data[cp]["perf_disk_types"])):
                                                    vm_perf_tier = 4
                                                # 04/06/26 MR - changed alg to make V2 a perf 4 not perf 5 trigger
                                                if any("ULTRA" in s for s in vm_data[cp]["perf_disk_types"]):
                                                    vm_perf_tier = 5
                                        # print(f"vm {cp} tier {vm_perf_tier}")

                                        min_ec_model = ec_config[ec_mods[0]]["pscSku"]
                                        # print(f"min_ec_model {min_ec_model} alice")
                                        for i in range(1, len(ec_mods)):
                                            # print(f" iops {ec_config[ec_mods[i]]["usable_limit_iops"]* initial_iops_full_rate} and {vm_data[cp].get("perf_tot_iops")} and {vm_data[cp].get("non_perf_tot_iops")} bw {vm_data[cp]["perf_tot_bw"]} {per_vm_v20_bw_limit} and {per_vm_v10_bw_limit}")
                                            if (vm_data[cp].get("perf_tot_iops") + vm_data[cp].get("non_perf_tot_iops")) > (ec_config[ec_mods[i]]["usable_limit_iops"] * initial_iops_full_rate):
                                                min_ec_model = ec_config[ec_mods[i]]["pscSku"]
                                            # 04/06/26 MR added flag to skip BW affect
                                            if vm_data[cp]["perf_tot_bw"] > per_vm_v20_bw_limit and not ignore_bw_provisioned_flag:
                                                # print(f"bump to v50 was due to bandwidth {vm_data[cp]['perf_tot_bw']}")
                                                min_ec_model = "V50MP2R2"
                                            # 04/06/26 MR added flag to skip BW affect
                                            elif vm_data[cp]["perf_tot_bw"] > per_vm_v10_bw_limit and not ignore_bw_provisioned_flag:
                                                min_ec_model = "V20MP2R2"
                                        # print(f"min_ec_model {min_ec_model} bob")
                                        if supported_region_list_flag and int(ec_config[min_ec_model]["index"]) > int(supported_region_list[reg]) and min_ec_model == "V50MP2R2":
                                            min_ec_model = "V20MP2R2"

                                        vm_data[cp]["vm_perf_tier"] = vm_perf_tier
                                        vm_data[cp]["min_ec_model"] = min_ec_model
                                        # print(vm_perf_tier,vm_data)
                                        condition = (df_csv[compute] == cp) & (min_ec_model > df_csv["min_ec_model"])
                                        df_csv["vm_perf_tier"] = df_csv["vm_perf_tier"].mask(condition, vm_perf_tier)
                                        df_csv["min_ec_model"] = df_csv["min_ec_model"].mask(condition, min_ec_model)

                                # max_vol_perf_tiers = df_csv.loc[(df_csv['group_id'] == group_id), "vm_perf_tier"].max()
                                # print(f"max tier {max_vol_perf_tiers}")

                                # min_sku_perf = df_csv.loc[(df_csv['group_id'] == group_id), "min_ec_model"].unique()
                                # print(f"main skus {min_sku_perf}")

                            else:
                                unique_compute_instances = 0
                                min_sku_perf = 100
                                max_vol_perf_tiers = 0

                            if in_region_mapping_flag or cross_region_mapping:
                                group_condition = (df_csv[region] == reg) & \
                                                  (df_csv[zone] == az) & \
                                                  (df_csv[disk_type] == dt) & \
                                                  (df_csv[a_name] == an) & \
                                                  (df_csv[other2_column_name] == nt) & \
                                                  ((df_csv[root_flag] == False) |
                                                   ((df_csv[root_flag] == True) & (
                                                       df_csv[host_type].str.contains("Windows", case=False)) &
                                                    (df_csv[disk_size] > win_threshold)) |
                                                   ((df_csv[root_flag] == True) & (
                                                       df_csv[host_type].str.contains("Linux", case=False)) &
                                                    (df_csv[disk_size] > lin_threshold))) & \
                                                  (df_csv[disk_status].str.lower().isin(valid_disk_status))

                                df_csv['group_id'] = df_csv['group_id'].mask(group_condition, group_id)
                                # print(f" groups {df_csv['group_id'].unique()} ")
                                rep_flag = 1
                                # print(f"in rep {in_region_mapping_flag} {reg} {az}")

    # Get the indices of rows that meet the condition (e.g., column 'B' is 'apple')
    # print("in the soup",df_csv.shape)
    df_sample2 = df_csv.loc[df_csv['group_id'] != 0]
    # print(df_sample2.shape)
    del df_csv
    df_csv = df_sample2
    del df_sample2
    # print(df_csv.shape)
    price_mode_list = ["1 Year", "3 Years", "onDemand"]
    price_mode = price_mode_list[1]

    initial_size_rate = initial_cap_rate
    growth_rate = growth
    num_of_years = 5
    # df_ec_pricing and df_ec_model

    # Parse each group
    group_list = df_csv['group_id'].unique()
    # print(f"group ids: {group_list}")
    group_config = []
    # Look at each group individually
    array_costs = []
    # print(f"error 4 group_list {group_list}")
    tot_capacity_all_tiers = 0
    tot_native_costs = 0
    for g in group_list:
        group_set = 0
        group_index = 0
        set_index = 0
        # find the regino for this group
        reg_list = df_csv.loc[(df_csv['group_id'] == g), region].unique()
        if len(reg_list) == 1:
            reg = reg_list[0]
            if supported_region_list_flag:
                if (not reg in supported_region_list) or supported_region_list[reg] == 0:
                    print(f"data found in region that does not supprot ec, skipping region {reg} group {g}")
                    continue
        else:
            raise f"group {g} could not get region info {reg_list}"

        group_tot_capacity_all_tiers = df_csv.loc[
            (df_csv['group_id'] == g), disk_size].sum()
        # print(f"Group Id {g} Total Capacity {group_tot_capacity_all_tiers}")
        tot_capacity_all_tiers = tot_capacity_all_tiers + group_tot_capacity_all_tiers
        tot_native_costs = tot_native_costs + return_sum_total_cost(df_csv,g)
    file_name_parsed_data = "parsed_data.csv"
    object_prefix = f"{output_bucket_prefix}"
    # Use the put_object API to upload the CSV data
    status_up = upload_df_to_s3(df_csv, file_name_parsed_data, object_prefix)

    # Also upload a copy of the JSON configuration used for this run
    try:
        config_json_key = f"{output_bucket_prefix}source_data_config.json"
        s3_client_cfg = _s3_client()
        config_payload = json.dumps(event, indent=2, default=str).encode("utf-8")
        s3_client_cfg.put_object(
            Bucket=s3_bucket,
            Key=config_json_key,
            Body=config_payload,
            ContentType="application/json",
        )
        # print(f"Uploaded run config JSON to {config_json_key}")
    except Exception as cfg_exc:
        print(f"Warning: could not upload run config JSON: {cfg_exc}")
    tot_reg_list = return_unique_list_from_df(df_csv,region)
    results = {"status": status_up, "num_groups": len(group_list), "tot_capacity": tot_capacity_all_tiers, "tot_costs": tot_native_costs, "regions": tot_reg_list, "df": df_csv}
    return results

def calc_compute_usage(row, vm_name, compute_column_name, iops_column_name, bw_column_name, disk_size_column_name,
                       disk_type_column_name, vm_data, min_sku_value):
    if row[compute_column_name] == vm_name:
        new_iops = row[iops_column_name]
        new_bw = row[bw_column_name]
        new_size = row[disk_size_column_name]
        new_type = row[disk_type_column_name]
        if vm_name in vm_data:
            if "Standard" in new_type:
                perf_type = 0
                vm_data[vm_name]["non_perf_tot_iops"] = vm_data[vm_name]["non_perf_tot_iops"] + new_iops
                vm_data[vm_name]["non_perf_tot_bw"] = vm_data[vm_name]["non_perf_tot_bw"] + new_bw
                vm_data[vm_name]["non_perf_num_vols"] = vm_data[vm_name]["non_perf_num_vols"] + 1
                vm_data[vm_name]["non_perf_tot_cap"] = vm_data[vm_name]["non_perf_tot_cap"] + new_size
                if not new_type in vm_data[vm_name].get('non_perf_disk_types', []):
                    disk_types = vm_data[vm_name].get('non_perf_disk_types', [])
                    disk_types.append(new_type)
                    vm_data[vm_name]['non_perf_disk_types'] = disk_types
            else:
                perf_type = 1
                vm_data[vm_name]["perf_tot_iops"] = vm_data[vm_name]["perf_tot_iops"] + new_iops
                vm_data[vm_name]["perf_tot_bw"] = vm_data[vm_name]["perf_tot_bw"] + new_bw
                vm_data[vm_name]["perf_num_vols"] = vm_data[vm_name]["perf_num_vols"] + 1
                vm_data[vm_name]["perf_tot_cap"] = vm_data[vm_name]["perf_tot_cap"] + new_size
                if not new_type in vm_data[vm_name].get('perf_disk_types', []):
                    disk_types = vm_data[vm_name].get('perf_disk_types', [])
                    disk_types.append(new_type)
                    vm_data[vm_name]['perf_disk_types'] = disk_types
        else:
            if "Standard" in new_type:
                perf_type = 0
                vm_data[vm_name] = {
                    "non_perf_tot_iops": new_iops,
                    "non_perf_tot_bw": new_bw,
                    "non_perf_num_vols": 1,
                    "non_perf_disk_types": [new_type],
                    "non_perf_tot_cap": new_size,
                    "perf_tot_iops": 0,
                    "perf_tot_bw": 0,
                    "perf_num_vols": 0,
                    "perf_disk_types": [],
                    "perf_tot_cap": 0,
                    "vm_perf_tier": perf_type,
                    "min_ec_model": min_sku_value}
            else:
                perf_type = 1
                vm_data[vm_name] = {
                    "non_perf_tot_iops": 0,
                    "non_perf_tot_bw": 0,
                    "non_perf_num_vols": 0,
                    "non_perf_disk_types": [],
                    "non_perf_tot_cap": 0,
                    "perf_tot_iops": new_iops,
                    "perf_tot_bw": new_bw,
                    "perf_num_vols": 1,
                    "perf_disk_types": [new_type],
                    "perf_tot_cap": new_size,
                    "vm_perf_tier": perf_type,
                    "min_ec_model": min_sku_value}
        # print(f"vm data {vm_data[vm_name]}")
    return


def calc_compute_usage_all(row, vm_data, compute_column_name, iops_column_name, bw_column_name,
                           disk_size_column_name, disk_type_column_name, min_sku_value):
    """One-pass equivalent of calc_compute_usage: buckets EACH row into its own VM's
    aggregate, so a SINGLE df.apply builds vm_data for every VM at once — instead of
    running a full-frame apply once per VM (which was O(VMs x rows)). The per-row
    accumulation logic is identical to calc_compute_usage (same running sums, same
    NaN propagation, same disk-type ordering), so results are unchanged."""
    vm_name = row[compute_column_name]
    if vm_name == "not_given":
        return
    new_iops = row[iops_column_name]
    new_bw = row[bw_column_name]
    new_size = row[disk_size_column_name]
    new_type = row[disk_type_column_name]
    if vm_name in vm_data:
        if "Standard" in new_type:
            vm_data[vm_name]["non_perf_tot_iops"] = vm_data[vm_name]["non_perf_tot_iops"] + new_iops
            vm_data[vm_name]["non_perf_tot_bw"] = vm_data[vm_name]["non_perf_tot_bw"] + new_bw
            vm_data[vm_name]["non_perf_num_vols"] = vm_data[vm_name]["non_perf_num_vols"] + 1
            vm_data[vm_name]["non_perf_tot_cap"] = vm_data[vm_name]["non_perf_tot_cap"] + new_size
            if not new_type in vm_data[vm_name].get('non_perf_disk_types', []):
                disk_types = vm_data[vm_name].get('non_perf_disk_types', [])
                disk_types.append(new_type)
                vm_data[vm_name]['non_perf_disk_types'] = disk_types
        else:
            vm_data[vm_name]["perf_tot_iops"] = vm_data[vm_name]["perf_tot_iops"] + new_iops
            vm_data[vm_name]["perf_tot_bw"] = vm_data[vm_name]["perf_tot_bw"] + new_bw
            vm_data[vm_name]["perf_num_vols"] = vm_data[vm_name]["perf_num_vols"] + 1
            vm_data[vm_name]["perf_tot_cap"] = vm_data[vm_name]["perf_tot_cap"] + new_size
            if not new_type in vm_data[vm_name].get('perf_disk_types', []):
                disk_types = vm_data[vm_name].get('perf_disk_types', [])
                disk_types.append(new_type)
                vm_data[vm_name]['perf_disk_types'] = disk_types
    else:
        if "Standard" in new_type:
            perf_type = 0
            vm_data[vm_name] = {
                "non_perf_tot_iops": new_iops, "non_perf_tot_bw": new_bw, "non_perf_num_vols": 1,
                "non_perf_disk_types": [new_type], "non_perf_tot_cap": new_size,
                "perf_tot_iops": 0, "perf_tot_bw": 0, "perf_num_vols": 0, "perf_disk_types": [],
                "perf_tot_cap": 0, "vm_perf_tier": perf_type, "min_ec_model": min_sku_value}
        else:
            perf_type = 1
            vm_data[vm_name] = {
                "non_perf_tot_iops": 0, "non_perf_tot_bw": 0, "non_perf_num_vols": 0,
                "non_perf_disk_types": [], "non_perf_tot_cap": 0,
                "perf_tot_iops": new_iops, "perf_tot_bw": new_bw, "perf_num_vols": 1,
                "perf_disk_types": [new_type], "perf_tot_cap": new_size,
                "vm_perf_tier": perf_type, "min_ec_model": min_sku_value}
    return

def return_sum_capcaity(df,gid,disk_size_col_name):
    return df.loc[(df['group_id'] == gid), disk_size_col_name].sum()

def return_volume_count(df,gid,col_name):
    return df.loc[(df['group_id'] == gid), col_name].count()

def return_compute_count(df,gid,col_name):
    return df.loc[(df['group_id'] == gid), col_name].drop_duplicates()

def return_unique_list_from_df(df,column):
    return df[[column]].drop_duplicates()

def return_sum_total_cost(df,gid):
    return df.loc[(df['group_id'] == gid), "total_cost"].sum()

def return_list_and_frequency_from_df(df,column):
    return df[column].value_counts().reset_index()

def upload_df_to_s3(df, name,prefix):
    global s3_region
    global s3_bucket
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)  # index=False prevents writing the pandas index as a column
    object_key = rf"{prefix}{name}"
    # Write via the session's storage backend (S3 or local disk).
    try:
        _s3_client().put_object(
            Bucket=s3_bucket,
            Key=object_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        # print(f"Successfully uploaded {object_key} to {s3_bucket}")
    except Exception as e:
        print(f"Error uploading to storage: {e}")
        return 500
    return 200

def tco_by_group_y1(params,df_parsed, df_azure_disk, df_ec_infra, s3_path, save_outputs=True):

    use_vm_inclusion_list = params.get("use_vm_inclusion_list", 0)
    mid_tier_ratio_limit_V10 = params.get("mid_tier_ratio_limit_V10", 0.75)
    mid_tier_capacity_limit_V10_GiB = params.get("mid_tier_capacity_limit_V10_GiB", 51050)
    top_tier_ratio_limit_V20 = params.get("top_tier_ratio_limit_V20", 0.75)
    top_tier_capacity_limit_V20_GiB = params.get("top_tier_capacity_limit_V20_GiB", 102400)
    ec_sku_bias = params.get("default_sku_model", "V10MP2R2")
    initial_iops_full_rate = params.get("initial_iops_full_rate", 0.9)
    initial_cap_rate = params.get("initial_cap_rate", 0.80)
    ec_license_cost_per_gib = float(params.get("ec_license_cost_per_gib", "0.06"))
    price_mode_list = ["1 Year", "3 Years", "onDemand"]
    price_mode = params.get("price_mode", price_mode_list[1])
    mixed_skus_in_group_flag = params.get("mixed_skus_in_group_flag", False)
    ramp_months = int(params.get("ramp_months", 0))
    #price_mode = price_mode_list[1]

    initial_size_rate = initial_cap_rate
    growth_rate = params.get("growth", 0.2)
    num_of_years = params.get("num_of_years", 5)
    data_reduction_ratio = params.get("drr", 4)
    # df_ec_pricing and df_ec_model

    # Parse each group
    monthly_snapshot_rate = params.get('monthly_snapshot_rate', 0.1)
    initial_cap_rate = params.get('initial_cap_rate', 0.8)
    ignore_iops_provisioned = params.get('ignore_iops_provisioned', False)
    ignore_iops_provisioned_flag = ignore_iops_provisioned
    efficiency = params.get('efficiency', 0.65)

    # 04/13 MR - adding check for max number of volumes
    max_volumes_per_array_val = params.get("max_volumes_per_array", -99)
    if max_volumes_per_array_val > 0:
        max_volumes_per_array_flag = True
        max_volumes_per_array = int(max_volumes_per_array_val)
    else:
        max_volumes_per_array_flag = False

    # print(f"group ids: {group_list}")
    group_config = []
    # Look at each group individually
    array_costs = []
    # print(f"error 4 group_list {group_list}")
    group_list = df_parsed['group_id'].unique()
    ec_config = params.get("ec_data", {}).get("models", {})
    sku_list = ec_config.keys()

    supported_region_list = params.get("region_list", [])

    # ── Resolve source-data column names from the stored config ────────────
    # source_data_config holds the column INDEX for each field (the same keys
    # main2() reads). A value of -99 means the field was absent in the source
    # data and main2() created a placeholder column with the literal name used
    # below, so these names always match the columns present in df_parsed.
    source_data_config = params.get("source_data_config", {}) or {}
    col_names = df_parsed.columns.tolist()

    region_column_value        = int(source_data_config.get("region", -99))
    disk_size_column_value     = int(source_data_config.get("disk_size", -99))
    iops_column_value          = int(source_data_config.get("iops", -99))
    count_compute_column_value = int(source_data_config.get("count_compute", -99))
    zone_column_value          = int(source_data_config.get("zone", -99))
    account_column_value       = int(source_data_config.get("subscription_or_account_id", -99))
    vnet_column_value          = int(source_data_config.get("vnet_or_vpc", -99))

    region    = "no_region"    if region_column_value == -99 else col_names[region_column_value]
    disk_size = col_names[disk_size_column_value]
    iops      = "no_iops"      if iops_column_value == -99 else col_names[iops_column_value]
    compute   = "no_vms_given" if count_compute_column_value == -99 else col_names[count_compute_column_value]
    # Account (subscription), availability zone, and vnet for reporting. main2()
    # uses placeholder names when a field is absent, and renames the zone column
    # to "true_az" when az-mapping is enabled.
    zone      = "no_zone"   if zone_column_value    == -99 else col_names[zone_column_value]
    account   = "no_other"  if account_column_value == -99 else col_names[account_column_value]
    vnet      = "no_other2" if vnet_column_value    == -99 else col_names[vnet_column_value]
    if "true_az" in col_names:
        zone = "true_az"

    for g in group_list:
        sku_in_region = []
        group_set = 0
        group_index = 0
        set_index = 0
        # find the regino for this group
        reg_list = df_parsed.loc[(df_parsed['group_id'] == g), region].unique()
        reg = None
        if len(reg_list) == 1:
            reg = reg_list[0]
            for sku in sku_list:
                if reg in ec_config.get(sku).get("azure_supported_regions",[]):
                    sku_in_region.append(sku)
        if len(sku_in_region) == 0 or reg == None:
            print(f"data found in region that does not support any ec skus, skipping region {reg} group {g}")
            continue

        group_tot_capacity_all_tiers = df_parsed.loc[
            (df_parsed['group_id'] == g), disk_size].sum()
        group_cap_by_tier = {"0": 0, "1": 0, "2": 0, "3": 0, "4": 0, "5": 0}

        min_perf_list = df_parsed.loc[(df_parsed['group_id'] == g), "vm_perf_tier"].unique().tolist()
        # print(min_perf_list, "min perf list", g)
        for b in range(6):
            group_cap_by_tier[str(b)] = df_parsed.loc[
                (df_parsed['group_id'] == g) &
                (df_parsed["vm_perf_tier"] == b), disk_size].sum()
        # print(min_perf_list, "min perf list", g, group_cap_by_tier)
        # 04/06/26 MR change tier groups to make it harder to get to V50
        # mid_tier_capacity = (group_cap_by_tier["3"] + group_cap_by_tier["4"] + group_cap_by_tier["5"])
        # top_tier_capacity = (group_cap_by_tier["4"] + group_cap_by_tier["5"])
        mid_tier_capacity = (group_cap_by_tier["3"] + group_cap_by_tier["4"] + group_cap_by_tier["5"])
        # 04/06 MR Removed tier 4 as a trigger for V50 usage consideration
        top_tier_capacity = group_cap_by_tier["5"]
        group_min_sku = "V10MP2R2"
        if group_tot_capacity_all_tiers > 0:
            percent_mid_tier_capacity = mid_tier_capacity / group_tot_capacity_all_tiers
            percent_top_tier_capacity = top_tier_capacity / group_tot_capacity_all_tiers

            if (mid_tier_capacity > mid_tier_capacity_limit_V10_GiB) and (
                    percent_mid_tier_capacity > mid_tier_ratio_limit_V10) and ignore_iops_provisioned == False:
                if "v20MP2R2" in sku_in_region and "V10MP2R2" in sku_in_region:
                        group_min_sku = "V20MP2R2"
                elif "V10MP2R2" in sku_in_region:
                    group_min_sku = "V10MP2R2"
            if ((top_tier_capacity) > top_tier_capacity_limit_V20_GiB) and (
                    percent_top_tier_capacity > top_tier_ratio_limit_V20) and ignore_iops_provisioned == False:
                if "v50MP2R2" in sku_in_region and ("V20MP2R2" in sku_in_region or "V10MP2R2" in sku_in_region):
                    group_min_sku = "V50MP2R2"
                elif "V20MP2R2" in sku_in_region:
                    group_min_sku = "V20MP2R2"
                elif "V10MP2R2" in sku_in_region:
                    group_min_sku = "V10MP2R2"
                else:
                    print("ERROR: no sku found in region")
                    pass
        if not mixed_skus_in_group_flag:
            # SKUs may not be mixed within a group: promote every member of this
            # group to the highest SKU found in the group (largest "index").
            group_skus = df_parsed.loc[
                (df_parsed['group_id'] == g), "min_ec_model"].dropna().unique().tolist()
            group_skus.append(group_min_sku)
            highest_sku = group_min_sku
            highest_index = ec_config.get(group_min_sku, {}).get("index", -1)
            for sku in group_skus:
                sku_index = ec_config.get(sku, {}).get("index")
                if sku_index is not None and sku_index > highest_index:
                    highest_index = sku_index
                    highest_sku = sku
            group_min_sku = highest_sku
            df_parsed.loc[(df_parsed['group_id'] == g), "min_ec_model"] = group_min_sku

        # print(f"group {g} group min sku {group_min_sku} ")

        models = []
        # check if there is a minimum sku set for this group
        df_parsed["min_ec_model"] = df_parsed["min_ec_model"].fillna(group_min_sku)
        #min_sku_list_unsorted = []
        #for sku in sku_in_region:
        #    min_sku_list_unsorted.append(int(ec_config.get(sku).get("index")))

        # In the parsed data for this group return back a list of min sku values indicated by performance
        data_min_sku_list = sorted(df_parsed.loc[(df_parsed['group_id'] == g), "min_ec_model"].unique())
        # Compare the min sku values in the data to what skus are supported in the region
        min_sku_list = list(set(data_min_sku_list) & set(sku_in_region))
        min_sku_list.sort(key=lambda x: int(re.search(r'\d+', x).group()))
        # If no overlap between min sku values in data and what is supported in the region (e.g. data requires a bigger sku then is supported) return back the largest sku supported in the region
        if len(min_sku_list) == 0:
            min_sku_list.append(max(sku_in_region))
        # print(f"sorted min sku list {min_sku_list}")
        min_value_from_min_sku_list = 500
        for ms in min_sku_list:
            # print(ms)
            if ec_config.get(ms).get("index") < min_value_from_min_sku_list:
                min_value_from_min_sku_list = ec_config.get(ms).get("index")
        # print(f" final min sku value {min_value_from_min_sku_list}")

        group_fixed_cost = {}

        # print(f"error 3 {g} {reg_list}")
        # Get the list of SKUs that were available in the pricing data - some skus might not be supported because of region
        ec_model_list = sku_in_region
        #ec_model_list_pre = df_ec_infra["pscSku"].dropna().unique()
        #ec_model_list = list(filter(None, ec_model_list_pre))
        # print(f"filtered ec model list {ec_model_list}")
        #try:
        #    ec_model_list.remove("nan")
            # print("found and removed nan")
        #except:
        #    aaaaa = True

        # Calculate the fixed costs that are model independent - OS disk for controllers
        controller_os_disk_cost_rate_list = df_ec_infra.loc[
            (df_ec_infra["armRegionName"] == reg) &
            (df_ec_infra["costType"] == "osDisk"), "monthRate"].unique()
        if len(controller_os_disk_cost_rate_list) == 1:
            controller_os_disk_cost_monthly_rate = controller_os_disk_cost_rate_list[0] * 2
        else:
            raise f"lookup of controller cost error {controller_os_disk_cost_rate_list}"
        # Calculate the SKU dependdent csots for all SKUs
        for p in ec_model_list:

            # Populate the sku list that will be used later for price comparison - this will be all skus minus the one that dont meet the minimum requirements
            #if (supported_region_list_flag and ec_config[p]["index"] <= supported_region_list[reg]) or not supported_region_list_flag:
            if ec_config.get(p).get("index") >= min_value_from_min_sku_list:
                models.append(p)
            else:
                print(f"warning: not all ec models suported in this region {reg} group {g} sku {p}")
                continue

            # Calculate the  monthly rate for the controllers
            controller_rate_monthly_price_list = df_ec_infra.loc[
                (df_ec_infra["pscSku"] == p) &
                (df_ec_infra["armRegionName"] == reg) &
                (df_ec_infra["costType"] == "Controller") &
                (df_ec_infra["term"] == price_mode), "monthRate"].unique()
            if len(controller_rate_monthly_price_list) == 1:
                controller_vm_cost_monthly_rate = controller_rate_monthly_price_list[0]
            else:
                print(f"found error in controller cost lookup {p} {reg} {price_mode}")
                if len(controller_rate_monthly_price_list) > 1:
                    print(f"more then one {controller_rate_monthly_price_list}")
                    controller_vm_cost_monthly_rate = controller_rate_monthly_price_list[0]
                else:
                    print("none found")
                    ec_model_list.remove(p)
                    continue
                # raise f"lookup of controller cost error {controller_rate_monthly_price_list}"

            # Calculate the  monthly rate for the controllers nvram capacity
            controller_nvram_cap_monthly_price_list = df_ec_infra.loc[
                (df_ec_infra["meterName"] == "Premium LRS Provisioned Capacity") &
                (df_ec_infra["armRegionName"] == reg), "monthRate"].unique()
            if len(controller_nvram_cap_monthly_price_list) == 1:
                # print(controller_nvram_cap_monthly_price_list[0],  ec_config.get(p).get("nvram_size"), ec_config.get(p).get("nvram_num"))
                controller_nvram_cap_cost_monthly_rate = controller_nvram_cap_monthly_price_list[0] * ec_config.get(p).get("nvram_size") * ec_config.get(p).get("nvram_num")
            else:
                print(f"found error in nvrmam cost lookup {p} {reg} {price_mode}")
                if len(controller_nvram_cap_monthly_price_list) > 1:
                    print(f"more then one {controller_nvram_cap_monthly_price_list}")
                    controller_vm_cost_monthly_rate = controller_rate_monthly_price_list[0]
                else:
                    print("none found")
                    ec_model_list.remove(p)
                    continue
                # raise f"lookup of controller nvram cap cost error {controller_nvram_cap_monthly_price_list}"

            # Calculate the  monthly rate for the controllers nvram iops
            controller_nvram_iops_monthly_price_list = df_ec_infra.loc[
                (df_ec_infra["meterName"] == "Premium LRS Provisioned IOPS") &
                (df_ec_infra["armRegionName"] == reg), "monthRate"].unique()
            if len(controller_nvram_iops_monthly_price_list) == 1:
                controller_nvram_iops_cost_monthly_rate = controller_nvram_iops_monthly_price_list[0] * (
                        ec_config.get(p).get("NvramIOPS") - 3000) * ec_config.get(p).get("nvram_num")
            else:
                print(f"found error in nvrmam iops cost lookup {p} {reg} {price_mode}")
                if len(controller_nvram_iops_monthly_price_list) > 1:
                    print(f"more then one {controller_nvram_iops_monthly_price_list}")
                    controller_vm_cost_monthly_rate = controller_rate_monthly_price_list[0]
                else:
                    print("none found")
                    ec_model_list.remove(p)
                    continue
                # raise f"lookup of controller nvram iops cost error {controller_nvram_iops_monthly_price_list}"

            # Calculate the  monthly rate for the controllers nvram bw
            controller_nvram_bw_monthly_price_list = df_ec_infra.loc[
                (df_ec_infra["meterName"] == "Premium LRS Provisioned Throughput (MBps)") &
                (df_ec_infra["armRegionName"] == reg), "monthRate"].unique()

            if len(controller_nvram_bw_monthly_price_list) == 1:
                controller_nvram_bw_cost_monthly_rate = controller_nvram_bw_monthly_price_list[0] * (ec_config.get(p).get("NvramBW") - 125) * ec_config.get(p).get("nvram_num")
            else:
                print(f"found error in nvrmam bw cost lookup {p} {reg} {price_mode}")
                if len(controller_nvram_bw_monthly_price_list) > 1:
                    print(f"more then one {controller_nvram_bw_monthly_price_list}")
                    controller_vm_cost_monthly_rate = controller_rate_monthly_price_list[0]
                else:
                    print("none found")
                    ec_model_list.remove(p)
                    continue
                # raise f"lookup of controller nvram iops cost error {controller_nvram_bw_monthly_price_list}"
            # if g == 1 or g == "1":
            #    print("group 1 fixed cost",int(controller_vm_cost_monthly_rate), int(controller_os_disk_cost_monthly_rate), int(controller_nvram_cap_cost_monthly_rate), int(controller_nvram_iops_cost_monthly_rate), int(controller_nvram_bw_cost_monthly_rate))

            # Calculate the per sku per instance fixed costs - includes everything but the virtual disks cost
            group_fixed_cost[p] = (controller_vm_cost_monthly_rate + controller_os_disk_cost_monthly_rate + controller_nvram_cap_cost_monthly_rate + controller_nvram_iops_cost_monthly_rate + controller_nvram_bw_cost_monthly_rate) * 2
            # if g == 1 or g == "1":
            #    print("group 1 total fixed cost",group_fixed_cost[p],p)
            # print(group_fixed_cost)
        if len(models) < 1:
            continue
        vd_cap_monthly_price_list = df_ec_infra.loc[
            (df_ec_infra["meterName"] == "Premium LRS Provisioned Capacity") &
            (df_ec_infra["armRegionName"] == reg), "monthRate"].unique()
        vd_iops_monthly_price_list = df_ec_infra.loc[
            (df_ec_infra["meterName"] == "Premium LRS Provisioned IOPS") &
            (df_ec_infra["armRegionName"] == reg), "monthRate"].unique()
        vd_bw_monthly_price_list = df_ec_infra.loc[
            (df_ec_infra["meterName"] == "Premium LRS Provisioned Throughput (MBps)") &
            (df_ec_infra["armRegionName"] == reg), "monthRate"].unique()
        if len(vd_bw_monthly_price_list) != 1 or len(vd_iops_monthly_price_list) != 1 or len(
                vd_cap_monthly_price_list) != 1:
            raise f"lookup from price list failed {vd_cap_monthly_price_list} {vd_iops_monthly_price_list} {vd_bw_monthly_price_list}"
        else:
            vd_cap_monthly_price = vd_cap_monthly_price_list[0]
            vd_iops_monthly_price = vd_iops_monthly_price_list[0]
            vd_bw_monthly_price = vd_bw_monthly_price_list[0]
        configuration_id = 0
        # group_total_cost = []
        # group_cap_cost = []
        # group_iops_cost = []
        # group_bw_cost = []
        # group_snap_cost = []
        group_tot_iops = {}
        # group_tot_bw = []
        group_prov_cap = {}

        # group_paid_cap = []
        group_tot_compute = {}
        group_tot_volumes = {}
        # group_disk_types = []
        # group_cap_by_type = []
        # print(f"error {min_sku_list}")
        for ms in min_sku_list:
            group_tot_iops[ms] = df_parsed.loc[
                (df_parsed['group_id'] == g) &
                (df_parsed["min_ec_model"] == ms), iops].sum()

            group_prov_cap[ms] = df_parsed.loc[
                (df_parsed['group_id'] == g) &
                (df_parsed["min_ec_model"] == ms), disk_size].sum()

            tmp_list2 = df_parsed.loc[
                (df_parsed['group_id'] == g) &
                (df_parsed["min_ec_model"] == ms), compute]
            # print(f"error 2 {tmp_list2}")

            tmp_list = df_parsed.loc[
                (df_parsed['group_id'] == g) &
                (df_parsed["min_ec_model"] == ms), compute].unique()

            group_tot_compute[ms] = len(tmp_list)
            group_tot_volumes[ms] = len(df_parsed.loc[
                                            (df_parsed['group_id'] == g) &
                                            (df_parsed["min_ec_model"] == ms)])

        # Loop through all of the model types for this group to get cost points
        index = 0
        sku_index = 0
        # print(f" models list {models}")
        for mod in models:
            min_sku_index = 0
            if ec_config[mod]["index"] >= min_value_from_min_sku_list:
                native_cost_tot = df_parsed.loc[(df_parsed['group_id'] == g), "total_cost"].sum()
                # print(f"processing model {mod} {g}")
                # first run use the first vd drive count
                disk_count = ec_config.get(mod).get("vd_count")

                # Calculate the EC capacity size to be licensed for this group.  This includes the replacement capacity (reduced by the efficiency rate) plus the capacity needed for snapshots
                group_prov_cap_all = 0
                # group_cap_cost_all = 0
                # group_total_cost_all = 0
                # group_iops_cost_all = 0
                # group_bw_cost_all = 0
                # group_paid_cap_all = 0
                group_tot_compute_all = 0
                group_tot_volumes_all = 0
                group_tot_iops_all = 0
                # group_disk_types_all = []
                run_count = 0
                additional_skus_for_this_run = []

                for sku in min_sku_list:
                    if int(ec_config.get(mod).get("index")) >= int(ec_config.get(sku).get("index")):
                        # print(f"error in sku? {sku} {type(group_prov_cap)}")
                        group_prov_cap_all = int(group_prov_cap[sku]) + group_prov_cap_all
                        # group_cap_cost_all = group_cap_cost[sku] + group_cap_cost_all
                        # group_total_cost_all = group_total_cost[sku] + group_total_cost_all
                        # group_iops_cost_all = group_iops_cost[sku] + group_iops_cost_all
                        # group_bw_cost_all = group_bw_cost[sku] + group_bw_cost_all
                        # group_paid_cap_all = group_paid_cap[sku] + group_paid_cap_all

                        group_tot_compute_all = int(group_tot_compute[sku]) + int(group_tot_compute_all)
                        # print(f"sku stuff {group_tot_compute_all} {group_tot_compute[sku]} {sku}")
                        group_tot_volumes_all = int(group_tot_volumes[sku]) + group_tot_volumes_all
                        # group_disk_types_all = list(set(group_disk_types_all + group_disk_types[sku]))
                        group_tot_iops_all = int(group_tot_iops[sku]) + group_tot_iops_all

                    else:
                        # print(f"made it to halla {run_count}")
                        additional_skus_for_this_run.append(sku)
                        run_count = run_count + 1
                        # print(f"made it to halla {run_count}")
                # print(f"add skus {additional_skus_for_this_run}")
                # print(f"clump for mod {mod} iops {group_tot_iops_all} cap {group_prov_cap_all}")
                # 04/06 MR Option to ingore all snaphot costs if change rate is zero
                if float(monthly_snapshot_rate) == 0:
                    group_ec_licensed_capacity = (group_prov_cap_all * efficiency)
                else:
                    group_ec_licensed_capacity = (group_prov_cap_all * efficiency) * (1 + (monthly_snapshot_rate / 2))
                group_ec_capacity_matrix = ec_config[mod]["raw_cap_matrix"]
                # group_ec_capacity_matrix_str = ec_config[mod]["raw_cap_matrix"].apply(lambda row_list: [key for d in row_list for key in d.keys()]).tolist()
                group_ec_capacity_matrix_str = ec_config[mod]["raw_cap_matrix"].keys()
                # print(group_ec_capacity_matrix_str)
                group_ec_capacity_matrix_list = [int(x) for x in group_ec_capacity_matrix_str]

                # Calculate the Initial Usable capacity limit for this SKU model usable_capacity = Model capacity (in TiB) * Initial usable capacity rate
                # print(f" mod {mod} max {max(group_ec_capacity_matrix_list)} list {group_ec_capacity_matrix_list} init {initial_size_rate} drr {data_reduction_ratio}")
                usable_cap_limit = max(group_ec_capacity_matrix_list) * initial_size_rate * data_reduction_ratio

                # Get the disk sizes (for virtual drives) for this model
                # disk_sizes = ec_config.get(mod).get("disk_sizes")

                # get array count for fist min-size sku in list (there will alwasy be at least one)
                # print(f"error 7 {group_ec_licensed_capacity} {data_reduction_ratio}")

                total_array_count = (group_ec_licensed_capacity + usable_cap_limit - 1) // usable_cap_limit
                # print(f"group {g} tot array count {total_array_count}")
                if total_array_count < 1:
                    total_array_count = 1

                # Calculate how much capacity is required for all raw disk across all arrays for this group
                raw_vd_disk_capacity = group_ec_licensed_capacity / (total_array_count * data_reduction_ratio)

                # find the smallest drive from the disk size list that is bigger then the size needed to meet this capacity
                eligible_sizes = [num for num in group_ec_capacity_matrix_list if num >= raw_vd_disk_capacity]
                # print(eligible_sizes)
                # If the filtered list is not empty, return the minimum value that is greater then the required size
                if eligible_sizes:
                    per_array_capacity = min(eligible_sizes)
                    max_raw_vd_disk_size = group_ec_capacity_matrix[str(per_array_capacity)]
                else:
                    per_array_capacity = max(group_ec_capacity_matrix_list)
                    max_raw_vd_disk_size = group_ec_capacity_matrix[str(per_array_capacity)]

                # Calculate the VD drives capacity cost
                vd_capacity_cost_per_month = vd_cap_monthly_price * int(
                    disk_count) * max_raw_vd_disk_size * total_array_count

                # Calculate the VD drives iop cost
                vd_iops_cost_per_month = vd_iops_monthly_price * (ec_config.get(mod).get("VdIOPS") - 3000) * int(
                    disk_count) * total_array_count

                # Calculate the VD Drives bw cost
                vd_bw_cost_per_month = vd_bw_monthly_price * (ec_config.get(mod).get("VdBW") - 125) * int(
                    disk_count) * total_array_count

                total_ec_iops = total_array_count * ec_config.get(mod).get("usable_limit_iops")
                if (total_ec_iops * ec_config.get(mod).get("iops_over_provision_rate")) < group_tot_iops_all:
                    if not ignore_iops_provisioned_flag:
                        iops_viable = "low iops"
                    else:
                        iops_viable = "ignore low iops"
                else:
                    iops_viable = "default"
                if not iops_viable == "low iops":
                    ec_tot_cost = (ec_license_cost_per_gib * group_ec_licensed_capacity) + (group_fixed_cost[mod] * total_array_count) + vd_capacity_cost_per_month + vd_iops_cost_per_month + vd_bw_cost_per_month
                    # if g ==1 or g =="1":
                    #    print("ec_tot_cost",ec_tot_cost, "iops", group_tot_iops_all )
                    # 04/13 MR - adding check for max number of volumes
                    if max_volumes_per_array_flag and (group_tot_volumes_all > (max_volumes_per_array * total_array_count)):
                        # print(f"adjust for volume count 1 {max_volumes_per_array} {(max_volumes_per_array * total_array_count)} volumes {group_tot_volumes_all} old array count {total_array_count}")

                        new_array_vol_count = -(-group_tot_volumes_all // (max_volumes_per_array))
                        # print(f"new array count {new_array_vol_count}")
                        total_array_count = new_array_vol_count
                        iops_viable = iops_viable + " volume adjustment"

                    array_costs.append({"group_id": g,
                                        "sku": mod,
                                        "sku_index": sku_index,
                                        "min_sku_index": 0,
                                        "year": 1,
                                        "iops_viable": iops_viable,
                                        "ec_tot_cost": ec_tot_cost,
                                        "azure_native_cost": native_cost_tot,
                                        "license_cost": (ec_license_cost_per_gib * group_ec_licensed_capacity),
                                        "ec_capacity_size": group_ec_licensed_capacity,
                                        "number_of_arrays": total_array_count,
                                        "fixed_cost_per_month": (group_fixed_cost[mod] * total_array_count),
                                        "vd_capacity_cost_per_month": vd_capacity_cost_per_month,
                                        "vd_iops_cost_per_month": vd_iops_cost_per_month,
                                        "vd_bw_cost_per_month": vd_bw_cost_per_month,
                                        "instance_type": mod,
                                        "vd_num": disk_count,
                                        "vd_disk_size": max_raw_vd_disk_size,
                                        "number_of_compute": group_tot_compute_all,
                                        "number_of_volumes": group_tot_volumes_all,
                                        "ec_iops": total_ec_iops,
                                        "lc": group_ec_licensed_capacity,
                                        "ti": group_tot_iops_all,
                                        "tv": group_tot_volumes_all,
                                        "region": reg
                                        })
                    if g == 222 or g == "222":
                        tmp = {"group_id": g,
                               "sku": mod,
                               "sku_index": sku_index,
                               "min_sku_index": 0,
                               "year": 1,
                               "iops_viable": iops_viable,
                               "ec_tot_cost": ec_tot_cost,
                               "azure_native_cost": native_cost_tot,
                               "license_cost": (ec_license_cost_per_gib * group_ec_licensed_capacity),
                               "ec_capacity_size": group_ec_licensed_capacity,
                               "number_of_arrays": total_array_count,
                               "fixed_cost_per_month": (group_fixed_cost[mod] * total_array_count),
                               "vd_capacity_cost_per_month": vd_capacity_cost_per_month,
                               "vd_iops_cost_per_month": vd_iops_cost_per_month,
                               "vd_bw_cost_per_month": vd_bw_cost_per_month,
                               "instance_type": mod,
                               "vd_num": disk_count,
                               "vd_disk_size": max_raw_vd_disk_size,
                               "number_of_compute": group_tot_compute_all,
                               "number_of_volumes": group_tot_volumes_all,
                               "ec_iops": total_ec_iops,
                               "lc": group_ec_licensed_capacity,
                               "ti": group_tot_iops_all,
                               "tv": group_tot_volumes_all,
                               "region": reg
                               }
                        # print(f"rofft1 {g}, {sku}, {min_sku_list}, {mod} {iops_viable} run {run_count} {tmp}")
                else:
                    iops_viable = "iops tuned"
                    # print("in hanna")
                    # Calculate how many more IOPs were required
                    missing_iops = (group_tot_iops_all - (
                            total_ec_iops * ec_config.get(mod).get("iops_over_provision_rate")))
                    # translate ec iops to overprovisioned iops
                    iops_increment = ec_config.get(mod).get("iops_over_provision_rate") * ec_config.get(
                        mod).get("usable_limit_iops")
                    # Calculate how many more ec arrays are required to meet the iops requirements using math floor as a ceiling divsion operation
                    missing_arrays = (missing_iops + iops_increment - 1) // iops_increment
                    # update total array count
                    total_array_count = total_array_count + missing_arrays

                    # 04/13 MR - adding check for max number of volumes
                    if max_volumes_per_array_flag and (group_tot_volumes_all > (max_volumes_per_array * total_array_count)):
                        # print(f"adjust for volume count 2{max_volumes_per_array} {(max_volumes_per_array * total_array_count)} volumes {group_tot_volumes_all} old array count {total_array_count}")

                        new_array_vol_count = -(-group_tot_volumes_all // (max_volumes_per_array))
                        # print(f"new array count {new_array_vol_count}")
                        total_array_count = new_array_vol_count
                        iops_viable = iops_viable + " volume adjustment"

                    total_ec_iops = total_array_count * ec_config.get(mod).get("usable_limit_iops")
                    # For IOPs consideration only use the maximum disk count size
                    disk_count = ec_config.get(mod).get("vd_count")

                    # Calculate how much capacity is required for all raw disk across all arrays for this group
                    raw_vd_disk_capacity = group_ec_licensed_capacity / (total_array_count * data_reduction_ratio)
                    # print("raw_vd_disk_capacity = ", raw_vd_disk_capacity)

                    # find the smallest drive from the disk size list that is bigger then the size needed to meet this capacity
                    eligible_sizes = [num for num in group_ec_capacity_matrix_list if num >= raw_vd_disk_capacity]
                    # print(eligible_sizes)
                    # If the filtered list is not empty, return the minimum value that is greater then the required size
                    if eligible_sizes:
                        per_array_capacity = min(eligible_sizes)
                        # print(per_array_capacity, "per array cap")
                        max_raw_vd_disk_size = group_ec_capacity_matrix[str(per_array_capacity)]
                    else:
                        per_array_capacity = max(group_ec_capacity_matrix_list)
                        # print(per_array_capacity, "per array cap 2")
                        max_raw_vd_disk_size = group_ec_capacity_matrix[str(per_array_capacity)]
                    # print("max_raw_vd_disk_size = ", max_raw_vd_disk_size, g)
                    # Calculate the VD drives capacity cost
                    vd_capacity_cost_per_month = vd_cap_monthly_price * int(
                        disk_count) * max_raw_vd_disk_size * total_array_count

                    # Calculate the VD drives iop cost
                    vd_iops_cost_per_month = vd_iops_monthly_price * (
                            ec_config.get(mod).get("VdIOPS") - 3000) * int(disk_count) * total_array_count

                    # Calculate the VD Drives bw cost
                    vd_bw_cost_per_month = vd_bw_monthly_price * (ec_config.get(mod).get("VdBW") - 125) * int(
                        disk_count) * total_array_count
                    ec_tot_cost = (ec_license_cost_per_gib * group_ec_licensed_capacity) + (group_fixed_cost[mod] * total_array_count) + vd_capacity_cost_per_month + vd_iops_cost_per_month + vd_bw_cost_per_month

                    array_costs.append({"group_id": g,
                                        "sku": mod,
                                        "sku_index": sku_index,
                                        "min_sku_index": 0,
                                        "year": 1,
                                        "iops_viable": iops_viable,
                                        "ec_tot_cost": ec_tot_cost,
                                        "azure_native_cost": native_cost_tot,
                                        "license_cost": (ec_license_cost_per_gib * group_ec_licensed_capacity),
                                        "ec_capacity_size": group_ec_licensed_capacity,
                                        "number_of_arrays": total_array_count,
                                        "fixed_cost_per_month": (group_fixed_cost[mod] * total_array_count),
                                        "vd_capacity_cost_per_month": vd_capacity_cost_per_month,
                                        "vd_iops_cost_per_month": vd_iops_cost_per_month,
                                        "vd_bw_cost_per_month": vd_bw_cost_per_month,
                                        "instance_type": mod,
                                        "vd_num": disk_count,
                                        "vd_disk_size": max_raw_vd_disk_size,
                                        "number_of_compute": group_tot_compute_all,
                                        "number_of_volumes": group_tot_volumes_all,
                                        "ec_iops": total_ec_iops,
                                        "lc": group_ec_licensed_capacity,
                                        "ti": group_tot_iops_all,
                                        "tv": group_tot_volumes_all,
                                        "region": reg
                                        })
                    if g == 222 or g == "222":
                        pass
                        # print(f"rofft2 {g}, {sku}, {min_sku_index} {min_sku_list}, {mod} {iops_viable}")
                if run_count > 0:
                    min_sku_index = 1
                    for mod2 in additional_skus_for_this_run:
                        group_prov_cap_all = group_prov_cap[mod2]
                        # 04/06 MR Option to ingore all snaphot costs if change rate is zero
                        if float(monthly_snapshot_rate) == 0:
                            group_ec_licensed_capacity = (group_prov_cap_all * efficiency)
                        else:
                            group_ec_licensed_capacity = (group_prov_cap_all * efficiency) * (1 + (monthly_snapshot_rate / 2))
                        group_ec_capacity_matrix = ec_config[mod]["raw_cap_matrix"]
                        # print("no error here: ", ec_config[mod]["raw_cap_matrix"])
                        # group_ec_capacity_matrix_str = ec_config[mod]["raw_cap_matrix"].apply( lambda row_list: [key for d in row_list for key in d.keys()]).tolist()
                        group_ec_capacity_matrix_str = ec_config[mod]["raw_cap_matrix"].keys()
                        group_ec_capacity_matrix_list = [int(x) for x in group_ec_capacity_matrix_str]

                        # Calculate the Initial Usable capacity limit for this SKU model usable_capacity = Model capacity (in TiB) * Initial usable capacity rate
                        usable_cap_limit = int(ec_config.get(mod2).get("usable_limit_capacity")) * initial_size_rate

                        # Get the disk sizes (for virtual drives) for this model
                        disk_sizes = ec_config.get(mod2).get("disk_sizes")

                        # get array count for fist min-size sku in list (there will alwasy be at least one)

                        total_array_count = (group_ec_licensed_capacity + usable_cap_limit - 1) // usable_cap_limit
                        total_ec_iops = total_array_count * ec_config.get(mod2).get("usable_limit_iops")
                        # print(f"vlurp for {mod} min-sku {mod2} iops {group_tot_iops[mod2]} array count {total_array_count}")
                        if (total_ec_iops * ec_config.get(mod2).get("iops_over_provision_rate")) < group_tot_iops[mod2]:
                            if not ignore_iops_provisioned_flag:
                                iops_viable = "low iops"
                            else:
                                iops_viable = "ignore low iops"
                        else:
                            iops_viable = "default"
                        if not iops_viable == "low iops":

                            group_ec_capacity_matrix = ec_config[mod2]["raw_cap_matrix"]
                            # print("no error here: ", ec_config[mod2]["raw_cap_matrix"])
                            # group_ec_capacity_matrix_str = ec_config[mod2]["raw_cap_matrix"].apply(lambda row_list: [key for d in row_list for key in d.keys()]).tolist()
                            group_ec_capacity_matrix_str = ec_config[mod2]["raw_cap_matrix"].keys()
                            group_ec_capacity_matrix_list = [int(x) for x in group_ec_capacity_matrix_str]

                            # Calculate the Initial Usable capacity limit for this SKU model usable_capacity = Model capacity (in TiB) * Initial usable capacity rate
                            usable_cap_limit = max(group_ec_capacity_matrix_list) * initial_size_rate * data_reduction_ratio

                            # Get the disk sizes (for virtual drives) for this model
                            # disk_sizes = ec_config.get(mod).get("disk_sizes")

                            # get array count for fist min-size sku in list (there will alwasy be at least one)

                            total_array_count = (group_ec_licensed_capacity + usable_cap_limit - 1) // usable_cap_limit
                            if total_array_count < 1:
                                total_array_count = 1

                            # 04/13 MR - adding check for max number of volumes
                            if max_volumes_per_array_flag and (group_tot_volumes_all > (max_volumes_per_array * total_array_count)):
                                # print(f"adjust for volume count 4{max_volumes_per_array} {(max_volumes_per_array * total_array_count)} volumes {group_tot_volumes_all} old array count {total_array_count}")

                                new_array_vol_count = -(-group_tot_volumes_all // (max_volumes_per_array))
                                # print(f"new array count {new_array_vol_count}")
                                total_array_count = new_array_vol_count
                                # print(f"new array count {total_array_count}")
                                iops_viable = iops_viable + " volume adjustment"

                            # Calculate how much capacity is required for all raw disk across all arrays for this group
                            raw_vd_disk_capacity = group_ec_licensed_capacity / (total_array_count * data_reduction_ratio)

                            # find the smallest drive from the disk size list that is bigger then the size needed to meet this capacity
                            eligible_sizes = [num for num in group_ec_capacity_matrix_list if num >= raw_vd_disk_capacity]
                            # print(eligible_sizes)
                            # If the filtered list is not empty, return the minimum value that is greater then the required size
                            if eligible_sizes:
                                per_array_capacity = min(eligible_sizes)
                                max_raw_vd_disk_size = group_ec_capacity_matrix[str(per_array_capacity)]
                            else:
                                per_array_capacity = max(group_ec_capacity_matrix_list)
                                max_raw_vd_disk_size = group_ec_capacity_matrix[str(per_array_capacity)]

                            # Calculate the VD drives capacity cost
                            vd_capacity_cost_per_month = vd_cap_monthly_price * int(
                                disk_count) * max_raw_vd_disk_size * total_array_count

                            # Calculate the VD drives iop cost
                            vd_iops_cost_per_month = vd_iops_monthly_price * (
                                    ec_config.get(mod2).get("VdIOPS") - 3000) * int(
                                disk_count) * total_array_count

                            # Calculate the VD Drives bw cost
                            vd_bw_cost_per_month = vd_bw_monthly_price * (
                                    ec_config.get(mod2).get("VdBW") - 125) * int(disk_count) * total_array_count
                            ec_tot_cost = (ec_license_cost_per_gib * group_ec_licensed_capacity) + (group_fixed_cost[mod2] * total_array_count) + vd_capacity_cost_per_month + vd_iops_cost_per_month + vd_bw_cost_per_month

                            array_costs.append({"group_id": g,
                                                "sku": mod,
                                                "sku_index": sku_index,
                                                "min_sku_index": min_sku_index,
                                                "year": 1,
                                                "iops_viable": iops_viable,
                                                "ec_tot_cost": ec_tot_cost,
                                                "azure_native_cost": 0,
                                                "license_cost": (ec_license_cost_per_gib * group_ec_licensed_capacity),
                                                "ec_capacity_size": group_ec_licensed_capacity,
                                                "number_of_arrays": total_array_count,
                                                "fixed_cost_per_month": (group_fixed_cost[mod2] * total_array_count),
                                                "vd_capacity_cost_per_month": vd_capacity_cost_per_month,
                                                "vd_iops_cost_per_month": vd_iops_cost_per_month,
                                                "vd_bw_cost_per_month": vd_bw_cost_per_month,
                                                "instance_type": mod2,
                                                "vd_num": disk_count,
                                                "vd_disk_size": max_raw_vd_disk_size,
                                                "number_of_compute": group_tot_compute[mod2],
                                                "number_of_volumes": group_tot_volumes[mod2],
                                                "ec_iops": total_ec_iops,
                                                "lc": group_ec_licensed_capacity,
                                                "ti": group_tot_iops[mod2],
                                                "tv": group_tot_volumes[mod2],
                                                "region": reg
                                                })
                            if g == 222 or g == "222":
                                pass
                                # print(f"rofft4 {g}, {sku}, {min_sku_index}, {additional_skus_for_this_run} {mod} mod2 {mod2} {iops_viable}")
                        else:

                            iops_viable = "iops tuned"
                            # print("in claude")
                            # Calculate how many more IOPs were required
                            missing_iops = (group_tot_iops[mod2] - (
                                    total_ec_iops * ec_config.get(mod2).get("iops_over_provision_rate")))
                            # translate ec iops to overprovisioned iops
                            iops_increment = ec_config.get(mod).get("iops_over_provision_rate") * ec_config.get(
                                mod2).get("usable_limit_iops")
                            # Calculate how many more ec arrays are required to meet the iops requirements using math floor as a ceiling divsion operation
                            missing_arrays = (missing_iops + iops_increment - 1) // iops_increment
                            # update total array count
                            total_array_count = total_array_count + missing_arrays

                            if max_volumes_per_array_flag and (group_tot_volumes_all > (max_volumes_per_array * total_array_count)):
                                # print(f"adjust for volume count 5{max_volumes_per_array} {(max_volumes_per_array * total_array_count)} volumes {group_tot_volumes_all} old array count {total_array_count}")

                                new_array_vol_count = -(-group_tot_volumes_all // (max_volumes_per_array))
                                # print(f"new array count {new_array_vol_count}")
                                total_array_count = new_array_vol_count
                                # print(f"new array count {total_array_count}")
                                iops_viable = iops_viable + " volume adjustment"

                            total_ec_iops = total_array_count * ec_config.get(mod2).get("usable_limit_iops")
                            # For IOPs consideration only use the maximum disk count size
                            disk_count = ec_config.get(mod).get("vd_count")
                            # Calculate how much capacity is required for each raw disk across all arrays for this group

                            # Calculate how much capacity is required for all raw disk across all arrays for this group
                            raw_vd_disk_capacity = group_ec_licensed_capacity / (total_array_count * data_reduction_ratio)

                            # find the smallest drive from the disk size list that is bigger then the size needed to meet this capacity
                            eligible_sizes = [num for num in group_ec_capacity_matrix_list if num >= raw_vd_disk_capacity]
                            # print(eligible_sizes)
                            # If the filtered list is not empty, return the minimum value that is greater then the required size
                            if eligible_sizes:
                                per_array_capacity = min(eligible_sizes)
                                max_raw_vd_disk_size = group_ec_capacity_matrix[str(per_array_capacity)]
                            else:
                                per_array_capacity = max(group_ec_capacity_matrix_list)
                                max_raw_vd_disk_size = group_ec_capacity_matrix[str(per_array_capacity)]

                            # Calculate the VD drives capacity cost
                            vd_capacity_cost_per_month = vd_cap_monthly_price * int(
                                disk_count) * max_raw_vd_disk_size * total_array_count

                            # Calculate the VD drives iop cost
                            vd_iops_cost_per_month = vd_iops_monthly_price * (ec_config.get(mod).get("VdIOPS") - 3000) * int(disk_count) * total_array_count

                            # Calculate the VD Drives bw cost
                            vd_bw_cost_per_month = vd_bw_monthly_price * (ec_config.get(mod).get("VdBW") - 125) * int(disk_count) * total_array_count
                            ec_tot_cost = (ec_license_cost_per_gib * group_ec_licensed_capacity) + (group_fixed_cost[mod2] * total_array_count) + vd_capacity_cost_per_month + vd_iops_cost_per_month + vd_bw_cost_per_month

                            # 04/13 MR - adding check for max number of volumes

                            array_costs.append({"group_id": g,
                                                "sku": mod,
                                                "sku_index": sku_index,
                                                "min_sku_index": min_sku_index,
                                                "year": 1,
                                                "iops_viable": iops_viable,
                                                "ec_tot_cost": ec_tot_cost,
                                                "azure_native_cost": 0,
                                                "license_cost": (ec_license_cost_per_gib * group_ec_licensed_capacity),
                                                "ec_capacity_size": group_ec_licensed_capacity,
                                                "number_of_arrays": total_array_count,
                                                "fixed_cost_per_month": (group_fixed_cost[mod2] * total_array_count),
                                                "vd_capacity_cost_per_month": vd_capacity_cost_per_month,
                                                "vd_iops_cost_per_month": vd_iops_cost_per_month,
                                                "vd_bw_cost_per_month": vd_bw_cost_per_month,
                                                "instance_type": mod2,
                                                "vd_num": disk_count,
                                                "vd_disk_size": max_raw_vd_disk_size,
                                                "number_of_compute": group_tot_compute_all,
                                                "number_of_volumes": group_tot_volumes_all,
                                                "ec_iops": total_ec_iops,
                                                "lc": group_ec_licensed_capacity,
                                                "ti": group_tot_iops_all,
                                                "tv": group_tot_volumes_all,
                                                "region": reg
                                                })

                            if g == 222 or g == "222":
                                pass
                                # print(f"rofft5 {g}, {sku}, {min_sku_index}, {mod} {iops_viable}")
                        min_sku_index = min_sku_index + 1

                sku_index = sku_index + 1

    cost_sheet, df_groups = calc_best_ec_config(array_costs, ec_config, ec_sku_bias, group_list, df_parsed, 1)
    # print(f"cost_sheet: {cost_sheet}")
    # print(f"df_groups shape: {df_groups.shape}")

    # Build one summary row per group (shape of the original `fc` template) and
    # persist both the cost sheet and the group summary as CSVs in S3, in a
    # `tco/` subprefix alongside the parsed data file.
    group_rows = []
    saved_prefix = None
    if cost_sheet:
        for cs in cost_sheet:
            g = cs["group_id"]
            sku = cs["sku"]
            # Matching per-SKU cost row for this group (year 1)
            dfg = df_groups[(df_groups["group_id"] == g) &
                            (df_groups["sku_index"] == cs["sku_index"]) &
                            (df_groups["year"] == 1)]
            if len(dfg) == 0:
                print(f"warning: no df_groups row for group {g} sku {sku}; skipping summary row")
                continue
            row = dfg.iloc[0]

            # Per-group provisioned capacity from the parsed data
            orig_cap = df_parsed.loc[df_parsed["group_id"] == g, disk_size].sum()

            # Account / availability zone / vnet for this group (constant within a
            # group; "—" if the column is not present in the parsed data).
            def _group_attr(col):
                if col and col in df_parsed.columns:
                    vals = df_parsed.loc[df_parsed["group_id"] == g, col].unique()
                    if len(vals):
                        return vals[0]
                return "—"

            savings = cs.get("Y1 Savings", 0)
            azure_native = cs.get("Y1 Azure Native Cost", 0)
            save_ratio = (savings / azure_native) if azure_native else 0

            # Azure Managed Disk cost breakdown for this group (from the parsed data:
            # capacity, performance = provisioned IOPS + throughput, and snapshots).
            def _grp_cost_sum(col):
                if col in df_parsed.columns:
                    return float(pd.to_numeric(df_parsed.loc[df_parsed["group_id"] == g, col],
                                               errors="coerce").fillna(0).sum())
                return 0.0
            azure_md_cap  = _grp_cost_sum("cap_cost")
            azure_md_perf = _grp_cost_sum("iops_cost") + _grp_cost_sum("mbps_cost")
            azure_md_snap = _grp_cost_sum("snap_cost")

            group_rows.append({
                "desc": g,
                "Region": row["region"],
                "Account": _group_attr(account),
                "Availability Zone": _group_attr(zone),
                "VNet": _group_attr(vnet),
                "EC Config": sku,
                "Original Capacity": orig_cap,
                "Y1 Save Ratio": save_ratio,
                "Y1 PSC Lic $": row["license_cost"],
                "Y1 PSC Res $": row["ec_tot_cost"] - row["license_cost"],
                "Y1 PSC Tot $": row["ec_tot_cost"],
                "Y1 Azure Native $": row["azure_native_cost"],
                "Y1 Azure MD Capacity $": azure_md_cap,
                "Y1 Azure MD Performance $": azure_md_perf,
                "Y1 Azure MD Snapshots $": azure_md_snap,
                "Y1 PSC Licensed Capacity": row["ec_capacity_size"],
                "Y1 PSC Array Count": row["number_of_arrays"],
                "Azure Paid Capacity": orig_cap,
                "Num Compute": cs.get("tc", 0),
                "Num Volumes": int((df_parsed["group_id"] == g).sum()),
            })

        # Subprefix token: sum of drr + growth + license cost, plus a datetime stamp
        if save_outputs:
            stamp = datetime.now().strftime("%Y%m%d%H%M%S")
            token = f"{(data_reduction_ratio + growth_rate + ec_license_cost_per_gib):.4f}"
            prefix = f"{s3_path}/tco/{token}_{stamp}/"
            saved_prefix = prefix
            upload_df_to_s3(pd.DataFrame(cost_sheet), "cost_sheet.csv", prefix)
            upload_df_to_s3(pd.DataFrame(df_groups), "df_groups.csv", prefix)
            upload_df_to_s3(pd.DataFrame(group_rows), "group_summary.csv", prefix)

            # Persist a user-supplied description, the stamp, and the scalar run
            # parameters alongside the run so the TCO Review tab can label it and
            # show the parameters on hover. Skip large/derived structures.
            try:
                skip_keys = {"ec_data", "source_data_config", "region_list", "description"}
                meta_params = {k: v for k, v in params.items()
                               if k not in skip_keys and not isinstance(v, (dict, list))}
                meta = {
                    "description": str(params.get("description", "")).strip(),
                    "generated": stamp,
                    "params": meta_params,
                }
                _s3_client().put_object(
                    Bucket=s3_bucket,
                    Key=f"{prefix}meta.json",
                    Body=json.dumps(meta).encode("utf-8"),
                    ContentType="application/json",
                )
            except Exception as exc:
                print(f"Warning: could not write tco meta.json: {exc}")

    return {"group_rows": group_rows, "cost_sheet": cost_sheet, "df_groups": df_groups, "prefix": saved_prefix}

def tco_by_group_azure_native(params, df_parsed, ecan_config, s3_path, save_outputs=True):
    """Azure Native (ECAN) TCO — a much simpler cost model than the Dedicated
    engine (tco_by_group_y1). It does NOT look up any infrastructure pricing.
    Per group the Everpure cost is:

        everpure_total = capacity_cost + throughput_cost

      capacity_cost   = effective_capacity * capacity_rate
                        effective_capacity = original_capacity * efficiency
                        capacity_rate      = capacity_cost_normal  (DRR >= min_drr_normal)
                                             capacity_cost_encrypt (DRR <  min_drr_normal)
      throughput_cost = num_arrays * array_mbps * per_mbps_cost_per_array

    Array count is the greater of:
      - IOPS-driven: ceil(required_iops / usable_limit_iops)
      - capacity-driven: ceil(effective_capacity / (max_raw_capacity_size_GiB * DRR))
    The per-array MBps is the smallest tier in `mbps_sku_iops` whose IOPS (and
    MBps) capacity covers the per-array load.

    Output mirrors tco_by_group_y1 (group_rows / cost_sheet / df_groups / prefix)
    so the TCO Review tab consumes it identically. The whole Everpure cost is
    reported as licensing (Y1 PSC Lic $) with zero reserved/infra cost, so the
    commercial discount + partner margin in the TCO Review apply to all of it."""
    efficiency   = float(params.get("efficiency", 0.65))
    drr          = float(params.get("drr", 2.0))
    # Estimated DRR is used ONLY to size the maximum capacity of an array
    # (max_raw_capacity_size_GiB * estimated_drr). It does not affect the capacity
    # rate, which is selected by the DRR bucket (drr) above.
    est_drr      = float(params.get("azn_estimated_drr", 4.0) or 4.0)
    # Independent sizing toggles:
    #  - consider IOPS (Consider IOPS = On): include the IOPS-driven array count.
    #  - consider throughput (Consider Throughput = On): include the MBps-driven
    #    array count and size each array's MBps tier to the load; when Off, MBps is
    #    ignored for array count and every array is billed at the largest tier.
    ignore_iops         = bool(params.get("ignore_iops_provisioned", False))
    consider_iops       = not ignore_iops
    consider_throughput = bool(params.get("consider_throughput", True))
    growth_rate  = float(params.get("growth", 0.2) or 0)
    # Monthly snapshot rate uplifts the effective (licensed/billed) capacity, mirroring
    # the Dedicated engine (tco_by_group_y1): licensed capacity = eff_cap * (1 + rate/2).
    # rate == 0 -> factor 1.0 (no snapshot capacity).
    snap_rate    = float(params.get("monthly_snapshot_rate", 0.0) or 0.0)

    if not ecan_config:
        print("Azure Native: ecan_config is empty — cannot size arrays or price capacity.")
        return {"group_rows": [], "cost_sheet": [], "df_groups": pd.DataFrame(), "prefix": None}

    # ECAN config is keyed by SKU (e.g. {"V20AZN": {...}}). Use the first entry.
    ecan_sku = list(ecan_config.keys())[0]
    cfg = ecan_config[ecan_sku]
    usable_limit_iops = float(cfg.get("usable_limit_iops", 150000)) or 150000
    minimum_capacity  = float(cfg.get("minimum_capacity", 0) or 0)
    # Cost mode: prices live under cfg["cost_mode"][<mode>]. The selected mode comes
    # from params["azn_cost_mode"]; if unset/unknown, default to the lowest-cost mode.
    # Falls back to top-level keys for older configs without cost_mode.
    cost_modes = cfg.get("cost_mode") or {}
    def _mode_total(m):
        return (float(m.get("capacity_cost_normal", 0) or 0)
                + float(m.get("capacity_cost_encrypt", 0) or 0)
                + float(m.get("per_mbps_cost_per_array", 0) or 0))
    selected_cost_mode = str(params.get("azn_cost_mode", "") or "")
    if cost_modes:
        if selected_cost_mode not in cost_modes:
            selected_cost_mode = min(cost_modes, key=lambda k: _mode_total(cost_modes[k]))
        mp = cost_modes[selected_cost_mode]
    else:
        selected_cost_mode = "default"
        mp = cfg
    cap_cost_normal   = float(mp.get("capacity_cost_normal", 0) or 0)
    cap_cost_encrypt  = float(mp.get("capacity_cost_encrypt", 0) or 0)
    per_mbps_cost     = float(mp.get("per_mbps_cost_per_array", 0) or 0)
    min_drr_normal    = float(cfg.get("min_drr_normal", 2.0))
    max_raw           = float(cfg.get("max_raw_capacity_size_GiB", 0) or 0)
    supported_regions = cfg.get("azure_supported_regions", []) or []
    mbps_sku_iops     = cfg.get("mbps_sku_iops", {}) or {}
    # Over-provisioning rates: the measured (provisioned) IOPS/MBps from the source
    # data are right-sized down by these before determining how many arrays are
    # needed (default 1.0 = no adjustment).
    op_iops_rate      = float(cfg.get("overprovisioned_iops_rate", 1.0) or 1.0)
    op_mbps_rate      = float(cfg.get("overprovisioned_mbps_rate", 1.0) or 1.0)

    # MBps tiers sorted ascending by IOPS capacity: [(mbps, iops), ...]
    tiers = sorted(((float(m), float(i)) for m, i in mbps_sku_iops.items()),
                   key=lambda t: t[1])
    if not tiers:
        tiers = [(0.0, usable_limit_iops)]
    max_tier_mbps = tiers[-1][0]

    # Capacity rate is selected by the DRR bucket (below vs. at/above min_drr_normal).
    cap_rate = cap_cost_normal if drr >= min_drr_normal else cap_cost_encrypt

    # ── Resolve source-data column names (same scheme as tco_by_group_y1) ──
    source_data_config = params.get("source_data_config", {}) or {}
    col_names = df_parsed.columns.tolist()

    def _col(field):
        idx = int(source_data_config.get(field, -99))
        return None if idx == -99 else col_names[idx]

    # IOPS/MBps: when unmapped (-99), main2 creates placeholder "no_iops"/"no_mbps"
    # columns populated with tier-based estimates (matching tco_by_group_y1, which
    # reads those placeholders). Use them so Azure Native sees the same demand.
    def _perf_col(field, placeholder):
        idx = int(source_data_config.get(field, -99))
        return placeholder if idx == -99 else col_names[idx]

    region_col  = _col("region")
    disk_size   = col_names[int(source_data_config.get("disk_size", -99))]
    iops_col    = _perf_col("iops", "no_iops")
    mbps_col    = _perf_col("mbps", "no_mbps")
    compute_col = _col("count_compute")
    zone_col    = _col("zone")
    account_col = _col("subscription_or_account_id")
    vnet_col    = _col("vnet_or_vpc")
    if "true_az" in col_names:
        zone_col = "true_az"

    def _gsum(g, col):
        if col and col in df_parsed.columns:
            return float(pd.to_numeric(
                df_parsed.loc[df_parsed["group_id"] == g, col],
                errors="coerce").fillna(0).sum())
        return 0.0

    def _gattr(g, col):
        if col and col in df_parsed.columns:
            vals = df_parsed.loc[df_parsed["group_id"] == g, col].unique()
            if len(vals):
                return vals[0]
        return "—"

    group_list = df_parsed["group_id"].unique()
    group_rows = []
    cost_sheet = []
    dfg_rows   = []

    for g in group_list:
        # Region (constant within a group). Skip groups in regions the Azure
        # Native SKU does not support (unless no region column is present).
        reg = "no_region"
        if region_col and region_col in df_parsed.columns:
            reg_vals = df_parsed.loc[df_parsed["group_id"] == g, region_col].unique()
            reg = reg_vals[0] if len(reg_vals) else "no_region"
            if supported_regions and reg not in supported_regions:
                print(f"Azure Native: region {reg} not supported for {ecan_sku}; skipping group {g}")
                continue

        orig_cap    = _gsum(g, disk_size)
        req_iops    = _gsum(g, iops_col)
        req_mbps    = _gsum(g, mbps_col)
        num_compute = _gsum(g, compute_col)
        azure_native = _gsum(g, "total_cost")

        # Effective capacity includes the snapshot uplift (1 + snap_rate/2), matching
        # the Dedicated engine so the snapshot-rate control affects Azure Native too.
        eff_cap = orig_cap * efficiency * (1 + snap_rate / 2)
        # Minimum billable capacity: any group below minimum_capacity is billed at
        # minimum_capacity, and flagged so the condition is visible in the results.
        min_cap_applied = eff_cap < minimum_capacity
        billed_cap = max(eff_cap, minimum_capacity)

        # Right-size the measured (over-provisioned) IOPS/MBps down by the
        # over-provision rates: a single array serves usable_limit_iops of IOPS and
        # max_tier_mbps of throughput.
        eff_iops = (req_iops / op_iops_rate) if op_iops_rate else req_iops
        eff_mbps = (req_mbps / op_mbps_rate) if op_mbps_rate else req_mbps

        # Array count: greatest of IOPS-, throughput-, and capacity-driven counts.
        # The per-array max capacity uses the estimated DRR (not the rate DRR bucket).
        arrays_by_iops = max(1, math.ceil(eff_iops / usable_limit_iops)) if eff_iops > 0 else 1
        arrays_by_mbps = max(1, math.ceil(eff_mbps / max_tier_mbps)) if (eff_mbps > 0 and max_tier_mbps) else 1
        cap_per_array  = max_raw * est_drr
        arrays_by_cap  = max(1, math.ceil(billed_cap / cap_per_array)) if cap_per_array > 0 else 1

        # Array count: capacity always counts; IOPS and throughput count only when
        # their respective toggles are on.
        drivers = [arrays_by_cap]
        if consider_iops:       drivers.append(arrays_by_iops)
        if consider_throughput: drivers.append(arrays_by_mbps)
        num_arrays = max(drivers)

        if not consider_throughput:
            # Throughput ignored for sizing — bill every array at the largest
            # (default) MBps tier.
            array_mbps = max_tier_mbps
        else:
            # Per-array MBps: smallest tier whose MBps (and IOPS, if considered)
            # covers the per-array right-sized load; fall back to the largest tier.
            per_array_iops = eff_iops / num_arrays if num_arrays else 0
            per_array_mbps = eff_mbps / num_arrays if num_arrays else 0
            array_mbps = max_tier_mbps
            for m, i in tiers:
                if m >= per_array_mbps and (not consider_iops or i >= per_array_iops):
                    array_mbps = m
                    break

        capacity_cost   = billed_cap * cap_rate
        throughput_cost = num_arrays * array_mbps * per_mbps_cost
        everpure_total  = capacity_cost + throughput_cost

        savings    = azure_native - everpure_total
        save_ratio = (savings / azure_native) if azure_native else 0

        group_rows.append({
            "desc": g,
            "Region": reg,
            "Account": _gattr(g, account_col),
            "Availability Zone": _gattr(g, zone_col),
            "VNet": _gattr(g, vnet_col),
            "EC Config": ecan_sku,
            "Original Capacity": orig_cap,
            "Y1 Save Ratio": save_ratio,
            "Y1 PSC Lic $": everpure_total,   # whole Everpure cost is discountable
            "Y1 PSC Res $": 0,                # no infrastructure cost in Azure Native
            "Y1 PSC Tot $": everpure_total,
            "Y1 Azure Native $": azure_native,
            # Azure Managed Disk cost breakdown (from parsed data): capacity,
            # performance (provisioned IOPS + throughput), and snapshots.
            "Y1 Azure MD Capacity $": _gsum(g, "cap_cost"),
            "Y1 Azure MD Performance $": _gsum(g, "iops_cost") + _gsum(g, "mbps_cost"),
            "Y1 Azure MD Snapshots $": _gsum(g, "snap_cost"),
            "Y1 PSC Licensed Capacity": billed_cap,
            "Y1 PSC Array Count": num_arrays,
            "Azure Paid Capacity": orig_cap,
            "Num Compute": num_compute,
            "Num Volumes": int((df_parsed["group_id"] == g).sum()),
            "Y1 Capacity $": capacity_cost,
            "Y1 Throughput $": throughput_cost,
            "Min Capacity Applied": "Yes" if min_cap_applied else "No",
        })

        cost_sheet.append({
            "group_id": g, "sku": ecan_sku, "sku_index": 0,
            "Y1 Azure Native Cost": azure_native, "Y1 Savings": savings, "tc": num_compute,
            "capacity_cost": round(capacity_cost, 2),
            "throughput_cost": round(throughput_cost, 2),
            "everpure_total": round(everpure_total, 2),
            "num_arrays": num_arrays, "array_mbps": array_mbps,
            "req_iops": req_iops, "req_mbps": req_mbps,
            "effective_capacity": round(eff_cap, 2),
            "billed_capacity": round(billed_cap, 2),
            "min_capacity_applied": bool(min_cap_applied),
        })

        dfg_rows.append({
            "group_id": g, "sku": ecan_sku, "sku_index": 0, "year": 1, "region": reg,
            "license_cost": everpure_total, "ec_tot_cost": everpure_total,
            "azure_native_cost": azure_native, "ec_capacity_size": billed_cap,
            "number_of_arrays": num_arrays,
            "capacity_cost": round(capacity_cost, 2),
            "throughput_cost": round(throughput_cost, 2),
            "capacity_rate": cap_rate, "array_mbps": array_mbps,
            "cost_mode": selected_cost_mode,
            "req_iops": req_iops, "req_mbps": req_mbps,
            "eff_iops": round(eff_iops, 1), "eff_mbps": round(eff_mbps, 1),
            "overprovisioned_iops_rate": op_iops_rate, "overprovisioned_mbps_rate": op_mbps_rate,
            "arrays_by_iops": arrays_by_iops, "arrays_by_mbps": arrays_by_mbps,
            "arrays_by_cap": arrays_by_cap,
            "consider_iops": bool(consider_iops), "consider_throughput": bool(consider_throughput),
            "drr": drr, "estimated_drr": est_drr, "efficiency": efficiency,
            "original_capacity": orig_cap, "effective_capacity": round(eff_cap, 2),
            "min_capacity_applied": bool(min_cap_applied),
            "number_of_compute": num_compute,
        })

    df_groups = pd.DataFrame(dfg_rows)
    # print(f"Azure Native cost_sheet: {len(cost_sheet)} groups; df_groups shape: {df_groups.shape}")

    saved_prefix = None
    if save_outputs and group_rows:
        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        token = f"{(drr + growth_rate + cap_rate):.4f}"
        prefix = f"{s3_path}/tco/azn_{token}_{stamp}/"
        saved_prefix = prefix
        upload_df_to_s3(pd.DataFrame(cost_sheet), "cost_sheet.csv", prefix)
        upload_df_to_s3(df_groups, "df_groups.csv", prefix)
        upload_df_to_s3(pd.DataFrame(group_rows), "group_summary.csv", prefix)
        try:
            skip_keys = {"ec_data", "source_data_config", "region_list",
                         "description", "ecan_config"}
            meta_params = {k: v for k, v in params.items()
                           if k not in skip_keys and not isinstance(v, (dict, list))}
            meta_params["method"] = "azure_native"
            meta = {
                "description": str(params.get("description", "")).strip(),
                "generated": stamp,
                "params": meta_params,
            }
            _s3_client().put_object(
                Bucket=s3_bucket,
                Key=f"{prefix}meta.json",
                Body=json.dumps(meta).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as exc:
            print(f"Warning: could not write azure-native tco meta.json: {exc}")

    return {"group_rows": group_rows, "cost_sheet": cost_sheet,
            "df_groups": df_groups, "prefix": saved_prefix}

def tco_by_group_growth(params, df_parsed, df_azure_disk, df_ec_infra, s3_path, growth):
    """Like tco_by_group_y1, but projects cost after applying a capacity growth
    factor to each group's ORIGINAL capacity. Scales the disk_size column (which
    drives the whole Everpure re-sizing) and the precomputed Azure "total_cost"
    column (so the Azure-native side grows proportionally too), re-runs the engine
    without saving, and returns aggregate Everpure/Azure/savings totals."""
    factor = 1.0 + float(growth or 0)
    dfg = df_parsed.copy()
    # Resolve the capacity column exactly as the engine does
    sdc = params.get("source_data_config", {}) or {}
    col_names = dfg.columns.tolist()
    disk_size = col_names[_cfg_int(sdc, "disk_size")]
    if disk_size in dfg.columns:
        dfg[disk_size] = pd.to_numeric(dfg[disk_size], errors="coerce").fillna(0) * factor
    if "total_cost" in dfg.columns:  # Azure native cost — grow proportionally with capacity
        dfg["total_cost"] = pd.to_numeric(dfg["total_cost"], errors="coerce").fillna(0) * factor

    method = str(params.get("method", "dedicated")).strip().lower()
    # Suppress the engine's verbose per-run debug prints during projection
    with open(os.devnull, "w") as _dn, contextlib.redirect_stdout(_dn):
        if method == "azure_native":
            res = tco_by_group_azure_native(params, dfg, params.get("ecan_config", {}),
                                            s3_path, save_outputs=False)
        else:
            res = tco_by_group_y1(params, dfg, df_azure_disk, df_ec_infra, s3_path, save_outputs=False)
    rows = res.get("group_rows", []) if isinstance(res, dict) else []
    groups = []
    for r in rows:
        license = float(r.get("Y1 PSC Lic $", 0) or 0)   # Everpure licensing (discount/margin apply here)
        infra   = float(r.get("Y1 PSC Res $", 0) or 0)   # Everpure infrastructure
        az      = float(r.get("Y1 Azure Native $", 0) or 0)
        lc      = float(r.get("Y1 PSC Licensed Capacity", 0) or 0)
        cap     = float(r.get("Y1 Capacity $", 0) or 0)    # Azure Native: capacity portion
        thr     = float(r.get("Y1 Throughput $", 0) or 0)  # Azure Native: throughput portion
        groups.append({"group": r.get("desc"), "license": license, "infra": infra,
                       "azure_native": az, "lic_cap": lc, "cap": cap, "thr": thr})
    return {"groups": groups}

def project_growth_over_time(params, df_parsed, df_azure_disk, df_ec_infra, s3_path, yearly_growth, frequency):
    """Step capacity growth across the horizon on a monthly/quarterly/yearly cycle
    and collect the projected Everpure vs Azure-native cost at each period.

    Growth is simple/linear: cumulative growth at period t = yearly_growth * t/ppy,
    so after one full year the capacity has grown by exactly yearly_growth."""
    ppy = {"monthly": 12, "quarterly": 4, "yearly": 1}.get(frequency, 1)
    years = int(params.get("num_of_years", 5) or 5)
    total = years * ppy
    prefix = {"monthly": "M", "quarterly": "Q", "yearly": "Y"}.get(frequency, "P")
    # Store PER-GROUP results per period so the TCO Review tab can dynamically
    # aggregate over whichever groups are currently included (the aggregation and
    # savings math is done client-side against the review's include/exclude set).
    # Periods are 1-based: period 1 is the original (ungrown) numbers, and growth
    # accrues from there. A 5-year horizon therefore has 5 periods (1..5), not 6.
    periods = []
    for t in range(0, total):
        growth = yearly_growth * (t / ppy)          # t=0 -> original; grows from there
        r = tco_by_group_growth(params, df_parsed, df_azure_disk, df_ec_infra, s3_path, growth)
        gmap = {}
        for g in r["groups"]:
            gmap[str(g["group"])] = {
                "lic":   round(g["license"], 2),      # Everpure licensing (raw, pre-discount)
                "infra": round(g["infra"], 2),        # Everpure infrastructure
                "az":    round(g["azure_native"], 2), # Azure native (raw, pre-discount)
                "lc":    round(g["lic_cap"], 2),      # EC licensed capacity
                "cap":   round(g.get("cap", 0), 2),   # Azure Native: capacity portion
                "thr":   round(g.get("thr", 0), 2),   # Azure Native: throughput portion
            }
        periods.append({
            "period":     t + 1,
            "label":      f"{prefix}{t + 1}",
            "growth_pct": round(growth * 100, 2),
            "groups":     gmap,
        })
    return {
        "frequency":        frequency,
        "periods_per_year": ppy,
        "years":            years,
        "yearly_growth":    yearly_growth,
        "method":           str(params.get("method", "dedicated")).strip().lower(),
        "periods":          periods,
    }

def calc_best_ec_config(array_costs,ec_config, ec_sku_bias,group_list,df_csv,years):

    df_groups = pd.DataFrame(array_costs)
    # print("in trackka", ec_sku_bias)
    cost_sheet = []
    its = []
    if years == 1:
        ya = 1
        yb = 2
    else:
        ya = 2
        yb = years + 1
    for y in range(ya,yb):
        #print(f"in best onfig {y}")
        year_column_name_prefix = f"Y{y}"
        for g in sorted(group_list):
            its = []
            # first first sku in this group because azure cost does not change with sku type
            skus_for_g = df_groups.loc[
                    (df_groups['group_id'] == g) &
                    (df_groups['year'] == y), "sku"].tolist()
            if not skus_for_g:
                # Group produced no viable EC configuration during sizing
                # (unsupported region / no models / no capacity) — skip it.
                print(f"skipping group {g}: no viable EC configuration")
                continue
            first_sku = sorted(skus_for_g)[0]
            azure_native_tot_cost = max(df_groups.loc[
                    (df_groups['group_id'] == g) &
                    (df_groups['year'] == y), "azure_native_cost"].unique().tolist())
            if g == 222 or g == "222":
                pass
                # print("bokka 5.1, ", azure_native_tot_cost)
            #print(f"azure native cost for group {g} {azure_native_tot_cost}")
            current_best_savings = -999999
            current_best_sku = None
            current_best_sku_index = None
            its_value = []
            group_done = False
            # print("native ",g, " cost ", native_cost_tot)
            # Number of configurations for each group to be evaluated
            sku_indexs = sorted(df_groups.loc[
                (df_groups['group_id'] == g) &
                (df_groups['year'] == y), "sku_index"].unique())
            # For year 1 get a list of skus for this group
            sku_types_in_this_group = sorted(df_groups.loc[
                (df_groups['group_id'] == g) &
                (df_groups['year'] == y), "sku"].astype(str).unique())
            if g == 222 or g == "222":
                pass
                #print("bokka 5, ", azure_native_tot_cost)
                # print(f"sku types for group {g} : {sku_types_in_this_group} {sku_indexs} {ec_sku_bias}")
            # if processing year one and the bias sku is in this group, evaluate that one first.

            if ec_sku_bias in sku_types_in_this_group:
                # print(f"in bias {ec_sku_bias} {y}")
                # Remove this sku index from later consideration
                sku_index_for_bias = df_groups.loc[
                    (df_groups['group_id'] == g) &
                    (df_groups['sku'] == ec_sku_bias) &
                    (df_groups['year'] == y), "sku_index"].tolist()[0]
                if g == 222 or g == "222":
                    pass
                    # print(type(sku_index_for_bias), sku_index_for_bias,g)
                sku_indexs.remove(sku_index_for_bias)


                ec_total_cost = df_groups.loc[
                    (df_groups['group_id'] == g) &
                    (df_groups['year'] == y) &
                    (df_groups['sku'] == ec_sku_bias), "ec_tot_cost"].sum()
                #print(f"ec_total_cost for group {g} {ec_total_cost}")

                savings = azure_native_tot_cost - ec_total_cost

                items_in_sku_set = df_groups.loc[
                    (df_groups['group_id'] == g) &
                    (df_groups['year'] == y) &
                    (df_groups['sku_index'] == sku_index_for_bias) &
                    (df_groups['sku'] == ec_sku_bias), "min_sku_index"].unique().tolist()
                #print(f"num of items in sku set for group {g} {len(items_in_sku_set)}")
                tc = 0
                tv = 0
                if y == 1:
                    # Since this is year one save all of the array information for every array type in this configuration for mulit-year evaluation
                    tc = df_groups.loc[
                            (df_groups['group_id'] == g) &
                            (df_groups['year'] == y) &
                            (df_groups['sku_index'] == sku_index_for_bias), "number_of_compute"].unique().tolist()[0]
                    tv = df_groups.loc[
                        (df_groups['group_id'] == g) &
                        (df_groups['year'] == y) &
                        (df_groups['sku_index'] == sku_index_for_bias), "tv"].unique().tolist()[0]
                    add_first = True
                    for item in items_in_sku_set:
                        # add total cost to first array type in this configuration flag

                        instance_type = df_groups.loc[
                            (df_groups['group_id'] == g) &
                            (df_groups['year'] == y) &
                            (df_groups['sku_index'] == sku_index_for_bias) &
                            (df_groups["min_sku_index"] == item), "instance_type"].unique().tolist()[0]
                        lc = df_groups.loc[
                            (df_groups['group_id'] == g) &
                            (df_groups['year'] == y) &
                            (df_groups['sku_index'] == sku_index_for_bias) &
                            (df_groups["min_sku_index"] == item), "lc"].unique().tolist()[0]
                        ti = df_groups.loc[
                            (df_groups['group_id'] == g) &
                            (df_groups['year'] == y) &
                            (df_groups['sku_index'] == sku_index_for_bias) &
                            (df_groups["min_sku_index"] == item), "ti"].unique().tolist()[0]
                        reg = df_groups.loc[
                            (df_groups['group_id'] == g) &
                            (df_groups['year'] == y) &
                            (df_groups['sku_index'] == sku_index_for_bias) &
                            (df_groups["min_sku_index"] == item), "region"].unique().tolist()[0]
                        if add_first:
                            it = {"g": g, "i": sku_index_for_bias, "min_sku_index": item, "instance_type": instance_type, "lc": lc, "ti": ti, "tv": tv, "tc": ec_total_cost, "region": reg}
                            add_first = False
                        else:
                            it = {"g": g, "i": sku_index_for_bias, "min_sku_index": item, "instance_type": instance_type, "lc": lc, "ti": ti, "tv": tv, "tc": 0, "region": reg}
                        its.append(it)
                if savings > 0 and len(items_in_sku_set) == 1:
                    lc_list = df_groups.loc[
                        (df_groups['group_id'] == g) &
                        (df_groups['year'] == y) &
                        (df_groups['sku_index'] == current_best_sku_index), "lc"].unique().tolist()
                    ti_list = df_groups.loc[
                        (df_groups['group_id'] == g) &
                        (df_groups['year'] == y) &
                        (df_groups['sku_index'] == current_best_sku_index), "ti"].unique().tolist()
                    if len(lc_list) > 0:
                        lc = lc_list[0]
                    else:
                        lc = 0
                    if len(ti_list) > 0:
                        ti = ti_list[0]
                    else:
                        ti = 0
                    final_group_value = {
                        "group_id": g,
                        "sku_index": sku_index_for_bias,
                        "sku": ec_sku_bias,
                        "year": y,
                        f"{year_column_name_prefix} Savings": savings,
                        f"{year_column_name_prefix} Azure Native Cost": azure_native_tot_cost,
                        "viable": "good",
                        "lc": lc,
                        "ti": ti,
                        "tc": tc,
                        "tv": tv,
                        "its": its,
                    }
                    cost_sheet.append(final_group_value)
                    group_done = True
                else:
                    current_best_sku_index = sku_index_for_bias
                    current_best_sku = ec_sku_bias
                    current_best_savings = savings
                    its_value = its
            if not group_done:
                if g == 222 or g == "222":
                    pass
                    # print("bokka 7, ", sku_indexs)
                for si in sku_indexs:
                    its = []
                    #print(f" not in bias {si} {y} ")
                    sku_name = df_groups.loc[
                            (df_groups['group_id'] == g) &
                            (df_groups['year'] == y) &
                            (df_groups['sku_index'] == si), "sku"].unique().tolist()[0]
                    ec_total_cost = df_groups.loc[
                        (df_groups['group_id'] == g) &
                        (df_groups['year'] == y) &
                        (df_groups['sku_index'] == si), "ec_tot_cost"].sum()
                    if y == 1:

                        # Get values needed for multi-year evaluations
                        items_in_sku_set = df_groups.loc[
                            (df_groups['group_id'] == g) &
                            (df_groups['year'] == y) &
                            (df_groups['iops_viable'] != "low iops") &
                            (df_groups['sku_index'] == si), "min_sku_index"].unique().tolist()
                        if g == 222 or g == "222":
                            pass
                            # print("bokka 18, ", items_in_sku_set)
                        add_first = True
                        if g == 222 or g == "222":
                            pass
                            # print("bokka 2.5, ", items_in_sku_set, sku_indexs)
                        for item in items_in_sku_set:
                            # add total cost to first array type in this configuration flag

                            instance_type = df_groups.loc[
                                (df_groups['group_id'] == g) &
                                (df_groups['year'] == y) &
                                (df_groups['sku_index'] == si) &
                                (df_groups["min_sku_index"] == item), "instance_type"].unique().tolist()[0]
                            lc = df_groups.loc[
                                (df_groups['group_id'] == g) &
                                (df_groups['year'] == y) &
                                (df_groups['sku_index'] == si) &
                                (df_groups["min_sku_index"] == item), "lc"].unique().tolist()[0]
                            ti = df_groups.loc[
                                (df_groups['group_id'] == g) &
                                (df_groups['year'] == y) &
                                (df_groups['sku_index'] == si) &
                                (df_groups["min_sku_index"] == item), "ti"].unique().tolist()[0]
                            tv = df_groups.loc[
                                (df_groups['group_id'] == g) &
                                (df_groups['year'] == y) &
                                (df_groups['sku_index'] == si) &
                                (df_groups["min_sku_index"] == item), "tv"].unique().tolist()[0]
                            reg = df_groups.loc[
                                (df_groups['group_id'] == g) &
                                (df_groups['year'] == y) &
                                (df_groups['sku_index'] == si) &
                                (df_groups["min_sku_index"] == item), "region"].unique().tolist()[0]
                            if add_first:
                                it = {"g": g, "i": si, "min_sku_index": item, "instance_type": instance_type, "lc": lc, "ti": ti, "tv": tv, "tc": ec_total_cost, "region": reg}
                                add_first = False
                            else:
                                it = {"g": g, "i": si, "min_sku_index": item, "instance_type": instance_type, "lc": lc, "ti": ti, "tv": tv, "tc": 0, "region": reg}
                            its.append(it)
                            if g == 222 or g == "222":
                                pass
                                # print("bokka 2, ", it, items_in_sku_set, sku_indexs)

                    savings = azure_native_tot_cost - ec_total_cost
                    if g == 222 or g == "222":
                        pass
                        # print("bokka 8, ", sku_name, ec_total_cost, savings, current_best_savings, y, si)
                    #print(f" eeert savings: {savings}")
                    if savings > current_best_savings:
                        current_best_sku = sku_name
                        current_best_savings = savings
                        current_best_sku_index = si
                        its_value = its
                        if g == 222 or g == "222":
                            pass
                            # print("bokka 3, ",its_value, savings)
                    else:
                        if g == 222 or g == "222":
                            pass
                            # print("bokka 4, ",its_value, savings)


                if current_best_savings > 0:
                    viable = "good"
                else:
                    viable = f"loss in year {y}"
                lc_list =  df_groups.loc[
                    (df_groups['group_id'] == g) &
                    (df_groups['year'] == y) &
                    (df_groups['sku_index'] == current_best_sku_index), "lc"].unique().tolist()
                ti_list =  df_groups.loc[
                    (df_groups['group_id'] == g) &
                    (df_groups['year'] == y) &
                    (df_groups['sku_index'] == current_best_sku_index), "ti"].unique().tolist()
                if len(lc_list) > 0:
                    lc = lc_list[0]
                else:
                    lc = 0
                if len(ti_list) > 0:
                    ti = ti_list[0]
                else:
                    ti = 0
                tc = 0
                tv = 0
                if y == 1:
                    tc = df_groups.loc[
                        (df_groups['group_id'] == g) &
                        (df_groups['year'] == y) &
                        (df_groups['sku_index'] == current_best_sku_index), "number_of_compute"].unique().tolist()[0]
                    tv = df_groups.loc[
                        (df_groups['group_id'] == g) &
                        (df_groups['year'] == y) &
                        (df_groups['sku_index'] == current_best_sku_index), "tv"].unique().tolist()[0]

                final_group_value = {
                    "group_id": g,
                    "sku_index": current_best_sku_index,
                    "sku": current_best_sku,
                    "year": y,
                    f"{year_column_name_prefix} Savings": current_best_savings,
                    f"{year_column_name_prefix} Azure Native Cost": azure_native_tot_cost,
                    "viable": viable,
                    "lc": lc,
                    "ti": ti,
                    "tc": tc,
                    "tv": tv,
                    "its": its_value
                }
                if len(its_value) == 0 and (g == 222 or g == "222"):
                    pass
                    # print("boka boka",final_group_value, years, ec_sku_bias)

                cost_sheet.append(final_group_value)

    return cost_sheet, df_groups

#########  START OF MAIN ##############



# ── Credentials ───────────────────────────────────────────
# Demo credentials — swap for a real auth store / env vars in prod
VALID_USERS = {
    "admin": "password123",
    "demo":  "demo",
}

# ── S3 Configuration ──────────────────────────────────────
try:
    iam_data   = read_compressed_json(os.environ.get("AWS_ARCH_FILE", r'C:\Users\micha\aws.arch'))
    aws_key    = iam_data['aws_access_key_id']
    aws_secret = iam_data['aws_secret_access_key']
except Exception:
    aws_key    = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")

PRESIGN_EXPIRY   = int(os.environ.get("PRESIGN_EXPIRY_SECONDS", 300))
MAX_UPLOAD_BYTES = 99 * 1024 * 1024

# ── Customer list S3 key ───────────────────────────────────
CUSTOMER_LIST_S3_KEY = "TCO-GUI/_config/customer_list.json"

EC_CONFIG_KEY = "TCO-GUI/_config/ec_config.json"

# Azure Native (ECAN) config — same _config prefix as the EC config.
ECAN_CONFIG_KEY = "TCO-GUI/_config/ecan_config.json"

ALLOWED_EXTENSIONS = {
    "csv", "CSV",
}

# ── JSON data store (in-memory; replace with DB if needed) ─
_json_data_store = {}

s3_region = 'us-east-1'
s3_bucket = '980182764859-virg-bucket'
config_data_flag = False
config_data = {}

disk_type = None
mbps = None
iops = None
disk_size = None
zone = None
az_mapping = {}
in_region_mapping = []
cross_region_mapping = []
disk_usage_string = None
other = None
a_name = None
other2_column_name = None
parse_network_name_list = []
os_disk_device_list = []
default_zone_id = None
efficiency = None



if __name__ == "__main__":
    # Host/port/debug are env-configurable so the same entrypoint works locally and
    # in a container (bind 0.0.0.0 there). For production, run behind a WSGI server
    # (the container uses waitress) instead of Flask's dev server.
    host  = os.environ.get("HOST", "127.0.0.1")
    port  = int(os.environ.get("PORT", "5000"))
    debug = os.environ.get("FLASK_DEBUG", "1").lower() in ("1", "true", "yes")
    app.run(host=host, port=port, debug=debug)