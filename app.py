import os  
import json  
import base64  
import threading  
import datetime  
import uuid  
import traceback  
import re  
import io  
import mimetypes  
import time  
from concurrent.futures import ThreadPoolExecutor, as_completed  
from urllib.parse import quote, unquote, urlparse  
  
import bleach  
import certifi  
import markdown2  
from flask import (  
    Flask,  
    request,  
    render_template,  
    redirect,  
    url_for,  
    session,  
    flash,  
    send_file,  
    Response,  
    abort,  
    g,  
)  
from flask_session import Session  
from werkzeug.utils import secure_filename  
  
from azure.core.pipeline.transport import RequestsTransport  
from azure.cosmos import CosmosClient  
from azure.identity import (  
    AzureCliCredential,  
    ManagedIdentityCredential,  
    get_bearer_token_provider,  
)  
from azure.search.documents import SearchClient  
from azure.storage.blob import (  
    BlobServiceClient,  
    generate_blob_sas,  
    BlobSasPermissions,  
)  
from openai import AzureOpenAI  
  
try:  
    from azure.search.documents.models import VectorizedQuery  
except Exception:  
    VectorizedQuery = None     

MODE_CONFIG = {  
    "qa": {  
        "model": "gpt-5.2",  
        "extra_args": {"reasoning_effort": "low"},  
        "system_message": """あなたは社内ナレッジベースの専門家です。ユーザーの質問には、最新かつ正確な情報を日本語で回答してください。  
• 必要な場合のみ箇条書きを使用。  
• 参照した箇所があるときは本文中に [n] 形式で番号を付け、本文の最後に下記形式でまとめる:Sources:[n] ファイル名／タイトル  
• 思考過程や感情表現は出力しない。  
• 数式を含む場合は LaTeX 記法で記述し、インライン数式は `$...$` 、ディスプレイ数式は `$$...$$` で囲んでください。  
  - `$$` の開始・終了はそれぞれ「単独行」に置いてください（例：開始行は `$$` だけ、終了行も `$$` だけ）。  
  - 数式（`$...$` / `$$...$$`）はコードブロック（```）やインデント（先頭の空白）内に入れないでください。  
• MathML や画像など他形式の数式表現は使用せず、コードブロック内ではなく通常の本文として記述してください。  
• 記号の意味を箇条書きで説明するときは、必ず同じ行に「- $t$：時間」のように書いてください（記号だけの行を作らない／次行に「：説明」を書かないでください。  
• 記号の意味を箇条書きで説明するときは、「- $F$ : 物体に働く合力」のようにインライン数式 `$...$` を文中に埋め込み、記号だけを1行のディスプレイ数式として単独で出力しないでください。
• コンテキスト内に【図・画像 chunk_id:xxx】が含まれている場合は、回答の最後にまとめて出力するのではなく、必ずその図について言及・解説している文章の直後（文脈に沿ったインライン）に `[Image: xxx]` の形式で挿入してください。""",  
    },  
    "reasoning": {  
        "model": "gpt-5.2",  
        "extra_args": {"reasoning_effort": "high"},  
        "system_message": """あなたは研究者向け AI リサーチアシスタントです。提供された社内文書と会話履歴を基に段階的に推論を行い、最終的な結論を導いてください。  
出力フォーマット:  
Step-by-step:  
  1) …  
  2) …  
Conclusion: …  
Sources:[n] ファイル名／タイトル  
• 推論の過程は公開するが冗長になり過ぎないこと。  
• 根拠が不足していると判断したら、その旨と追加で必要な情報を提示すること。  
• 数式を含む場合は LaTeX 記法で記述し、インライン数式は `$...$` 、ディスプレイ数式は `$$...$$` で囲んでください。  
  - `$$` の開始・終了はそれぞれ「単独行」に置いてください（例：開始行は `$$` だけ、終了行も `$$` だけ）。  
  - 数式（`$...$` / `$$...$$`）はコードブロック（```）やインデント（先頭の空白）内に入れないでください。  
• MathML や画像など他形式の数式表現は使用せず、コードブロック内ではなく通常の本文として記述してください。  
• 記号の意味を箇条書きで説明するときは、必ず同じ行に「- $t$：時間」のように書いてください（記号だけの行を作らない／次行に「：説明」を書かないでください。  
• 記号の意味を箇条書きで説明するときは、「- $F$ : 物体に働く合力」のようにインライン数式 `$...$` を文中に埋め込み、記号だけを1行のディスプレイ数式として単独で出力しないでください。
• コンテキスト内に【図・画像 chunk_id:xxx】が含まれている場合は、回答の最後にまとめて出力するのではなく、必ずその図について言及・解説している文章の直後（文脈に沿ったインライン）に `[Image: xxx]` の形式で挿入してください。""",  
    },  
    "programming": {  
        "model": "gpt-5.2",  
        "extra_args": {"reasoning_effort": "high"},  
        "system_message": """あなたはシニアソフトウェアエンジニアです。ルール：  
• ユーザーが明示的に要求しない限り、出力は日本語でなければなりません。  
• 言語タグ（```python など）を付けた適切なマークダウンフェンスで囲まれた実行可能なコードを提供してください。  
• 要点を説明する簡単な日本語のインラインコメントを追加してください。  
• 潜在的な安全性またはセキュリティ上のリスクがある場合は、コードブロックの後に「注意」という見出しを付けて指摘してください。""",  
    },  
}  
  
app = Flask(__name__)  

@app.template_filter('to_jst')
def to_jst_filter(iso_str):
    if not iso_str:
        return ""
    try:
        # 念のため末尾の 'Z' を '+00:00' に置換してパース
        dt = datetime.datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        # 日本時間 (UTC+9) に変換
        jst = datetime.timezone(datetime.timedelta(hours=9))
        dt_jst = dt.astimezone(jst)
        return dt_jst.strftime('%Y-%m-%d %H:%M')
    except Exception:
        # パース失敗時は元の文字列を簡易整形して返す
        return iso_str[:16].replace('T', ' ')

APP_ENV = os.getenv("APP_ENV", "prod").lower()  
IS_LOCAL = APP_ENV == "local"  
IS_PROD = APP_ENV == "prod"  
  
secret = os.getenv("FLASK_SECRET_KEY")  
if IS_PROD and not secret:  
    raise RuntimeError("FLASK_SECRET_KEY is required in production")  
  
app.secret_key = secret or "your-default-secret-key"  
app.config["SESSION_TYPE"] = "filesystem"  
app.config["SESSION_FILE_DIR"] = "/tmp/flask_session"  
app.config["SESSION_PERMANENT"] = False  
app.config.update(  
    SESSION_COOKIE_SECURE=IS_PROD,  
    SESSION_COOKIE_HTTPONLY=True,  
    SESSION_COOKIE_SAMESITE="Lax",  
)  
os.makedirs(app.config["SESSION_FILE_DIR"], exist_ok=True)  
Session(app)  
  
USE_SAS_LINKS = os.getenv("USE_SAS_LINKS", "true").lower() == "true"  
SAS_EXPIRY_MIN = int(os.getenv("SAS_EXPIRY_MIN", "15"))  
ALLOW_APP_PROXY_FALLBACK = os.getenv("ALLOW_APP_PROXY_FALLBACK", "false").lower() == "true"  
  
MAX_MULTIQUERY = int(os.getenv("MAX_MULTIQUERY", "4"))  
SEARCH_MAX_WORKERS = int(os.getenv("SEARCH_MAX_WORKERS", "12"))  
PER_SEARCH_TOP = int(os.getenv("PER_SEARCH_TOP", "30"))  
FUSED_TOP = int(os.getenv("FUSED_TOP", "15"))  
  
CHUNK_ID_FIELD = os.getenv("CHUNK_ID_FIELD", "chunk_id")  
SEARCH_TARGET_FIELDS = ["content", "title", "folder_name"]  
  
MAX_RAG_CHARS_PER_SOURCE = int(os.getenv("MAX_RAG_CHARS_PER_SOURCE", "10000"))  
DOC_FETCH_PAGE_SIZE = int(os.getenv("DOC_FETCH_PAGE_SIZE", "1000"))  
DOC_FETCH_MAX_CHUNKS = int(os.getenv("DOC_FETCH_MAX_CHUNKS", "5000"))  
MAX_RAG_FILES = int(os.getenv("MAX_RAG_FILES", "8"))  
MAX_SOURCE_IMAGES = int(os.getenv("MAX_SOURCE_IMAGES", "50"))  
MAX_ANSWER_IMAGES = int(os.getenv("MAX_ANSWER_IMAGES", "6"))  
  
MAX_WINDOWS_PER_FILE = int(os.getenv("MAX_WINDOWS_PER_FILE", "4"))  
MAX_TOTAL_RAG_CHARS_PER_FILE = int(os.getenv("MAX_TOTAL_RAG_CHARS_PER_FILE", "25000"))  
  
UPDATE_TIMESTAMP_ON_FEEDBACK = os.getenv("UPDATE_TIMESTAMP_ON_FEEDBACK", "false").lower() == "true"  
  
search_service_endpoint = (os.getenv("AZURE_SEARCH_ENDPOINT") or "").strip().rstrip("/")  
search_index_name = os.getenv("AZURE_SEARCH_INDEX_NAME", "index-test-v2")  
transport = RequestsTransport(verify=certifi.where())  
  
cosmos_endpoint = (os.getenv("AZURE_COSMOS_ENDPOINT") or "").strip()  
database_name = "chatdb"  
cosmos_container_name = "personalchats"  
  
main_container_name = os.getenv("MAIN_CONTAINER_NAME", "index-test")  
image_container_name = os.getenv("UPLOAD_IMAGE_CONTAINER_NAME", "chatgpt-image")  
kb_image_container_name = os.getenv("KB_IMAGE_CONTAINER_NAME", "index-test-image")  
  
lock = threading.Lock()  
  
  
def build_credential():  
    if IS_LOCAL:  
        return AzureCliCredential()  
    mi_client_id = os.getenv("AZURE_CLIENT_ID")  
    return ManagedIdentityCredential(client_id=mi_client_id) if mi_client_id else ManagedIdentityCredential()  
  
  
credential = build_credential()  
  
token_provider = get_bearer_token_provider(  
    credential,  
    "https://cognitiveservices.azure.com/.default",  
)  
  
print("APP_ENV =", APP_ENV)  
print("IS_LOCAL =", IS_LOCAL)  
print("AZURE_OPENAI_ENDPOINT =", (os.getenv("AZURE_OPENAI_ENDPOINT") or "").strip())  
print("AZURE_SEARCH_ENDPOINT =", search_service_endpoint)  
print("AZURE_SEARCH_INDEX_NAME =", search_index_name)  
print("Azure auth mode = Entra ID")  
  
client = AzureOpenAI(  
    api_version=(os.getenv("AZURE_OPENAI_API_VERSION") or "").strip(),  
    azure_endpoint=(os.getenv("AZURE_OPENAI_ENDPOINT") or "").strip().rstrip("/"),  
    azure_ad_token_provider=token_provider,  
)  
  
cosmos_container = None  
cosmos_client = None  
try:  
    cosmos_client = CosmosClient(cosmos_endpoint, credential=credential)  
    cosmos_container = (  
        cosmos_client.get_database_client(database_name)  
        .get_container_client(cosmos_container_name)  
    )  
    print("Cosmos DB: 初期化しました。")  
except Exception as e:  
    print("Cosmos DB 初期化エラー:", e)  
  
blob_service_client = None  
try:  
    blob_service_client = BlobServiceClient(  
        account_url=(os.getenv("AZURE_STORAGE_ACCOUNT_URL") or "").strip(),  
        credential=credential,  
    )  
except Exception as e:  
    print("Blob Storage 初期化エラー:", e)  
  
image_container_client = (  
    blob_service_client.get_container_client(image_container_name)  
    if blob_service_client  
    else None  
)  
  
  
@app.context_processor  
def inject_mode_config():  
    return dict(MODE_CONFIG=MODE_CONFIG)  
  
  
def get_authenticated_user():  
    if "user_id" in session and "user_name" in session:  
        return session["user_id"]  
  
    client_principal = request.headers.get("X-MS-CLIENT-PRINCIPAL")  
    if client_principal:  
        try:  
            decoded = base64.b64decode(client_principal).decode("utf-8")  
            user_data = json.loads(decoded)  
            user_id = None  
            user_name = None  
            if "claims" in user_data:  
                for claim in user_data["claims"]:  
                    if claim.get("typ") == "http://schemas.microsoft.com/identity/claims/objectidentifier":  
                        user_id = claim.get("val")  
                    if claim.get("typ") == "name":  
                        user_name = claim.get("val")  
            if user_id:  
                session["user_id"] = user_id  
            if user_name:  
                session["user_name"] = user_name  
            return user_id  
        except Exception as e:  
            print("Easy Auth ユーザー情報の取得エラー:", e)  
  
    if IS_LOCAL:  
        session["user_id"] = "localdev@example.com"  
        session["user_name"] = "localdev"  
        return session["user_id"]  
  
    return None  
  
  
@app.before_request  
def enforce_auth_all_routes():  
    user_id = get_authenticated_user()  
    if not user_id:  
        accept = request.headers.get("Accept", "")  
        login_url = "/.auth/login/aad?post_login_redirect_uri=" + quote(request.url, safe="")  
        if "text/html" in accept:  
            return redirect(login_url)  
        return Response("Unauthorized", status=401)  
    g.current_user = user_id  
  
  
ALLOWED_TAGS = [  
    "p",  
    "br",  
    "ul",  
    "ol",  
    "li",  
    "strong",  
    "em",  
    "code",  
    "pre",  
    "table",  
    "thead",  
    "tbody",  
    "tr",  
    "th",  
    "td",  
    "blockquote",  
    "a",
    "img",  
]  
ALLOWED_ATTRS = {  
    "a": ["href", "title", "target", "rel"],  
    "code": ["class"],  
    "pre": ["class"],
    "img": ["src", "alt", "class", "width", "height"], 
}  
ALLOWED_PROTOCOLS = ["http", "https", "mailto"]  
  
  
def sanitize_html(html: str) -> str:  
    return bleach.clean(  
        html,  
        tags=ALLOWED_TAGS,  
        attributes=ALLOWED_ATTRS,  
        protocols=ALLOWED_PROTOCOLS,  
        strip=True,  
        strip_comments=True,  
    )  
  
  
def validate_blob_path(blobname: str):  
    if ".." in blobname or blobname.startswith(("/", "\\")):  
        abort(400)  
    if not re.match(r"^[\w\-/\. ]+$", blobname):  
        abort(400)  
  
  
def validate_container_and_path(container: str, blobname: str):  
    if container != main_container_name:  
        abort(403)  
    validate_blob_path(blobname)  
  
  
_BAD_PERCENT_RE = re.compile(r"%(?![0-9A-Fa-f]{2})")  
  
  
def decode_percent_loose(s: str, rounds: int = 2) -> str:  
    if not s:  
        return ""  
    out = str(s)  
    for _ in range(max(1, rounds)):  
        repaired = _BAD_PERCENT_RE.sub("%25", out)  
        new = unquote(repaired)  
        if new == out:  
            out = new  
            break  
        out = new  
    return out  
  
  
def normalize_folder_for_ui(folder: str) -> str:  
    if not folder:  
        return ""  
    f = decode_percent_loose(folder).strip().strip("/")  
    if f.lower() == "root":  
        return ""  
    return f  
  
  
def generate_user_delegation_sas_url(  
    container: str,  
    blobname: str,  
    disposition: str = "attachment",  
) -> str | None:  
    if not blob_service_client:  
        return None  
  
    try:  
        start = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)  
        expiry = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=SAS_EXPIRY_MIN)  
  
        bc = blob_service_client.get_blob_client(container=container, blob=blobname)  
  
        filename = os.path.basename(blobname) or "download"  
        ascii_filename = secure_filename(filename) or "download"  
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"  
  
        disp = disposition if disposition in ("attachment", "inline") else "attachment"  
        content_disposition = (  
            f'{disp}; filename="{ascii_filename}"; '  
            f"filename*=UTF-8''{quote(filename)}"  
        )  
  
        udk = blob_service_client.get_user_delegation_key(start, expiry)  
        sas_token = generate_blob_sas(  
            account_name=bc.account_name,  
            container_name=container,  
            blob_name=blobname,  
            user_delegation_key=udk,  
            permission=BlobSasPermissions(read=True),  
            expiry=expiry,  
            start=start,  
            content_disposition=content_disposition,  
            content_type=content_type,  
        )  
  
        return f"{bc.url}?{sas_token}"  
    except Exception as e:  
        print("SAS発行失敗:", e)  
        traceback.print_exc()  
        return None  
  
  
def blob_exists(container: str, blobname: str) -> bool:  
    if not blob_service_client:  
        return False  
    try:  
        bc = blob_service_client.get_blob_client(container=container, blob=blobname)  
        return bool(bc.exists())  
    except Exception:  
        return False  
  
  
def build_doc_url(filepath: str, is_text: bool, fallback_url: str = "") -> str:  
    if USE_SAS_LINKS and filepath:  
        if blob_exists(main_container_name, filepath):  
            sas_url = generate_user_delegation_sas_url(  
                main_container_name,  
                filepath,  
                disposition="attachment",  
            )  
            if sas_url:  
                return sas_url  
        else:  
            print("[WARN] build_doc_url: blob not found:", main_container_name, filepath, "fallback:", fallback_url)  
  
    if filepath and ALLOW_APP_PROXY_FALLBACK:  
        if is_text:  
            return url_for("download_txt", container=main_container_name, blobname=filepath)  
        return url_for("download_blob", container=main_container_name, blobname=filepath)  
  
    return fallback_url or ""  
  
  
def extract_folder_from_blobpath(path: str) -> str:  
    if not path:  
        return ""  
    p = path.replace("\\", "/").strip("/")  
    if "/" not in p:  
        return ""  
    return p.rsplit("/", 1)[0]  
  
  
def extract_blob_path_from_result(result: dict) -> str:  
    u = (result.get("url") or "").strip()  
    if not u:  
        return ""  
    try:  
        p = (urlparse(u).path or "").lstrip("/")  
        if "/" not in p:  
            return ""  
        container, blob = p.split("/", 1)  
        if container != main_container_name:  
            return ""  
        return unquote(blob)  
    except Exception:  
        return ""  
  
  
def extract_folder_from_result(result: dict) -> str:  
    fn = (result.get("folder_name") or "").strip()  
    if fn:  
        return normalize_folder_for_ui(fn)  
    rel_path = extract_blob_path_from_result(result)  
    return normalize_folder_for_ui(extract_folder_from_blobpath(rel_path))  
  
  
def get_chunk_id_from_result(result: dict) -> str:  
    for k in (CHUNK_ID_FIELD, "chunk_id", "chunkId", "chunkID", "chunkid"):  
        v = result.get(k)  
        if isinstance(v, str) and v.strip():  
            return v.strip()  
    v = result.get(CHUNK_ID_FIELD)  
    if v is not None:  
        return str(v)  
    return ""  
  
  
def extract_container_blob_from_content_path(content_path: str) -> tuple[str, str]:
    cp = (content_path or "").strip()
    if not cp: return ("", "")
    
    # フルURL (https://...) が入っている場合に対応
    if cp.startswith("http"):
        try:
            parsed = urlparse(cp)
            # /container/blob... というパスから分割
            parts = parsed.path.lstrip("/").split("/", 1)
            if len(parts) == 2:
                return (parts[0], unquote(parts[1]))
        except: pass
            
    # 相対パス (container/blob) の場合
    cp = cp.lstrip("/")
    if "/" in cp:
        container, blob = cp.split("/", 1)
        return (container, unquote(blob))
    
    return ("", "")  
  
  
def build_kb_image_inline_url(content_path: str) -> str:  
    if not content_path or not USE_SAS_LINKS:  
        return ""  
    container, blob = extract_container_blob_from_content_path(content_path)  
    if not container or not blob:  
        return ""  
    if container != kb_image_container_name:  
        return ""  
    if not blob_exists(container, blob):  
        return ""  
    sas = generate_user_delegation_sas_url(container, blob, disposition="inline")  
    return sas or ""  
  
  
MATH_FENCE_RE = re.compile(r"```(?:latex|tex)?\s*(\$\$[\s\S]*?\$\$)\s*```", re.IGNORECASE)  
  
  
def _deindent_display_math_blocks(text: str) -> str:  
    lines = text.splitlines()  
    out = []  
    in_math = False  
    indent = ""  
    for line in lines:  
        if not in_math:  
            m = re.match(r"^(\s{4,})\$\$\s*$", line)  
            if m:  
                in_math = True  
                indent = m.group(1)  
                out.append("$$")  
            else:  
                out.append(line)  
        else:  
            if re.match(rf"^{re.escape(indent)}\$\$\s*$", line) or re.match(r"^\s*\$\$\s*$", line):  
                in_math = False  
                indent = ""  
                out.append("$$")  
                continue  
            if indent and line.startswith(indent):  
                out.append(line[len(indent):])  
            else:  
                out.append(line)  
    return "\n".join(out)  
  
  
def normalize_math_blocks(text: str) -> str:  
    if not text:  
        return text  
    text = MATH_FENCE_RE.sub(r"\1", text)  
    text = _deindent_display_math_blocks(text)  
    if text.count("$$") % 2 == 1:  
        text += "\n$$"  
    return text  
  
  
_SYMBOL_TOKEN = r"(?:\$(?:\\.|[^$])+\$|\\$.+?\\$|[A-Za-z][A-Za-z0-9_]*|[^\s]{1,12})"  
DEF_LIST_COLON_JOIN_RE = re.compile(  
    rf"(^\s*[-*•]\s+{_SYMBOL_TOKEN}\s*)\n\s*[:：]\s*(.+)$",  
    re.MULTILINE,  
)  
  
  
def normalize_symbol_definitions(text: str) -> str:  
    if not text:  
        return text  
    return DEF_LIST_COLON_JOIN_RE.sub(r"\1：\2", text)  
  
  
MARKDOWN_EXTRAS = [  
    "tables",  
    "fenced-code-blocks",  
    "code-friendly",  
    "break-on-newline",  
    "cuddled-lists",  
]  
  
  
def render_assistant_markdown_to_html(md_text: str) -> str:  
    md_text = md_text or ""  
    md_text = normalize_math_blocks(md_text)  
    md_text = normalize_symbol_definitions(md_text)  
    html = markdown2.markdown(md_text, extras=MARKDOWN_EXTRAS)  
    return sanitize_html(html)  
  
  
def strip_content_html_from_messages(messages):  
    out = []  
    if not isinstance(messages, list):  
        return out  
    for m in messages:  
        if isinstance(m, dict):  
            nm = dict(m)  
            nm.pop("content_html", None)  
            nm.pop("answer_images_display", None)  
            out.append(nm)  
        else:  
            out.append(m)  
    return out  
  
  
def append_system_message_history(chat_obj: dict, new_system_message: str):  
    if chat_obj is None:  
        return  
    new_system_message = (new_system_message or "").strip()  
    if not new_system_message:  
        return  
  
    hist = chat_obj.get("system_message_history")  
    if not isinstance(hist, list):  
        hist = []  
  
    last_content = None  
    if hist and isinstance(hist[-1], dict):  
        last_content = hist[-1].get("content")  
  
    if last_content == new_system_message:  
        chat_obj["system_message_history"] = hist  
        return  
  
    hist.append(  
        {  
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),  
            "content": new_system_message,  
        }  
    )  
    chat_obj["system_message_history"] = hist  
  
  
def ensure_chat_has_system_history(chat_obj: dict, fallback_system_message: str = ""):  
    if chat_obj is None:  
        return  
    if isinstance(chat_obj.get("system_message_history"), list) and chat_obj["system_message_history"]:  
        return  
  
    sys_msg = (chat_obj.get("system_message") or fallback_system_message or "").strip()  
    chat_obj["system_message_history"] = []  
    if sys_msg:  
        append_system_message_history(chat_obj, sys_msg)  
  
  
def ensure_chat_has_created_at(chat_obj: dict, fallback_timestamp: str = ""):  
    if chat_obj is None:  
        return  
    created = (chat_obj.get("created_at") or "").strip()  
    if created:  
        return  
    ts = (fallback_timestamp or chat_obj.get("timestamp") or "").strip()  
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()  
    chat_obj["created_at"] = ts or now  
  
  
def sync_current_chat_messages_to_sidebar():  
    sidebar = session.get("sidebar_messages", [])  
    idx = session.get("current_chat_index", 0)  
    if not (sidebar and 0 <= idx < len(sidebar)):  
        return  
  
    current = sidebar[idx]  
    current["messages"] = strip_content_html_from_messages(session.get("main_chat_messages", []))  
    ensure_chat_has_system_history(  
        current,  
        fallback_system_message=current.get(  
            "system_message",  
            session.get(  
                "default_system_message",  
                MODE_CONFIG[session.get("conversation_mode", "qa")]["system_message"],  
            ),  
        ),  
    )  
    ensure_chat_has_created_at(current, fallback_timestamp=current.get("timestamp", ""))  
    session["sidebar_messages"] = sidebar  
    session.modified = True  
  
  
def save_chat_history():  
    if cosmos_container is None:  
        return  
    with lock:  
        try:  
            sidebar = session.get("sidebar_messages", [])  
            idx = session.get("current_chat_index", 0)  
            if idx < len(sidebar):  
                current = sidebar[idx]  
                user_id = g.get("current_user") or get_authenticated_user()  
                if not user_id:  
                    return  
  
                user_name = session.get("user_name", "anonymous")  
                session_id = current.get("session_id")  
  
                ensure_chat_has_system_history(  
                    current,  
                    fallback_system_message=current.get(  
                        "system_message",  
                        session.get(  
                            "default_system_message",  
                            MODE_CONFIG[session.get("conversation_mode", "qa")]["system_message"],  
                        ),  
                    ),  
                )  
                ensure_chat_has_created_at(current)  
  
                now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()  
                current["timestamp"] = now_iso  
  
                messages_clean = strip_content_html_from_messages(current.get("messages", []))  
                
                if not messages_clean:
                    return

                item = {  
                    "id": session_id,  
                    "user_id": user_id,  
                    "user_name": user_name,  
                    "session_id": session_id,  
                    "messages": messages_clean,  
                    "system_message": current.get(  
                        "system_message",  
                        session.get(  
                            "default_system_message",  
                            MODE_CONFIG[session.get("conversation_mode", "qa")]["system_message"],  
                        ),  
                    ),  
                    "system_message_history": current.get("system_message_history", []),  
                    "first_assistant_message": current.get("first_assistant_message", ""),  
                    "created_at": current.get("created_at"),  
                    "timestamp": now_iso,  
                }  
                cosmos_container.upsert_item(item)  
        except Exception as e:  
            print(f"チャット履歴保存エラー: {e}")  
            traceback.print_exc()  
  
  
def load_chat_history():  
    if cosmos_container is None:  
        return []  
    with lock:  
        user_id = g.get("current_user") or get_authenticated_user()  
        sidebar_messages = []  
        if not user_id:  
            return sidebar_messages  
  
        try:  
            six_months_ago = (
                datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=180)
            ).isoformat()
            query = """
                SELECT TOP 500 * FROM c
                WHERE c.user_id = @user_id AND c.timestamp >= @six_months_ago
                ORDER BY c.timestamp DESC
            """
            parameters = [
                {"name": "@user_id", "value": user_id},
                {"name": "@six_months_ago", "value": six_months_ago},
            ] 
            items = cosmos_container.query_items(  
                query=query,  
                parameters=parameters,  
                partition_key=user_id,  
                enable_cross_partition_query=False,  
            )  
            for item in items:  
                if "session_id" in item:  
                    messages_clean = strip_content_html_from_messages(item.get("messages", []))  
                    
                    if not messages_clean:
                        continue
                    
                    chat = {  
                        "session_id": item["session_id"],  
                        "messages": messages_clean,  
                        "system_message": item.get(  
                            "system_message",  
                            session.get(  
                                "default_system_message",  
                                MODE_CONFIG[session.get("conversation_mode", "qa")]["system_message"],  
                            ),  
                        ),  
                        "system_message_history": item.get("system_message_history", []),  
                        "first_assistant_message": item.get("first_assistant_message", ""),  
                        "created_at": item.get("created_at", ""),  
                        "timestamp": item.get("timestamp", ""),  
                    }  
                    ensure_chat_has_system_history(chat, fallback_system_message=chat.get("system_message", ""))  
                    ensure_chat_has_created_at(chat, fallback_timestamp=chat.get("timestamp", ""))  
                    sidebar_messages.append(chat)  
        except Exception as e:  
            print("チャット履歴読み込みエラー:", e)  
            traceback.print_exc()  
  
        return sidebar_messages  
  
  
def start_new_chat():  
    image_filenames = session.get("image_filenames", [])  
    for img_name in image_filenames:  
        if image_container_client:  
            blob_client = image_container_client.get_blob_client(img_name)  
            try:  
                blob_client.delete_blob()  
            except Exception as e:  
                print("画像削除エラー:", e)  
    session["image_filenames"] = []  
  
    new_session_id = str(uuid.uuid4())  
  
    mode = session.get("conversation_mode", "qa")  
    mode_default = MODE_CONFIG[mode]["system_message"]  
    session["default_system_message"] = mode_default  
  
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()  
  
    new_chat = {  
        "session_id": new_session_id,  
        "messages": [],  
        "first_assistant_message": "",  
        "system_message": mode_default,  
        "system_message_history": [],  
        "created_at": now_iso,  
        "timestamp": now_iso,  
    }  
    append_system_message_history(new_chat, mode_default)  
    sidebar = session.get("sidebar_messages", [])
    sidebar = [c for c in sidebar if len(c.get("messages", [])) > 0]
    sidebar.insert(0, new_chat)  
    session["sidebar_messages"] = sidebar  
    session["current_chat_index"] = 0  
    session["main_chat_messages"] = []  
    session.modified = True  
  
    save_chat_history()  
  
  
def encode_image_from_blob(blob_client):  
    downloader = blob_client.download_blob()  
    image_data = downloader.readall()  
    return base64.b64encode(image_data).decode("utf-8")  
  
  
def rewrite_queries_for_search(messages, current_prompt):  
    max_query_history_turns = 3  
    filtered = [m for m in messages if m.get("role") in ("system", "user", "assistant")]  
    history = []  
    for m in filtered[-(max_query_history_turns * 3):]:  
        role = m.get("role")  
        content = m.get("content", "")  
        history.append(f"{role}:{content}")  
    context = "\n".join(history)  
  
    max_q = MAX_MULTIQUERY  
    system_prompt = (  
        "あなたは社内文書検索クエリ生成AIです。\n"  
        '出力仕様: {"queries":["...", "..."]} の JSON を 1 行だけ返す。\n'  
        "・各クエリは30文字以内、日本語主体、必要なら英語同義語併記。\n"  
        f"・最大{max_q}件生成。\n"  
        "・複数クエリを出すときは、互いにできるだけ異なる観点やキーワード構成にしてください。\n"  
        "・同じ単語を並べ替えただけ、助詞だけを変えただけなど、意味がほぼ同じクエリは生成しないでください。\n"  
        "・意味が大きく変わる候補が2〜3件しか思いつかない場合は、その件数だけで構いません。\n"  
        "・説明・前後の余分な文字・改行は禁止。"  
    )  
  
    user_prompt = (  
        f"history{{{context}}}\n"  
        f"endtask ユーザの意図をよく表す文書検索用クエリを最大{max_q}件生成してください。\n"  
        "可能であれば、表現や観点が少しずつ異なるクエリを含めてください。\n"  
        f"最新ユーザ質問: {current_prompt}"  
    )  
  
    t0 = time.perf_counter()  
    result = client.chat.completions.create(  
        model="gpt-5-mini",  
        messages=[  
            {"role": "system", "content": system_prompt},  
            {"role": "user", "content": user_prompt},  
        ],  
        max_completion_tokens=4096,  
        response_format={"type": "json_object"},  
        reasoning_effort="minimal",  
    )  
    t1 = time.perf_counter()  
    print(f"[TIMING] query rewrite (gpt-5-mini): {t1 - t0:.3f}s")  
  
    msg_content = result.choices[0].message.content  
    text = (msg_content or "").strip()  
    if not text:  
        return [current_prompt[:30]]  
  
    try:  
        queries_obj = json.loads(text)  
        if "queries" in queries_obj and isinstance(queries_obj["queries"], list):  
            queries = [q.strip() for q in queries_obj["queries"] if q.strip()]  
            queries = queries[:max_q]  
            if not queries:  
                queries = [current_prompt[:30]]  
            return queries  
        return [current_prompt[:30]]  
    except Exception as e:  
        print("rewrite_queries_for_search JSONパースエラー:", e, "text=", text)  
        traceback.print_exc()  
        return [current_prompt[:30]]  
  
  
def get_search_client(index_name):  
    return SearchClient(  
        endpoint=search_service_endpoint,  
        index_name=index_name,  
        credential=credential,  
        transport=transport,  
    )  
  
  
def _select_fields_for_search() -> str:  
    base = [  
        "title",  
        "content",  
        "folder_name",  
        "url",  
        "chunk_id",  
        "content_path",  
        "page_number",  
        "chunk_kind",  
        "sort_key",  
        "projection_parent_text", 
        "projection_parent_image",     
    ]  
    if CHUNK_ID_FIELD and CHUNK_ID_FIELD not in base:  
        base.append(CHUNK_ID_FIELD)  
    return ", ".join(base)  
  
  
def get_query_embedding(query):  
    embedding_response = client.embeddings.create(  
        model="text-embedding-3-large",  
        input=query,  
        dimensions=1536,  
    )  
    return embedding_response.data[0].embedding  
  
  
def hybrid_semantic_search_single(query, topNDocuments, index_name, filter_expr=None):  
    search_client = get_search_client(index_name)  
    try:  
        query_embedding = get_query_embedding(query)  
  
        if VectorizedQuery is not None:  
            vector_query = VectorizedQuery(  
                vector=query_embedding,  
                k_nearest_neighbors=50,  
                fields="contentVector",  
                exhaustive=False,  
            )  
        else:  
            vector_query = {  
                "kind": "vector",  
                "vector": query_embedding,  
                "exhaustive": False,  
                "fields": "contentVector",  
                "k": 50,  
            }  
  
        results = search_client.search(  
            search_text=query,  
            search_fields=SEARCH_TARGET_FIELDS,  
            vector_queries=[vector_query],  
            filter=filter_expr,  
            query_type="semantic",  
            semantic_configuration_name="default",  
            select=_select_fields_for_search(),  
            top=topNDocuments,  
        )  
        return [dict(r) for r in results]  
    except Exception as e:  
        print(f"ハイブリッド検索エラー (query='{query}', filter='{filter_expr}'):", e)  
        traceback.print_exc()  
        return []  
  
  
def hybrid_search_multiqueries(queries, per_search_top, fused_top, index_name):  
    queries = queries[:MAX_MULTIQUERY]  
    rrf_k = 60  
    fusion_scores = {}  
    fusion_docs = {}  
  
    t0 = time.perf_counter()  
    futures = {}  
  
    with ThreadPoolExecutor(max_workers=SEARCH_MAX_WORKERS) as executor:  
        for query in queries:  
            futures[executor.submit(  
                hybrid_semantic_search_single,  
                query,  
                per_search_top,  
                index_name,  
                None,  
            )] = query  
  
        for future in as_completed(futures):  
            query = futures[future]  
            try:  
                result_list = future.result()  
            except Exception as e:  
                print(f"検索タスクエラー (query='{query}'):", e)  
                result_list = []  
  
            for idx, result in enumerate(result_list):  
                doc = dict(result)  
                chunk_id = get_chunk_id_from_result(doc)  
                dedupe_key = chunk_id or doc.get("url") or doc.get("title")  
                if not dedupe_key:  
                    continue  
  
                contribution = 1 / (rrf_k + (idx + 1))  
                fusion_scores[dedupe_key] = fusion_scores.get(dedupe_key, 0) + contribution  
  
                if dedupe_key not in fusion_docs:  
                    fusion_docs[dedupe_key] = doc  
  
    t1 = time.perf_counter()  
    print(f"[TIMING] search (hybrid, tasks={len(queries)}, workers={SEARCH_MAX_WORKERS}): {t1 - t0:.3f}s")  
  
    sorted_keys = sorted(fusion_scores, key=lambda d: fusion_scores[d], reverse=True)  
    ranked = []  
    for k in sorted_keys[:fused_top]:  
        doc = dict(fusion_docs[k])  
        doc["fusion_score"] = fusion_scores[k]  
        ranked.append(doc)  
  
    kind_counts = {}  
    for d in ranked:  
        k = (d.get("chunk_kind") or "unknown").lower()  
        kind_counts[k] = kind_counts.get(k, 0) + 1  
    print("[DEBUG] fused top kind counts:", kind_counts)  
  
    return ranked  
  
  
def _escape_odata_string(s: str) -> str:  
    return (s or "").replace("'", "''")  
  
  
def _get_page_number(doc: dict) -> int:  
    try:  
        lm = doc.get("locationMetadata") or {}  
        pn = lm.get("pageNumber")  
        if pn is not None:  
            return int(pn)  
    except Exception:  
        pass  
  
    pn2 = doc.get("page_number")  
    if pn2 is not None:  
        try:  
            return int(pn2)  
        except Exception:  
            pass  
    return 10**9  
  
  
def _get_page_number_or_none(doc: dict):  
    pn = _get_page_number(doc)  
    return None if pn >= 10**9 else pn  
  
  
def _get_sort_key(doc: dict) -> str:  
    return (doc.get("sort_key") or "").strip()  
  
  
def _get_parent_id(doc: dict) -> str:  
    return (doc.get("projection_parent_text") or doc.get("projection_parent_image") or "").strip()  
  
  
def _get_file_locator(doc: dict) -> tuple[str, str]:  
    url = (doc.get("url") or "").strip()  
    if url:  
        return ("url", url)  
  
    parent_id = _get_parent_id(doc)  
    if parent_id:  
        return ("parent", parent_id)  
  
    return ("", "")  
  
  
def _get_file_key(doc: dict) -> str:  
    kind, value = _get_file_locator(doc)  
    return f"{kind}:{value}" if kind and value else ""  
  
  
def _chunk_order_key(doc: dict):  
    sk = _get_sort_key(doc)  
    if sk:  
        return (0, sk)  
  
    kind_rank = 0 if (doc.get("chunk_kind") or "").lower() == "text" else 1  
    return (  
        1,  
        _get_page_number(doc),  
        kind_rank,  
        str(get_chunk_id_from_result(doc) or ""),  
    )  
  
  
def _sort_chunks_in_doc(chunks: list[dict]) -> list[dict]:  
    return sorted(chunks, key=_chunk_order_key)  
  
  
def _strip_redundant_header(text: str) -> str:  
    if not text:  
        return ""  
    lines = text.splitlines()  
    if lines and lines[0].startswith("[File:"):  
        return "\n".join(lines[1:]).lstrip("\n")  
    return text  
  
  
def _render_chunk_for_rag(doc: dict, strip_header: bool = False) -> str:  
    t = (doc.get("content") or "")  
    if strip_header:  
        t = _strip_redundant_header(t)  
    t = t.strip()  
    if not t:  
        return ""  
  
    kind = (doc.get("chunk_kind") or "text").lower()  
    pn = _get_page_number(doc)  
    page_label = f"p.{pn}" if pn < 10**9 else "p.?"  
    
    # 画像チャンクの場合はIDを明記する  
    if kind == "image":  
        chunk_id = get_chunk_id_from_result(doc)  
        label = f"【図・画像 chunk_id:{chunk_id}】"  
    else:  
        label = "【本文】"  
        
    return f"{label} {page_label}\n{t}" 
  
  
def _join_chunks_for_rag(chunks: list[dict], max_chars: int | None) -> str:  
    out_parts = []  
    total = 0  
  
    for i, c in enumerate(chunks):  
        rendered = _render_chunk_for_rag(c, strip_header=(i > 0))  
        if not rendered:  
            continue  
  
        if max_chars is not None and total >= max_chars:  
            break  
  
        if max_chars is not None and total + len(rendered) > max_chars:  
            rendered = rendered[: max_chars - total]  
  
        out_parts.append(rendered)  
        total += len(rendered)  
  
    return "\n\n".join(out_parts)  
  
  
def _fetch_all_chunks_for_file(  
    search_client: SearchClient,  
    file_kind: str,  
    file_value: str,  
) -> list[dict]:  
    if not file_kind or not file_value:  
        return []  
  
    esc = _escape_odata_string(file_value)  
    if file_kind == "url":  
        flt = f"url eq '{esc}'"  
    else:  
        flt = f"(projection_parent_text eq '{esc}' or projection_parent_image eq '{esc}')"  
  
    all_docs = []  
    skip = 0  
  
    while True:  
        try:  
            page = [  
                dict(d)  
                for d in search_client.search(  
                    search_text="*",  
                    filter=flt,  
                    select=_select_fields_for_search(),  
                    order_by=["sort_key asc"],  
                    top=DOC_FETCH_PAGE_SIZE,  
                    skip=skip,  
                )  
            ]  
        except Exception as e:  
            print("file fetch with sort_key order failed, fallback to unordered fetch:", e)  
            page = [  
                dict(d)  
                for d in search_client.search(  
                    search_text="*",  
                    filter=flt,  
                    select=_select_fields_for_search(),  
                    top=DOC_FETCH_PAGE_SIZE,  
                    skip=skip,  
                )  
            ]  
  
        if not page:  
            break  
  
        all_docs.extend(page)  
        skip += len(page)  
  
        if len(all_docs) >= DOC_FETCH_MAX_CHUNKS:  
            break  
  
    return all_docs  
  
  
def _build_window_around_anchor(  
    ordered_chunks: list[dict],  
    anchor_chunk_id: str,  
    max_chars: int,  
) -> list[dict]:  
    if not ordered_chunks:  
        return []  
  
    anchor_idx = 0  
    if anchor_chunk_id:  
        for i, c in enumerate(ordered_chunks):  
            if get_chunk_id_from_result(c) == anchor_chunk_id:  
                anchor_idx = i  
                break  
  
    anchor_only = ordered_chunks[anchor_idx].get("content") or ""  
    if len(anchor_only) > max_chars:  
        return [ordered_chunks[anchor_idx]]  
  
    selected_indexes = {anchor_idx}  
    left = anchor_idx - 1  
    right = anchor_idx + 1  
  
    def current_len() -> int:  
        chosen = [ordered_chunks[i] for i in sorted(selected_indexes)]  
        return len(_join_chunks_for_rag(chosen, max_chars=None))  
  
    while current_len() < max_chars and (left >= 0 or right < len(ordered_chunks)):  
        expanded = False  
  
        if left >= 0:  
            selected_indexes.add(left)  
            left -= 1  
            expanded = True  
            if current_len() >= max_chars:  
                break  
  
        if right < len(ordered_chunks):  
            selected_indexes.add(right)  
            right += 1  
            expanded = True  
  
        if not expanded:  
            break  
  
    return [ordered_chunks[i] for i in sorted(selected_indexes)]  
  
  
def _window_sort_range(window_chunks: list[dict]) -> tuple[str, str]:  
    ordered = _sort_chunks_in_doc(window_chunks)  
    if not ordered:  
        return ("", "")  
    start_sk = _get_sort_key(ordered[0])  
    end_sk = _get_sort_key(ordered[-1])  
    return (start_sk, end_sk)  
  
  
def _merge_overlapping_sort_ranges(ranges: list[tuple[str, str]]) -> list[tuple[str, str]]:  
    items = [(s, e) for s, e in ranges if s and e]  
    if not items:  
        return []  
  
    items.sort(key=lambda x: x[0])  
    merged = []  
  
    for s, e in items:  
        if not merged:  
            merged.append([s, e])  
            continue  
  
        last_s, last_e = merged[-1]  
        if s <= last_e:  
            if e > last_e:  
                merged[-1][1] = e  
        else:  
            merged.append([s, e])  
  
    return [(s, e) for s, e in merged]  
  
  
def _chunk_in_sort_range(chunk: dict, start_sk: str, end_sk: str) -> bool:  
    sk = _get_sort_key(chunk)  
    if not sk:  
        return False  
    return start_sk <= sk <= end_sk  
  
  
def _collect_chunks_in_ranges(  
    ordered_chunks: list[dict],  
    ranges: list[tuple[str, str]],  
) -> list[dict]:  
    if not ranges:  
        return []  
  
    out = []  
    seen = set()  
  
    for c in ordered_chunks:  
        cid = get_chunk_id_from_result(c)  
        dedupe_key = cid or f"{_get_sort_key(c)}|{c.get('content_path', '')}|{(c.get('content') or '')[:50]}"  
        for start_sk, end_sk in ranges:  
            if _chunk_in_sort_range(c, start_sk, end_sk):  
                if dedupe_key not in seen:  
                    out.append(c)  
                    seen.add(dedupe_key)  
                break  
  
    return out  
  
  
def _unique_chunks_preserve_order(chunks: list[dict]) -> list[dict]:  
    out = []  
    seen = set()  
    for c in chunks:  
        cid = get_chunk_id_from_result(c)  
        key = cid or f"{_get_sort_key(c)}|{c.get('content_path', '')}|{(c.get('content') or '')[:50]}"  
        if key in seen:  
            continue  
        seen.add(key)  
        out.append(c)  
    return out  
  
  
def _render_rag_blocks_from_ranges(  
    ordered_chunks: list[dict],  
    ranges: list[tuple[str, str]],  
    max_total_chars: int | None = None,  
) -> str:  
    blocks = []  
    total = 0  
  
    for i, (start_sk, end_sk) in enumerate(ranges, start=1):  
        window_chunks = _collect_chunks_in_ranges(ordered_chunks, [(start_sk, end_sk)])  
        body = _join_chunks_for_rag(window_chunks, max_chars=None)  
        if not body:  
            continue  
  
        block = f"--- 同一ファイル内の関連範囲 {i} ---\n{body}"  
  
        if max_total_chars is not None and total >= max_total_chars:  
            break  
        if max_total_chars is not None and total + len(block) > max_total_chars:  
            block = block[: max_total_chars - total]  
  
        blocks.append(block)  
        total += len(block) + 2  
  
    return "\n\n".join(blocks)  
  
  
def _collect_source_images(chunks: list[dict], limit: int = MAX_SOURCE_IMAGES) -> list[dict]:  
    images = []  
    seen = set()  
  
    for c in chunks:  
        cp = (c.get("content_path") or "").strip()  
        if not cp or cp in seen:  
            continue  
  
        image_url = build_kb_image_inline_url(cp)  
        if not image_url:  
            continue  
  
        seen.add(cp)  
        pn = _get_page_number(c)  
        images.append(  
            {  
                "content_path": decode_percent_loose(cp),  
                "image_url": image_url,  
                "page_number": None if pn >= 10**9 else pn,  
                "chunk_id": get_chunk_id_from_result(c),  
            }  
        )  
  
        if len(images) >= limit:  
            break  
  
    return images  
  
  
def build_rag_sources_from_top_hits(  
    top_hits: list[dict],  
    index_name: str,  
    max_chars_per_source: int = 10000,  
) -> list[dict]:  
    search_client = get_search_client(index_name)  
  
    file_groups: dict[str, dict] = {}  
    file_order: list[str] = []  
  
    for hit in top_hits:  
        file_key = _get_file_key(hit)  
        if not file_key:  
            file_key = f"chunk:{get_chunk_id_from_result(hit) or uuid.uuid4()}"  
  
        if file_key not in file_groups:  
            file_kind, file_value = _get_file_locator(hit)  
            file_groups[file_key] = {  
                "file_kind": file_kind,  
                "file_value": file_value,  
                "hits": [],  
            }  
            file_order.append(file_key)  
  
        file_groups[file_key]["hits"].append(hit)  
  
    file_order = file_order[:MAX_RAG_FILES]  
  
    rag_sources = []  
    file_cache: dict[str, dict] = {}  
  
    for file_key in file_order:  
        group = file_groups[file_key]  
        hits = group["hits"]  
        if not hits:  
            continue  
  
        best_hit = hits[0]  
        title = decode_percent_loose(best_hit.get("title", "不明"))  
        raw_url = (best_hit.get("url") or "").strip()  
        folder = extract_folder_from_result(best_hit)  
  
        anchor_score = max(float(h.get("fusion_score", 0) or 0) for h in hits)  
        anchor_sort_key = (best_hit.get("sort_key") or "").strip()  
  
        if group["file_kind"] and group["file_value"]:  
            if file_key not in file_cache:  
                chunks = _fetch_all_chunks_for_file(  
                    search_client,  
                    group["file_kind"],  
                    group["file_value"],  
                )  
                ordered = _sort_chunks_in_doc(chunks)  
                total_chars = len(_join_chunks_for_rag(ordered, max_chars=None))  
                file_cache[file_key] = {  
                    "ordered_chunks": ordered,  
                    "total_chars": total_chars,  
                }  
  
            ordered = file_cache[file_key]["ordered_chunks"]  
            total_chars = int(file_cache[file_key]["total_chars"] or 0)  
        else:  
            ordered = hits[:]  
            total_chars = len(_join_chunks_for_rag(ordered, max_chars=None))  
  
        if not ordered:  
            ordered = hits[:]  
  
        ui_snippets = []  
        seen_ui_keys = set()  
        for h in hits:  
            cid = get_chunk_id_from_result(h)  
            dedupe_key = cid or f"{_get_sort_key(h)}|{(h.get('content') or '')[:50]}"  
            if dedupe_key in seen_ui_keys:  
                continue  
            seen_ui_keys.add(dedupe_key)  
  
            snippet = _strip_redundant_header(h.get("content", "") or "")  
            if len(snippet) > 600:  
                snippet = snippet[:600] + "..."  
  
            ui_snippets.append(  
                {  
                    "chunk_id": cid,  
                    "content": snippet,  
                    "score": float(h.get("fusion_score", 0) or 0),  
                    "source_no": 0,  
                    "chunk_kind": h.get("chunk_kind", ""),  
                    "page_number": _get_page_number_or_none(h),  
                    "sort_key": h.get("sort_key", ""),  
                }  
            )  
  
        effective_ranges = []  
        selected_chunks = []  
        rag_content = ""  
  
        if total_chars <= max_chars_per_source:  
            selected_chunks = ordered[:]  
            first_sk = _get_sort_key(ordered[0]) if ordered else ""  
            last_sk = _get_sort_key(ordered[-1]) if ordered else ""  
            effective_ranges = [(first_sk, last_sk)] if first_sk and last_sk else []  
            rag_content = _join_chunks_for_rag(selected_chunks, max_chars=None)  
        else:  
            hit_anchor_ids = []  
            seen_anchor_ids = set()  
  
            for h in hits:  
                cid = get_chunk_id_from_result(h)  
                if not cid or cid in seen_anchor_ids:  
                    continue  
                seen_anchor_ids.add(cid)  
                hit_anchor_ids.append(cid)  
  
            hit_anchor_ids = hit_anchor_ids[:MAX_WINDOWS_PER_FILE]  
  
            window_ranges = []  
            window_chunk_union = []  
  
            for anchor_id in hit_anchor_ids:  
                window_chunks = _build_window_around_anchor(  
                    ordered,  
                    anchor_id,  
                    max_chars_per_source,  
                )  
  
                for c in window_chunks:  
                    window_chunk_union.append(c)  
  
                start_sk, end_sk = _window_sort_range(window_chunks)  
                if start_sk and end_sk:  
                    window_ranges.append((start_sk, end_sk))  
  
            effective_ranges = _merge_overlapping_sort_ranges(window_ranges)  
  
            if effective_ranges:  
                selected_chunks = _collect_chunks_in_ranges(ordered, effective_ranges)  
                rag_content = _render_rag_blocks_from_ranges(  
                    ordered,  
                    effective_ranges,  
                    max_total_chars=MAX_TOTAL_RAG_CHARS_PER_FILE,  
                )  
            else:  
                selected_chunks = _unique_chunks_preserve_order(window_chunk_union)  
                selected_chunks = _sort_chunks_in_doc(selected_chunks)  
                rag_content = _join_chunks_for_rag(  
                    selected_chunks,  
                    max_chars=MAX_TOTAL_RAG_CHARS_PER_FILE,  
                )  
  
            if not selected_chunks:  
                selected_chunks = [best_hit]  
                rag_content = _join_chunks_for_rag(  
                    selected_chunks,  
                    max_chars=max_chars_per_source,  
                )  
  
        source_images = _collect_source_images(selected_chunks)  
  
        rag_sources.append(  
            {  
                "source_no": 0,  
                "file_id": file_key,  
                "title": title,  
                "url": raw_url,  
                "folder": folder,  
                "rag_content": rag_content,  
                "ui_snippets": ui_snippets,  
                "ui_snippet": ui_snippets[0]["content"] if ui_snippets else "",  
                "anchor_chunk_id": get_chunk_id_from_result(best_hit),  
                "anchor_score": anchor_score,  
                "anchor_sort_key": anchor_sort_key,  
                "images": source_images,  
                "selected_ranges": effective_ranges,  
                "all_hits": hits,  
            }  
        )  
  
    for i, s in enumerate(rag_sources, start=1):  
        s["source_no"] = i  
        for sn in s.get("ui_snippets", []):  
            sn["source_no"] = i  
  
    return rag_sources  
  
  
CITATION_RE = re.compile(r"\[(\d+)\]") # [1] などの形式を正確に拾う  
  
  
def _extract_cited_source_numbers(text: str) -> list[int]:  
    nums = set()  
    for m in CITATION_RE.findall(text or ""):  
        try:  
            nums.add(int(m))  
        except Exception:  
            pass  
    return sorted(nums)  


def replace_inline_images_in_text(text: str, rag_sources: list[dict]) -> str:  
    if not text:  
        return text  
  
    # chunk_id をキーにして生成済みSAS URLを取得  
    image_map = {}  
    for src in rag_sources:  
        for img in (src.get("images") or []):  
            cid = img.get("chunk_id")  
            img_url = img.get("image_url")  
            if cid and img_url:  
                image_map[cid] = img_url  
  
    def replacer(match):  
        cid = match.group(1).strip()  
        if cid in image_map:  
            # 直接styleを書かず、クラスを付与する 
            return f'\n\n<img src="{image_map[cid]}" class="kb-inline-image" alt="参照画像"/>\n\n'  
        return ""  
  
    # [Image: xxx] の形式を検知  
    return re.sub(r"\[Image:\s*([^\]]+)\]", replacer, text)

  
def build_answer_images_from_citations(  
    answer_text: str,  
    rag_sources: list[dict],  
    max_images: int = MAX_ANSWER_IMAGES,  
) -> list[dict]:  
    cited = _extract_cited_source_numbers(answer_text)  
    if not cited:  
        return []  
  
    source_map = {int(s.get("source_no") or 0): s for s in rag_sources}  
    out = []  
    seen = set()  
  
    for no in cited:  
        src = source_map.get(no)  
        if not src:  
            continue  
  
        for img in (src.get("images") or []):  
            cp = (img.get("content_path") or "").strip()  
            if not cp or cp in seen:  
                continue  
  
            seen.add(cp)  
            out.append(  
                {  
                    "source_no": no,  
                    "title": src.get("title", ""),  
                    "content_path": cp,  
                    "page_number": img.get("page_number"),  
                }  
            )  
            if len(out) >= max_images:  
                return out  
  
    return out  
  
  
def materialize_answer_images(answer_images: list[dict]) -> list[dict]:  
    out = []  
    for img in answer_images or []:  
        cp = (img.get("content_path") or "").strip()  
        if not cp:  
            continue  
  
        image_url = build_kb_image_inline_url(cp)  
        if not image_url:  
            continue  
  
        item = dict(img)  
        item["image_url"] = image_url  
        out.append(item)  
  
    return out  
  
  
def build_render_messages(messages):  
    out = []  
    if not isinstance(messages, list):  
        return out  
  
    for m in messages:  
        if not isinstance(m, dict):  
            continue  
  
        nm = dict(m)  
        if nm.get("role") == "assistant":  
            nm["content_html"] = render_assistant_markdown_to_html(nm.get("content", ""))  
            nm["answer_images_display"] = materialize_answer_images(nm.get("answer_images", []))  
        else:  
            nm.pop("content_html", None)  
            nm.pop("answer_images_display", None)  
  
        out.append(nm)  
  
    return out  
  
  
def rematerialize_search_files(search_files: list[dict]) -> list[dict]:  
    out = []  
    if not isinstance(search_files, list):  
        return out  
  
    for file in search_files:  
        if not isinstance(file, dict):  
            continue  
  
        nf = dict(file)  
  
        rel_path = (nf.get("filepath") or "").strip()  
        raw_url = (nf.get("url") or "").strip()  
        if rel_path:  
            is_text = rel_path.lower().endswith(".txt")  
            nf["url"] = build_doc_url(rel_path, is_text, fallback_url=raw_url)  
  
        images = []  
        for img in (nf.get("images") or []):  
            if not isinstance(img, dict):  
                continue  
            nimg = dict(img)  
            cp = (nimg.get("content_path") or "").strip()  
            if cp:  
                fresh_url = build_kb_image_inline_url(cp)  
                if fresh_url:  
                    nimg["image_url"] = fresh_url  
            images.append(nimg)  
        nf["images"] = images  
  
        out.append(nf)  
  
    return out  
  
  
def extract_latest_search_payload(messages):  
    payload = {  
        "search_files": [],  
        "rag_context": "",  
        "search_query": [],  
    }  
    if not isinstance(messages, list):  
        return payload  
  
    for m in reversed(messages):  
        if not isinstance(m, dict):  
            continue  
        if m.get("role") != "assistant":  
            continue  
  
        search_files = m.get("search_files") or []  
        rag_context = m.get("rag_context") or ""  
        search_query = m.get("search_query") or []  
  
        if search_files or rag_context or search_query:  
            payload["search_files"] = rematerialize_search_files(search_files)  
            payload["rag_context"] = rag_context if isinstance(rag_context, str) else ""  
            payload["search_query"] = search_query if isinstance(search_query, list) else []  
            return payload  
  
    return payload  
  
  
@app.route("/", methods=["GET", "POST"])  
def index():  
    mode = session.get("conversation_mode", "qa")  
    if "default_system_message" not in session:  
        session["default_system_message"] = MODE_CONFIG[mode]["system_message"]  
        session.modified = True  
  
    if "sidebar_messages" not in session:  
        session["sidebar_messages"] = load_chat_history() or []  
        session.modified = True  
  
    if "current_chat_index" not in session:  
        start_new_chat()  
        session["show_all_history"] = False  
        session.modified = True  
  
    if "main_chat_messages" not in session:  
        idx = session.get("current_chat_index", 0)  
        sidebar = session.get("sidebar_messages", [])  
        if sidebar and idx < len(sidebar):  
            session["main_chat_messages"] = sidebar[idx].get("messages", [])  
        else:  
            session["main_chat_messages"] = []  
        session.modified = True  
  
    if "image_filenames" not in session:  
        session["image_filenames"] = []  
        session.modified = True  
  
    if "show_all_history" not in session:  
        session["show_all_history"] = False  
        session.modified = True  
  
    if "conversation_mode" not in session:  
        session["conversation_mode"] = "qa"  
        session.modified = True  
  
    try:  
        sidebar = session.get("sidebar_messages", [])  
        for c in sidebar:  
            ensure_chat_has_system_history(  
                c,  
                fallback_system_message=c.get("system_message", session.get("default_system_message", "")),  
            )  
            ensure_chat_has_created_at(c, fallback_timestamp=c.get("timestamp", ""))  
            c["messages"] = strip_content_html_from_messages(c.get("messages", []))  
        session["sidebar_messages"] = sidebar  
        session.modified = True  
    except Exception:  
        pass  
  
    if request.method == "POST":  
        if "set_conversation_mode" in request.form:  
            mode = request.form.get("conversation_mode", "qa")  
            session["conversation_mode"] = mode  
            new_default = MODE_CONFIG[mode]["system_message"]  
            session["default_system_message"] = new_default  
  
            idx = session.get("current_chat_index", 0)  
            sidebar = session.get("sidebar_messages", [])  
            if sidebar and 0 <= idx < len(sidebar):  
                sidebar[idx]["system_message"] = new_default  
                append_system_message_history(sidebar[idx], new_default)  
                ensure_chat_has_created_at(sidebar[idx], fallback_timestamp=sidebar[idx].get("timestamp", ""))  
                session["sidebar_messages"] = sidebar  
            session.modified = True  
            save_chat_history()  
            return redirect(url_for("index"))  
  
        if "set_system_message" in request.form:  
            sys_msg = request.form.get("system_message", "").strip()  
            session["default_system_message"] = sys_msg  
            idx = session.get("current_chat_index", 0)  
            sidebar = session.get("sidebar_messages", [])  
            if sidebar and idx < len(sidebar):  
                sidebar[idx]["system_message"] = sys_msg  
                append_system_message_history(sidebar[idx], sys_msg)  
                ensure_chat_has_created_at(sidebar[idx], fallback_timestamp=sidebar[idx].get("timestamp", ""))  
                session["sidebar_messages"] = sidebar  
            session.modified = True  
            save_chat_history()  
            return redirect(url_for("index"))  
  
        if "new_chat" in request.form:  
            start_new_chat()  
            session["show_all_history"] = False  
            session.modified = True  
            return redirect(url_for("index"))  
  
        if "select_chat" in request.form:  
            selected_session = request.form.get("select_chat")  
            sidebar = session.get("sidebar_messages", [])  
            for idx, chat in enumerate(sidebar):  
                if chat.get("session_id") == selected_session:  
                    session["current_chat_index"] = idx  
                    session["main_chat_messages"] = strip_content_html_from_messages(chat.get("messages", []))  
                    break  
            session.modified = True  
            return redirect(url_for("index"))  
  
        if "toggle_history" in request.form:  
            session["show_all_history"] = not session.get("show_all_history", False)  
            session.modified = True  
            return redirect(url_for("index"))  
  
        if "upload_images" in request.form:  
            if "images" in request.files and image_container_client:  
                files = request.files.getlist("images")  
                image_filenames = session.get("image_filenames", [])  
                for file in files:  
                    if file and file.filename != "":  
                        try:  
                            filename = secure_filename(file.filename)  
                            blob_client = image_container_client.get_blob_client(filename)  
                            file.stream.seek(0)  
                            blob_client.upload_blob(file.stream, overwrite=True)  
                            if filename not in image_filenames:  
                                image_filenames.append(filename)  
                        except Exception as e:  
                            print("画像アップロードエラー:", e)  
                            traceback.print_exc()  
                session["image_filenames"] = image_filenames  
                session.modified = True  
            return redirect(url_for("index"))  
  
        if "delete_image" in request.form:  
            delete_image_name = request.form.get("delete_image")  
            image_filenames = session.get("image_filenames", [])  
            image_filenames = [name for name in image_filenames if name != delete_image_name]  
            if image_container_client:  
                blob_client = image_container_client.get_blob_client(delete_image_name)  
                try:  
                    blob_client.delete_blob()  
                except Exception as e:  
                    print("画像削除エラー:", e)  
                    traceback.print_exc()  
            session["image_filenames"] = image_filenames  
            session.modified = True  
            return redirect(url_for("index"))  
  
    chat_history = build_render_messages(session.get("main_chat_messages", []))  
    sidebar_messages = session.get("sidebar_messages", [])  
  
    image_filenames = session.get("image_filenames", [])  
    images = []  
    if image_container_client:  
        for filename in image_filenames:  
            images.append({"name": filename, "url": url_for("get_image", filename=quote(filename))})  
  
    max_displayed_history = 6  
    max_total_history = 50  
    show_all_history = session.get("show_all_history", False)  
  
    current_sys_msg = session.get("default_system_message") or MODE_CONFIG[  
        session.get("conversation_mode", "qa")  
    ]["system_message"]  
    idx = session.get("current_chat_index", 0)  
    sidebar = session.get("sidebar_messages", [])  
    if sidebar and 0 <= idx < len(sidebar):  
        current_sys_msg = sidebar[idx].get("system_message", current_sys_msg)  
  
    current_session_id = ""  
    if sidebar and 0 <= idx < len(sidebar):  
        current_session_id = sidebar[idx].get("session_id", "")  
  
    initial_search_data = extract_latest_search_payload(session.get("main_chat_messages", []))  
  
    return render_template(  
        "index.html",  
        chat_history=chat_history,  
        chat_sessions=sidebar_messages,  
        images=images,  
        show_all_history=show_all_history,  
        max_displayed_history=max_displayed_history,  
        max_total_history=max_total_history,  
        session=session,  
        current_system_message=current_sys_msg,  
        current_session_id=current_session_id,  
        initial_search_data=initial_search_data,  
    )  
  
  
@app.route("/rewrite_queries", methods=["POST"])  
def rewrite_queries_endpoint():  
    data = request.get_json(silent=True) or {}  
    prompt = (data.get("prompt") or "").strip()  
    if not prompt:  
        return (  
            json.dumps({"ok": False, "error": "missing_prompt"}, ensure_ascii=False),  
            400,  
            {"Content-Type": "application/json"},  
        )  
  
    try:  
        base_messages = session.get("main_chat_messages", [])  
        temp_messages = list(base_messages) + [{"role": "user", "content": prompt}]  
        queries = rewrite_queries_for_search(temp_messages, prompt)  
        return (  
            json.dumps({"ok": True, "queries": queries[:MAX_MULTIQUERY]}, ensure_ascii=False),  
            200,  
            {"Content-Type": "application/json"},  
        )  
    except Exception as e:  
        print("rewrite_queries_endpoint エラー:", e)  
        traceback.print_exc()  
        return (  
            json.dumps({"ok": False, "error": "rewrite_failed"}, ensure_ascii=False),  
            500,  
            {"Content-Type": "application/json"},  
        )  
  
  
@app.route("/send_message", methods=["POST"])  
def send_message():  
    data = request.get_json(silent=True) or {}  
    prompt = (data.get("prompt") or "").strip()  
    if not prompt:  
        return (  
            json.dumps(  
                {  
                    "response": "",  
                    "search_query": [],  
                    "search_files": [],  
                    "answer_images": [],  
                },  
                ensure_ascii=False,  
            ),  
            400,  
            {"Content-Type": "application/json"},  
        )  
  
    user_msg_id = str(uuid.uuid4())  
    messages = session.get("main_chat_messages", [])  
    messages.append({"role": "user", "content": prompt, "message_id": user_msg_id})  
    session["main_chat_messages"] = messages  
    session.modified = True  
  
    sync_current_chat_messages_to_sidebar()  
    save_chat_history()  
  
    try:  
        mode = session.get("conversation_mode", "qa")  
        config = MODE_CONFIG.get(mode, MODE_CONFIG["qa"])  
  
        idx = session.get("current_chat_index", 0)  
        sidebar = session.get("sidebar_messages", [])  
        if sidebar and idx < len(sidebar) and sidebar[idx].get("system_message"):  
            system_msg = sidebar[idx]["system_message"]  
        else:  
            system_msg = session.get("default_system_message") or config["system_message"]  
  
        if sidebar and 0 <= idx < len(sidebar):  
            ensure_chat_has_system_history(sidebar[idx], fallback_system_message=system_msg)  
            ensure_chat_has_created_at(sidebar[idx], fallback_timestamp=sidebar[idx].get("timestamp", ""))  
            sidebar[idx]["messages"] = strip_content_html_from_messages(messages)  
            session["sidebar_messages"] = sidebar  
            session.modified = True  
            save_chat_history()  
  
        max_ai_history_turns = 10  
        main_chat_messages = session.get("main_chat_messages", [])  
        recent = main_chat_messages[-(max_ai_history_turns * 3):]  
        past_messages = [{"role": m.get("role"), "content": m.get("content", "")} for m in recent]  
  
        image_filenames = session.get("image_filenames", [])  
  
        if mode == "programming":  
            messages_list = [{"role": "system", "content": system_msg}]  
            messages_list.extend(past_messages)  
  
            if image_filenames and image_container_client:  
                image_contents = []  
                for img_name in image_filenames:  
                    blob_client = image_container_client.get_blob_client(img_name)  
                    try:  
                        encoded_image = encode_image_from_blob(blob_client)  
                        ext = img_name.rsplit(".", 1)[-1].lower()  
                        if ext in ["jpg", "jpeg"]:  
                            mime_type = "image/jpeg"  
                        elif ext in ["png", "gif", "bmp", "webp"]:  
                            mime_type = f"image/{ext}"  
                        else:  
                            mime_type = "application/octet-stream"  
                        data_url = f"data:{mime_type};base64,{encoded_image}"  
                        image_contents.append({"type": "image_url", "image_url": {"url": data_url}})  
                    except Exception as e:  
                        print("画像エンコードエラー:", e)  
                        traceback.print_exc()  
  
                if image_contents:  
                    messages_list.append({"role": "user", "content": image_contents})  
  
            extra_args = dict(config.get("extra_args", {}))  
  
            t0 = time.perf_counter()  
            response_obj = client.chat.completions.create(  
                model=config["model"],  
                messages=messages_list,  
                **extra_args,  
            )  
            t1 = time.perf_counter()  
            print(f"[TIMING] answer generation (mode=programming): {t1 - t0:.3f}s")  
  
            assistant_response_text = response_obj.choices[0].message.content or ""  
            assistant_response_text = normalize_math_blocks(assistant_response_text)  
            assistant_response_text = normalize_symbol_definitions(assistant_response_text)  
  
            assistant_response_html = render_assistant_markdown_to_html(assistant_response_text)  
            assistant_msg_id = str(uuid.uuid4())  
  
            messages.append(  
                {  
                    "role": "assistant",  
                    "content": assistant_response_text,  
                    "message_id": assistant_msg_id,  
                    "answer_images": [],  
                    "search_files": [],  
                    "rag_context": "",  
                    "search_query": [],  
                }  
            )  
  
            session["main_chat_messages"] = messages  
            session.modified = True  
  
            sidebar = session.get("sidebar_messages", [])  
            if idx < len(sidebar):  
                sidebar[idx]["messages"] = strip_content_html_from_messages(messages)  
                if not sidebar[idx].get("first_assistant_message"):  
                    sidebar[idx]["first_assistant_message"] = assistant_response_text  
                session["sidebar_messages"] = sidebar  
                session.modified = True  
            save_chat_history()  
  
            return (  
                json.dumps(  
                    {  
                        "response": assistant_response_html,  
                        "search_query": [],  
                        "search_files": [],  
                        "assistant_message_id": assistant_msg_id,  
                        "answer_images": [],  
                        "rag_context": "",  
                    },  
                    ensure_ascii=False,  
                ),  
                200,  
                {"Content-Type": "application/json"},  
            )  
  
        pre_queries = data.get("pre_queries")  
        if isinstance(pre_queries, list) and pre_queries:  
            queries = [str(q).strip() for q in pre_queries if str(q).strip()][:MAX_MULTIQUERY]  
        else:  
            queries = rewrite_queries_for_search(messages, prompt)  
  
        results_list = hybrid_search_multiqueries(  
            queries,  
            per_search_top=PER_SEARCH_TOP,  
            fused_top=FUSED_TOP,  
            index_name=search_index_name,  
        )  
  
        kind_counts = {}  
        for r in results_list:  
            k = (r.get("chunk_kind") or "unknown").lower()  
            kind_counts[k] = kind_counts.get(k, 0) + 1  
        print("[DEBUG] results_list kind counts:", kind_counts)  
  
        for i, r in enumerate(results_list[:20], start=1):  
            print(  
                f"[DEBUG] hit#{i}",  
                "kind=", r.get("chunk_kind"),  
                "page=", r.get("page_number"),  
                "sort_key=", r.get("sort_key"),  
                "title=", r.get("title"),  
                "content=", (r.get("content") or "")[:120].replace("\n", " "),  
            )  
  
        rag_sources = build_rag_sources_from_top_hits(  
            results_list,  
            index_name=search_index_name,  
            max_chars_per_source=MAX_RAG_CHARS_PER_SOURCE,  
        )  
  
        for s in rag_sources:  
            print(  
                "[DEBUG] rag_source",  
                "title=", s["title"],  
                "hit_count=", len(s.get("all_hits", [])),  
                "range_count=", len(s.get("selected_ranges", [])),  
                "image_count=", len(s.get("images", [])),  
            )  
  
        context_entries = []  
        for s in rag_sources:  
            title = s.get("title", "不明")  
            folder_decoded = decode_percent_loose(s.get("folder", "") or "").replace("\n", " ").strip()  
            u = s.get("url", "")  
            content = s.get("rag_content", "")  
  
            if folder_decoded:  
                headline = f"[{s['source_no']}] タイトル: {title} / フォルダ: {folder_decoded}"  
            else:  
                headline = f"[{s['source_no']}] タイトル: {title}"  
  
            entry = (  
                f"{headline}\n"  
                f"url: {u}\n"  
                f"内容: {content}"  
            )  
            context_entries.append(entry)  
  
        context = "\n\n".join(context_entries)  
  
        context_preamble = (  
            "以下は検索結果をファイル単位で再構成したコンテキストです。\n"  
            "各 source は 1 ファイルに対応します。\n"  
            f"短いファイルは全文を、長いファイルはヒットごとに前後へ広げた最大 {MAX_RAG_CHARS_PER_SOURCE} 文字の関連範囲を複数含めています。\n"  
            "複数の関連範囲がある場合は、同一ファイル内の別ブロックとしてまとめています。\n"  
            "また、各関連範囲の sort_key 範囲内に含まれる図・画像チャンクも可能な範囲で含めています。\n"  
            "回答内では参照した source 番号 [n] を必ず付けてください。図や画像を根拠にした場合も [n] を付けてください。\n\n"  
        )  
  
        file_groups: dict[str, dict] = {}  
        file_order: list[str] = []  
  
        for s in rag_sources:  
            title = s.get("title", "不明")  
            raw_url = (s.get("url") or "").strip()  
            folder = s.get("folder", "")  
            source_no = int(s.get("source_no") or 0)  
            file_id = s.get("file_id", "")  
  
            file_key = raw_url or file_id or title  
            if not file_key:  
                continue  
  
            rel_path = extract_blob_path_from_result({"url": raw_url})  
            is_text = rel_path.lower().endswith(".txt") if rel_path else False  
            link_url = build_doc_url(rel_path, is_text, fallback_url=raw_url)  
  
            if file_key not in file_groups:  
                file_groups[file_key] = {  
                    "file_key": file_key,  
                    "title": title,  
                    "folder": folder,  
                    "url": link_url,  
                    "filepath": rel_path,  
                    "chunks": [],  
                    "source_nos_set": set(),  
                    "images_map": {},  
                    "rag_contexts_map": {},  
                    "selected_ranges": [],  
                }  
                file_order.append(file_key)  
  
            file_groups[file_key]["source_nos_set"].add(source_no)  
            file_groups[file_key]["rag_contexts_map"][source_no] = {  
                "content": s.get("rag_content", "") or "",  
                "anchor_sort_key": s.get("anchor_sort_key", "") or "",  
            }  
  
            for rng in (s.get("selected_ranges") or []):  
                if rng not in file_groups[file_key]["selected_ranges"]:  
                    file_groups[file_key]["selected_ranges"].append(rng)  
  
            seen_chunk_keys = {  
                (c.get("chunk_id", ""), c.get("sort_key", ""), c.get("content", ""))  
                for c in file_groups[file_key]["chunks"]  
            }  
  
            for sn in (s.get("ui_snippets") or []):  
                chunk_obj = {  
                    "chunk_id": sn.get("chunk_id", "") or "",  
                    "content": sn.get("content", "") or "",  
                    "score": float(sn.get("score", 0) or 0),  
                    "source_no": source_no,  
                    "chunk_kind": sn.get("chunk_kind", "") or "",  
                    "page_number": sn.get("page_number"),  
                    "sort_key": sn.get("sort_key", "") or "",  
                }  
                dedupe_tuple = (  
                    chunk_obj["chunk_id"],  
                    chunk_obj["sort_key"],  
                    chunk_obj["content"],  
                )  
                if dedupe_tuple in seen_chunk_keys:  
                    continue  
                seen_chunk_keys.add(dedupe_tuple)  
                file_groups[file_key]["chunks"].append(chunk_obj)  
  
            for img in (s.get("images") or []):  
                cp = img.get("content_path")  
                if not cp:  
                    continue  
                if cp not in file_groups[file_key]["images_map"]:  
                    file_groups[file_key]["images_map"][cp] = {  
                        "source_no": source_no,  
                        "content_path": cp,  
                        "image_url": img.get("image_url", ""),  
                        "page_number": img.get("page_number"),  
                    }  
  
        for gobj in file_groups.values():  
            gobj["chunks"].sort(key=lambda c: c.get("score", 0), reverse=True)  
  
        search_files = []  
        for fk in file_order:  
            gobj = file_groups[fk]  
  
            chunks_ui = [  
                {  
                    "chunk_id": c.get("chunk_id", ""),  
                    "content": c.get("content", "") or "",  
                    "source_no": c.get("source_no", 0),  
                    "chunk_kind": c.get("chunk_kind", ""),  
                    "page_number": c.get("page_number"),  
                    "sort_key": c.get("sort_key", ""),  
                }  
                for c in (gobj.get("chunks") or [])  
            ]  
  
            source_nos = sorted([n for n in gobj.get("source_nos_set", set()) if n])  
  
            images_all = list((gobj.get("images_map") or {}).values())  
            images_all.sort(key=lambda x: int(x.get("source_no") or 0))  
            images_ui = images_all[:12]  
  
            rag_contexts_map = gobj.get("rag_contexts_map") or {}  
            rag_contexts = []  
            for k, v in rag_contexts_map.items():  
                rag_contexts.append(  
                    {  
                        "source_no": int(k),  
                        "content": v.get("content", "") or "",  
                        "anchor_sort_key": v.get("anchor_sort_key", "") or "",  
                    }  
                )  
  
            rag_contexts.sort(  
                key=lambda x: (0, x.get("anchor_sort_key", ""))  
                if x.get("anchor_sort_key")  
                else (1, int(x.get("source_no") or 0))  
            )  
  
            search_files.append(  
                {  
                    "title": gobj.get("title", "不明"),  
                    "url": gobj.get("url", ""),  
                    "folder": gobj.get("folder", ""),  
                    "filepath": gobj.get("filepath", ""),  
                    "chunks": chunks_ui,  
                    "source_nos": source_nos,  
                    "images": images_ui,  
                    "image_count": len(images_all),  
                    "rag_contexts": rag_contexts,  
                    "selected_ranges": gobj.get("selected_ranges", []),  
                }  
            )  
  
        messages_list = [{"role": "system", "content": system_msg}]  
        messages_list.extend(past_messages)  
  
        if image_filenames and image_container_client:  
            image_contents = []  
            for img_name in image_filenames:  
                blob_client = image_container_client.get_blob_client(img_name)  
                try:  
                    encoded_image = encode_image_from_blob(blob_client)  
                    ext = img_name.rsplit(".", 1)[-1].lower()  
                    if ext in ["jpg", "jpeg"]:  
                        mime_type = "image/jpeg"  
                    elif ext in ["png", "gif", "bmp", "webp"]:  
                        mime_type = f"image/{ext}"  
                    else:  
                        mime_type = "application/octet-stream"  
                    data_url = f"data:{mime_type};base64,{encoded_image}"  
                    image_contents.append({"type": "image_url", "image_url": {"url": data_url}})  
                except Exception as e:  
                    print("画像エンコードエラー:", e)  
                    traceback.print_exc()  
  
            messages_list.append(  
                {  
                    "role": "user",  
                    "content": [  
                        {  
                            "type": "text",  
                            "text": context_preamble + context,  
                        }  
                    ] + image_contents,  
                }  
            )  
        else:  
            messages_list.append(  
                {  
                    "role": "user",  
                    "content": context_preamble + context,  
                }  
            )  
  
        extra_args = dict(config.get("extra_args", {}))  
  
        t0 = time.perf_counter()  
        response_obj = client.chat.completions.create(  
            model=config["model"],  
            messages=messages_list,  
            **extra_args,  
        )  
        t1 = time.perf_counter()  
        print(f"[TIMING] answer generation (mode={mode}): {t1 - t0:.3f}s")  
  
        assistant_response_text = response_obj.choices[0].message.content or ""  
        assistant_response_text = normalize_math_blocks(assistant_response_text)  
        assistant_response_text = normalize_symbol_definitions(assistant_response_text)  
  
        # 追加: マーカーを実際の画像URL（HTMLタグ）に置換  
        assistant_response_text = replace_inline_images_in_text(  
            assistant_response_text,  
            rag_sources  
        )  
  
        answer_images = build_answer_images_from_citations(  
            assistant_response_text,  
            rag_sources,  
            max_images=MAX_ANSWER_IMAGES,  
        )

        answer_images_display = materialize_answer_images(answer_images)  
  
        assistant_response_html = render_assistant_markdown_to_html(assistant_response_text)  
        assistant_msg_id = str(uuid.uuid4())  
  
        messages.append(  
            {  
                "role": "assistant",  
                "content": assistant_response_text,  
                "message_id": assistant_msg_id,  
                "answer_images": answer_images,  
                "search_files": search_files,  
                "rag_context": context,  
                "search_query": queries[:MAX_MULTIQUERY],  
            }  
        )  
  
        session["main_chat_messages"] = messages  
        session.modified = True  
  
        sidebar = session.get("sidebar_messages", [])  
        if idx < len(sidebar):  
            sidebar[idx]["messages"] = strip_content_html_from_messages(messages)  
            if not sidebar[idx].get("first_assistant_message"):  
                sidebar[idx]["first_assistant_message"] = assistant_response_text  
            session["sidebar_messages"] = sidebar  
            session.modified = True  
        save_chat_history()  
  
        return (  
            json.dumps(  
                {  
                    "response": assistant_response_html,  
                    "search_query": queries[:MAX_MULTIQUERY],  
                    "search_files": search_files,  
                    "rag_context": context,  
                    "assistant_message_id": assistant_msg_id,  
                    "answer_images": answer_images_display,  
                },  
                ensure_ascii=False,  
            ),  
            200,  
            {"Content-Type": "application/json"},  
        )  
  
    except Exception as e:  
        print("チャット応答エラー:", e)  
        traceback.print_exc()  
        flash(f"エラーが発生しました: {e}", "error")  
        session.modified = True  
        return (  
            json.dumps(  
                {  
                    "response": f"エラーが発生しました: {e}",  
                    "search_query": [],  
                    "search_files": [],  
                    "answer_images": [],  
                    "rag_context": "",  
                },  
                ensure_ascii=False,  
            ),  
            500,  
            {"Content-Type": "application/json"},  
        )  
  
  
@app.route("/feedback", methods=["POST"])  
def submit_feedback():  
    data = request.get_json(silent=True) or {}  
    if not data:  
        return (  
            json.dumps({"ok": False, "error": "invalid_json"}, ensure_ascii=False),  
            400,  
            {"Content-Type": "application/json"},  
        )  
  
    session_id = data.get("session_id")  
    rating = data.get("rating")  
    comment = (data.get("comment") or "").strip()  
    message_id = data.get("message_id")  
    message_index = data.get("message_index")  
  
    if rating not in ("helpful", "unhelpful"):  
        return (  
            json.dumps({"ok": False, "error": "invalid_rating"}, ensure_ascii=False),  
            400,  
            {"Content-Type": "application/json"},  
        )  
    if not session_id:  
        return (  
            json.dumps({"ok": False, "error": "missing_session_id"}, ensure_ascii=False),  
            400,  
            {"Content-Type": "application/json"},  
        )  
  
    try:  
        comment = bleach.clean(comment, tags=[], attributes={}, strip=True)  
    except Exception:  
        pass  
  
    with lock:  
        sidebar = session.get("sidebar_messages", [])  
        target_chat = None  
        target_idx = None  
        for i, chat in enumerate(sidebar):  
            if chat.get("session_id") == session_id:  
                target_chat = chat  
                target_idx = i  
                break  
  
        if not target_chat:  
            return (  
                json.dumps({"ok": False, "error": "chat_not_found"}, ensure_ascii=False),  
                404,  
                {"Content-Type": "application/json"},  
            )  
  
        ensure_chat_has_created_at(target_chat, fallback_timestamp=target_chat.get("timestamp", ""))  
  
        msgs = strip_content_html_from_messages(target_chat.get("messages", []))  
        target_chat["messages"] = msgs  
  
        target_msg = None  
        if message_id:  
            for m in msgs:  
                if m.get("role") == "assistant" and m.get("message_id") == message_id:  
                    target_msg = m  
                    break  
        elif isinstance(message_index, int) and 0 <= message_index < len(msgs):  
            m = msgs[message_index]  
            if m.get("role") == "assistant":  
                target_msg = m  
  
        if not target_msg:  
            return (  
                json.dumps({"ok": False, "error": "message_not_found"}, ensure_ascii=False),  
                404,  
                {"Content-Type": "application/json"},  
            )  
  
        fb = {  
            "rating": rating,  
            "comment": comment,  
            "submitted_by": g.get("current_user") or get_authenticated_user(),  
            "submitted_name": session.get("user_name", "anonymous"),  
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),  
        }  
        target_msg["feedback"] = fb  
  
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()  
        if UPDATE_TIMESTAMP_ON_FEEDBACK:  
            target_chat["timestamp"] = now_iso  
  
        if session.get("current_chat_index") == target_idx:  
            main_msgs = strip_content_html_from_messages(session.get("main_chat_messages", []))  
            if message_id:  
                for m in main_msgs:  
                    if m.get("role") == "assistant" and m.get("message_id") == message_id:  
                        m["feedback"] = fb  
                        break  
            elif isinstance(message_index, int) and 0 <= message_index < len(main_msgs):  
                if main_msgs[message_index].get("role") == "assistant":  
                    main_msgs[message_index]["feedback"] = fb  
            session["main_chat_messages"] = main_msgs  
  
        sidebar[target_idx]["messages"] = msgs  
        session["sidebar_messages"] = sidebar  
        session.modified = True  
  
        if cosmos_container is not None:  
            try:  
                user_id = g.get("current_user") or get_authenticated_user()  
                user_name = session.get("user_name", "anonymous")  
  
                existing = None  
                try:  
                    existing = cosmos_container.read_item(item=target_chat["session_id"], partition_key=user_id)  
                except Exception:  
                    existing = None  
  
                if existing and not UPDATE_TIMESTAMP_ON_FEEDBACK:  
                    ts_to_save = existing.get("timestamp")  
                else:  
                    ts_to_save = target_chat.get("timestamp") or now_iso  
  
                item = {  
                    "id": target_chat["session_id"],  
                    "user_id": user_id,  
                    "user_name": user_name,  
                    "session_id": target_chat["session_id"],  
                    "messages": msgs,  
                    "system_message": target_chat.get(  
                        "system_message",  
                        session.get(  
                            "default_system_message",  
                            MODE_CONFIG[session.get("conversation_mode", "qa")]["system_message"],  
                        ),  
                    ),  
                    "system_message_history": target_chat.get("system_message_history", []),  
                    "first_assistant_message": target_chat.get("first_assistant_message", ""),  
                    "created_at": (existing.get("created_at") if existing else None) or target_chat.get("created_at"),  
                    "timestamp": ts_to_save,  
                }  
                cosmos_container.upsert_item(item)  
            except Exception as e:  
                print("フィードバック保存エラー（Cosmos）:", e)  
                traceback.print_exc()  
  
    return (  
        json.dumps({"ok": True}, ensure_ascii=False),  
        200,  
        {"Content-Type": "application/json"},  
    )  
  
  
@app.route("/view_txt/<container>/<path:blobname>")  
def view_txt(container, blobname):  
    blobname = unquote(blobname)  
    if not blob_service_client:  
        return Response("ストレージ未初期化", status=500)  
    validate_container_and_path(container, blobname)  
    bc = blob_service_client.get_blob_client(container=container, blob=blobname)  
    txt_bytes = bc.download_blob().readall()  
    try:  
        txt_str = txt_bytes.decode("utf-8")  
    except UnicodeDecodeError:  
        txt_str = txt_bytes.decode("cp932", errors="ignore")  
    return Response(  
        txt_str,  
        mimetype="text/plain; charset=utf-8",  
        headers={  
            "Content-Disposition": f'inline; filename="download.txt"; filename*=UTF-8\'\'{quote(os.path.basename(blobname))}'  
        },  
    )  
  
  
@app.route("/download_txt/<container>/<path:blobname>")  
def download_txt(container, blobname):  
    blobname = unquote(blobname)  
    if not blob_service_client:  
        return Response("ストレージ未初期化", status=500)  
    validate_container_and_path(container, blobname)  
    bc = blob_service_client.get_blob_client(container=container, blob=blobname)  
    txt_bytes = bc.download_blob().readall()  
    try:  
        txt_str = txt_bytes.decode("utf-8")  
    except UnicodeDecodeError:  
        txt_str = txt_bytes.decode("cp932", errors="ignore")  
    bom = b"\xef\xbb\xbf"  
    buf = io.BytesIO(bom + txt_str.encode("utf-8"))  
    filename = os.path.basename(blobname)  
    ascii_filename = "download.txt"  
    response = send_file(  
        buf,  
        as_attachment=True,  
        download_name=ascii_filename,  
        mimetype="text/plain; charset=utf-8",  
    )  
    response.headers["Content-Disposition"] = (  
        f'attachment; filename="{ascii_filename}"; filename*=UTF-8\'\'{quote(filename)}'  
    )  
    return response  
  
  
@app.route("/download_blob/<container>/<path:blobname>")  
def download_blob(container, blobname):  
    blobname = unquote(blobname)  
    if not blob_service_client:  
        return Response("ストレージ未初期化", status=500)  
    validate_container_and_path(container, blobname)  
    bc = blob_service_client.get_blob_client(container=container, blob=blobname)  
    data = bc.download_blob().readall()  
    filename = os.path.basename(blobname)  
    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"  
    return send_file(  
        io.BytesIO(data),  
        as_attachment=True,  
        download_name=filename,  
        mimetype=content_type,  
    )  
  
  
@app.route("/image/<path:filename>")  
def get_image(filename):  
    filename = unquote(filename)  
    if not image_container_client:  
        return Response("画像コンテナ未初期化", status=500)  
    validate_blob_path(filename)  
    bc = image_container_client.get_blob_client(filename)  
    data = bc.download_blob().readall()  
    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"  
    return Response(data, mimetype=content_type)  
  
  
if __name__ == "__main__":  
    app.run(debug=IS_LOCAL, host="0.0.0.0")  