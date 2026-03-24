import json
import sys
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


def post_json(url: str, data: dict, headers: dict | None = None) -> dict:
    payload = json.dumps(data).encode("utf-8")
    req = Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except HTTPError as e:
        try:
            return {"status": "error", "code": e.code, "body": e.read().decode("utf-8")}
        except Exception:
            return {"status": "error", "code": e.code, "body": str(e)}
    except URLError as e:
        return {"status": "error", "message": str(e)}


def main():
    base = "http://127.0.0.1:8081"
    tok = post_json(f"{base}/api/v1/auth/token", {})
    jwt = tok.get("jwt")
    if not jwt:
        print("Failed to get token:", tok)
        sys.exit(1)
    headers = {"Authorization": f"Bearer {jwt}"}
    payload = {
        "empresa_id": 1,
        "tipo_contraparte": "cliente",
        "fecha": "2025-09-12",
        "monto_min": 100000,
    }
    res = post_json(f"{base}/api/v1/reports/pdf/request", payload, headers=headers)
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

