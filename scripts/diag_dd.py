import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.core.config2 import settings as form_settings

def normalize_id(s):
    if s is None: return ""
    s = str(s).strip()
    return "".join(ch for ch in s if ch.isalnum()).upper()

def force_pymysql(url):
    u = url or ""
    u = u.replace("mysql+mysqlconnector", "mysql+pymysql")
    u = u.replace("+mysqlconnector", "+pymysql")
    if "mysql+pymysql" not in u and u.startswith("mysql://"):
        u = u.replace("mysql://", "mysql+pymysql://")
    return u

def build_mysqlconnector(url):
    u = url or ""
    u = u.replace("mysql+pymysql", "mysql+mysqlconnector")
    if "mysql+mysqlconnector" not in u and u.startswith("mysql://"):
        u = u.replace("mysql://", "mysql+mysqlconnector://")
    sep = "&" if "?" in u else "?"
    if "auth_plugin=" not in u:
        u = f"{u}{sep}auth_plugin=mysql_native_password"
    return u

def main():
    empresa_id = int(os.getenv("DD_EMPRESA", "0"))
    numero_id = normalize_id(os.getenv("DD_NUMERO_ID", ""))
    url = build_mysqlconnector(form_settings.SOURCE_DATABASE_URL)
    try:
        eng = create_engine(url, pool_pre_ping=True, future=True)
    except Exception:
        url = force_pymysql(form_settings.SOURCE_DATABASE_URL)
        eng = create_engine(url, pool_pre_ping=True, future=True)
    sess = sessionmaker(autocommit=False, autoflush=False, bind=eng, future=True)()
    try:
        info_sql = text("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE()")
        dfc = sess.execute(info_sql).fetchall()
        tables = {}
        for t, c in dfc:
            tables.setdefault(t, []).append(c)
        target = None
        emp_col = None
        id_col = None
        for t, cols in tables.items():
            for ec in ["id_empresa","empresa_id","company_id"]:
                if ec in cols:
                    for ic in ["numero_id","identificacion","documento","nit","num_id","no_documento_de_identidad"]:
                        if ic in cols:
                            target = t
                            emp_col = ec
                            id_col = ic
                            break
                if target: break
            if target: break
        if not target:
            print("NO_TARGET_TABLE")
            return
        q = text(f"SELECT {emp_col}, {id_col} FROM {target} WHERE {emp_col} = :eid")
        rows = sess.execute(q, {"eid": empresa_id}).fetchall()
        found = False
        for r in rows:
            nid = normalize_id(r[1])
            if nid == numero_id or (len(nid)>1 and nid[:-1]==numero_id) or (len(numero_id)>1 and numero_id[:-1]==nid):
                found = True
                break
        print("TARGET", target, emp_col, id_col)
        print("COUNT", len(rows))
        print("MATCH", "YES" if found else "NO")
    finally:
        sess.close()

if __name__ == "__main__":
    main()
