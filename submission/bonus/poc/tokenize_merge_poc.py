# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # PoC — Topic C: PII tokenization + late-data MERGE (Delta, lightweight)
#
# Chứng minh **phần khó nhất** của thiết kế CDC ride-hailing (Nghị định 13):
#
# 1. **Tokenize PII tại Bronze landing** — PII thô (số ĐT, CMND) *không bao giờ*
#    chạm đĩa; token **deterministic** nên vẫn join/dedup được; detokenize phải
#    qua vault + **ghi audit** (NĐ 13).
# 2. **Late-data MERGE** — `WHEN MATCHED AND s.ts > t.ts`: sự kiện đến muộn từ
#    tỉnh xa **không** ghi đè bản ghi mới hơn.
#
# Stack: `deltalake` (delta-rs) + Polars. Không cần Spark.
# Chạy:  `python submission/bonus/poc/tokenize_merge_poc.py`

# %%
import hashlib
import hmac
import shutil
import sys
import tempfile

import polars as pl
from deltalake import DeltaTable, write_deltalake

# In được tiếng Việt + ✓ trên console Windows (cp1258) lẫn Linux.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

SILVER = tempfile.mkdtemp(prefix="poc_silver_")  # kho tạm, xoá ở cuối

# %% [markdown]
# ## 1. Vault tokenize deterministic (đại diện cho FPE + KMS trong prod)
#
# Cùng một PII → cùng một token (để join/dedup), nhưng từ token **không** suy
# ngược ra PII nếu không có khoá vault. Mọi lần detokenize đều bị ghi log.

# %%
VAULT_KEY = b"demo-key--rotate-via-KMS-in-prod"
_vault: dict[str, str] = {}        # token -> raw  (trong prod: store có RBAC)
_pii_access_audit: list[dict] = []  # log mọi lần đọc PII


def tokenize(value: str | None) -> str | None:
    """HMAC-SHA256 keyed token — deterministic, không đảo ngược nếu thiếu khoá."""
    if value is None:
        return None
    tok = "tok_" + hmac.new(VAULT_KEY, value.encode(), hashlib.sha256).hexdigest()[:16]
    _vault[tok] = value  # vault giữ ánh xạ ngược, gác bằng RBAC ở prod
    return tok


def detokenize(token: str, who: str, reason: str) -> str | None:
    """Đọc PII thô = sự kiện nhạy cảm → BẮT BUỘC ghi audit (NĐ 13)."""
    _pii_access_audit.append({"token": token, "who": who, "reason": reason})
    return _vault.get(token)


# %% [markdown]
# ## 2. Bronze landing — tokenize PII ngay tại cửa
#
# `trip_id=1` và `trip_id=3` cùng một hành khách → token phải **trùng nhau**.

# %%
raw_cdc = pl.DataFrame({
    "trip_id": [1, 2, 3],
    "phone":   ["0901234567", "0907654321", "0901234567"],  # trip3 == trip1
    "cmnd":    ["079201001234", "079201005678", "079201001234"],
    "city_id": ["HCM", "HN", "HCM"],
    "status":  ["completed", "completed", "completed"],
    "ts":      [100, 100, 100],
})

landed = raw_cdc.with_columns([
    pl.col("phone").map_elements(tokenize, return_dtype=pl.Utf8).alias("phone_tok"),
    pl.col("cmnd").map_elements(tokenize, return_dtype=pl.Utf8).alias("cmnd_tok"),
]).drop("phone", "cmnd")  # PII thô KHÔNG được ghi xuống

# Bất biến tuân thủ: không còn cột PII thô nào trên đĩa
assert "phone" not in landed.columns and "cmnd" not in landed.columns
# Determinism: cùng hành khách → cùng token
tok1 = landed.filter(pl.col("trip_id") == 1)["phone_tok"][0]
tok3 = landed.filter(pl.col("trip_id") == 3)["phone_tok"][0]
assert tok1 == tok3, "tokenize phải deterministic để join/dedup"
print("✓ PII thô không hạ cánh; token deterministic (trip1==trip3):", tok1)

write_deltalake(SILVER, landed.to_arrow(), mode="overwrite")
print(pl.from_arrow(DeltaTable(SILVER).to_pyarrow_table()).sort("trip_id"))

# %% [markdown]
# ## 3. Late-data MERGE — `WHEN MATCHED AND s.ts > t.ts`

# %%
def upsert_late_safe(updates: pl.DataFrame) -> None:
    (DeltaTable(SILVER)
        .merge(source=updates.to_arrow(),
               predicate="t.trip_id = s.trip_id",
               source_alias="s", target_alias="t")
        .when_matched_update_all(predicate="s.ts > t.ts")  # chỉ cập nhật nếu mới hơn
        .when_not_matched_insert_all()
        .execute())


# 3a. Sự kiện MỚI cho trip 1 (ts=200): "paid" → phải áp dụng
upsert_late_safe(pl.DataFrame({
    "trip_id": [1], "city_id": ["HCM"], "status": ["paid"], "ts": [200],
    "phone_tok": [tok1], "cmnd_tok": [landed.filter(pl.col("trip_id") == 1)["cmnd_tok"][0]],
}))

# 3b. Sự kiện ĐẾN MUỘN cho trip 1 (ts=50, cũ hơn): "cancelled" → phải BỊ BỎ QUA
upsert_late_safe(pl.DataFrame({
    "trip_id": [1], "city_id": ["HCM"], "status": ["cancelled"], "ts": [50],
    "phone_tok": [tok1], "cmnd_tok": [landed.filter(pl.col("trip_id") == 1)["cmnd_tok"][0]],
}))

final = pl.from_arrow(DeltaTable(SILVER).to_pyarrow_table()).sort("trip_id")
print(final)

trip1 = final.filter(pl.col("trip_id") == 1)
assert trip1["status"][0] == "paid" and trip1["ts"][0] == 200, "late event đã ghi đè nhầm!"
print("✓ Late-data an toàn: trip1 = 'paid' (ts=200); event 'cancelled' (ts=50) bị bỏ qua")

# %% [markdown]
# ## 4. Detokenize có kiểm soát + audit (Nghị định 13)

# %%
who, reason = "analyst_007", "điều tra khiếu nại chuyến #1"
raw_phone = detokenize(tok1, who=who, reason=reason)
print(f"Detokenize (qua vault): {tok1} → {raw_phone}")
print("pii_access_audit:", _pii_access_audit)
assert len(_pii_access_audit) == 1 and _pii_access_audit[0]["who"] == who
print("✓ Mọi lần đọc PII đều để lại dấu vết audit")

# %% [markdown]
# ## ✅ PoC chứng minh
# - [x] PII thô không bao giờ ghi xuống lake; token deterministic (join/dedup OK)
# - [x] Late event (ts cũ hơn) không ghi đè bản ghi mới — `WHEN MATCHED AND s.ts > t.ts`
# - [x] Detokenize bắt buộc qua vault + ghi `pii_access_audit`

# %%
shutil.rmtree(SILVER, ignore_errors=True)  # dọn kho tạm
print("\nPoC hoàn tất — dọn kho tạm xong.")
