# PoC — Topic C (CDC ride-hailing, Nghị định 13)

Spike chứng minh **phần khó nhất** của thiết kế trong `../ARCHITECTURE.md`:

1. **Tokenize PII tại Bronze landing** — PII thô (số ĐT, CMND) không bao giờ ghi
   xuống lake; token **deterministic** nên vẫn join/dedup được.
2. **Late-data MERGE** — `WHEN MATCHED AND s.ts > t.ts`: sự kiện đến muộn không
   ghi đè bản ghi mới hơn.
3. **Detokenize có audit** — mọi lần đọc PII đều ghi `pii_access_audit`.

Stack lightweight (`deltalake` + Polars), **không cần Spark**.

## Chạy

```bash
# từ thư mục gốc repo, dùng venv lightweight đã tạo bởi `make setup`
.venv/Scripts/python.exe submission/bonus/poc/tokenize_merge_poc.py   # Windows
# hoặc: .venv/bin/python submission/bonus/poc/tokenize_merge_poc.py    # macOS/Linux
```

## Kết quả mong đợi

- `trip1` và `trip3` cùng token (deterministic), không còn cột `phone`/`cmnd` thô.
- Sau 2 lần MERGE: `trip1` = `paid` (ts=200); event `cancelled` (ts=50) bị bỏ qua.
- `pii_access_audit` có đúng 1 dòng cho lần detokenize.

> Đây là spike, không phải implementation đầy đủ: vault dùng HMAC + dict in-memory
> thay cho FPE + KMS/HSM; storage là thư mục tạm. Trong prod xem các quyết định
> 3.4 và 3.7 của `ARCHITECTURE.md`.
