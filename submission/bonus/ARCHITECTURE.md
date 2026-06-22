# Bonus Challenge — CDC Ride-Hailing VN → Lakehouse (tuân thủ Nghị định 13)

**Sinh viên:** Vũ Quang Bảo · **MSSV:** 2A202600610 · **Topic:** C

---

## 1. Problem statement

Một hãng gọi xe Việt Nam cần đưa dữ liệu giao dịch từ **Oracle production** sang
lakehouse để phục vụ analytics, qua **Debezium CDC**. Quy mô: **100 triệu
chuyến/năm**, peak **30K writes/giây**. Mỗi chuyến phát sinh ~7 sự kiện thay đổi
(created → assigned → started → completed → paid → rated) ⇒ **~700 triệu CDC
event/năm (~2 GB/ngày landing)**.

PII của tài xế + hành khách (số điện thoại, **CMND/CCCD**, **GPS**) thuộc phạm vi
**Nghị định 13/2023/NĐ-CP** — bắt buộc tối thiểu hoá, kiểm soát truy cập, và ghi
log mọi lần đọc dữ liệu cá nhân.

SLA: dashboard refresh **trong 60 giây** kể từ source commit; ad-hoc query **p95
< 1s**. **Sự kiện đến muộn** xảy ra thường xuyên (mất mạng ở tỉnh xa → thiết bị
dồn một lô cũ lên khi có sóng lại). Cái khó: vừa **low-latency**, vừa **chính xác
trước out-of-order**, vừa **tuân thủ pháp lý** — ba ràng buộc kéo ngược nhau.

---

## 2. Architecture diagram

```
                 ┌───────────────────────────────  GOVERNANCE / SECURITY  ───────────────────────────────┐
                 │  Catalog (Unity/Polaris): RBAC + column-mask PII · Token Vault (FPE) · pii_access_audit │
                 └───────────────────────────────────────────────────────────────────────────────────────┘
                        ▲ tokenize ở landing            ▲ mask khi đọc            ▲ log mọi lần đọc PII
                        │                               │                          │
 Oracle (prod)   Debezium      Kafka            Spark Structured Streaming (60s micro-batch)
 trips/drivers ─► (LogMiner) ─► topics  ──────► ┌─────────┐    ┌──────────┐    ┌──────────────┐   BI / Dashboard
 /passengers      redo log     (7d retain)      │ BRONZE  │───►│  SILVER  │───►│    GOLD      │──► (Trino/Databricks
   30K w/s        offset=      replay/backfill  │ raw CDC │CDF │ MERGE +  │CDF │ daily/city   │    SQL, p95<1s)
                  checkpoint                     │ append, │    │ SCD2,    │    │ aggregates,  │
                                                 │ PII →   │    │ late-data│    │ Z-order city │
                                                 │ token   │    │ dedup    │    └──────────────┘
                                                 └────┬────┘    └────┬─────┘
                          fail-closed: PII chưa token │             │ MERGE WHEN MATCHED
                                  → Dead-Letter Queue ▼             ▼ AND s.ts > t.ts (late-safe)
                                                  [ DLQ ]      time-travel RESTORE khi lỗi schema
```

Một luồng: **Oracle → Debezium/Kafka → Bronze (tokenize) → Silver (MERGE/SCD2) →
Gold**. Delta **Change Data Feed (CDF)** đẩy incremental giữa các tầng; catalog +
vault + audit bao quanh để tuân thủ NĐ 13.

---

## 3. Quyết định chính (kèm alternatives đã loại)

**3.1 Table format → Delta Lake.** Loại **Iceberg** vì merge-on-read với
equality-delete cho CDC upsert tần suất cao còn nhiều chi phí compaction và kém
chín hơn cho pattern MERGE liên tục ở thời điểm này. Loại **Hudi** vì vận hành
(timeline, compaction service) phức tạp hơn nhu cầu. Delta cho **CDF** sẵn (đẩy
incremental Silver→Gold) + `MERGE` tối ưu — đúng hai thứ bài toán cần nhất.

**3.2 Ingestion → Debezium + Kafka.** Loại **Oracle GoldenGate** vì license đắt
+ vendor lock-in. Loại **batch JDBC mỗi giờ** vì (a) không thể đạt SLA 60s, (b)
**bỏ sót DELETE** và các trạng thái trung gian. Debezium đọc redo log → bắt được
mọi thay đổi kể cả xoá, Kafka làm buffer chịu peak 30K/s và cho **replay theo
offset** khi downstream lỗi.

**3.3 Streaming engine → Spark Structured Streaming.** Loại **Flink** vì thêm một
stack phải vận hành riêng trong khi team đã dùng Spark/Delta. Loại **micro-batch
hằng giờ** vì phá SLA. Spark SS hợp nhất với `MERGE` của Delta trong cùng một
foreachBatch, micro-batch ~30–45s đủ headroom cho mốc 60s.

**3.4 PII → tokenize tại Bronze landing (vault FPE).** Loại **chỉ encrypt-at-rest**
vì PII thô vẫn nằm rõ trong Bronze, bất kỳ ai có khoá đều đọc được — vi phạm
nguyên tắc tối thiểu hoá của NĐ 13 và để hổng audit. Loại **mask-at-query** vì
PII thô vẫn *hạ cánh* xuống lake (rủi ro lộ ở tầng lưu trữ). Tokenize ngay khi
landing: PII thô **không bao giờ chạm đĩa**; token **deterministic (FPE)** để vẫn
join/dedup được; cột định danh chỉ tái lập qua vault có RBAC + ghi
`pii_access_audit`.

**3.5 Partitioning → `event_date` + Z-ORDER (city_id, driver_id).** Loại
**partition theo city** vì lệch nặng (HCMC/Hà Nội chiếm phần lớn → file khổng lồ
cạnh file tí hon). Loại **partition đa cấp date+city+hour** vì sinh small-file
(đúng anti-pattern NB2). Partition ngày giữ số partition ổn định; Z-order cụm
theo cột lọc nóng để đạt p95 < 1s nhờ file-skipping.

**3.6 Dimension tài xế/hành khách → SCD Type 2 bằng MERGE.** Loại **overwrite**
và **SCD Type 1** vì mất lịch sử — không trả lời được câu hỏi kiểm toán *"tại
thời điểm chuyến đi, trạng thái/hạng tài xế là gì?"* và không phục vụ được điều
tra theo NĐ 13. SCD2 (`valid_from/valid_to/is_current`) qua MERGE giữ đủ lịch sử.

**3.7 Late data → conditional MERGE `WHEN MATCHED AND s.ts > t.ts`.** Loại
**append thẳng** (sinh trùng) và **reprocess toàn bảng** (đắt). Điều kiện so sánh
timestamp + watermark cho phép lô cũ từ tỉnh xa cập nhật đúng bản ghi mà không
ghi đè dữ liệu mới hơn.

---

## 4. Failure modes (kịch bản 3 giờ sáng)

**4.1 Debezium tụt hậu / gap redo log lúc peak.** 3h sáng lễ hội, 30K/s làm
connector lag, Oracle xoay redo log trước khi đọc kịp → mất sự kiện.
*Detection:* alert **Kafka consumer lag** + metric **freshness** trên Bronze
(`now − max(ingest_ts) > 60s`). *Rollback:* Kafka giữ 7 ngày → **replay từ
offset**; nếu vượt redo retention thì backfill bằng LogMiner. An toàn vì `MERGE`
**idempotent** (replay không nhân đôi).

**4.2 Schema thay đổi đột ngột từ Oracle (Day-18 concept).** DBA thêm/đổi kiểu
cột lúc nửa đêm → batch Silver hỏng. *Detection:* **schema enforcement** của
Delta chặn ghi sai + alert. *Rollback:* **time-travel `RESTORE`** Silver về
version tốt gần nhất, cách ly lô xấu vào quarantine, rồi `mergeSchema` có chủ
đích sau khi review. (ACID + time travel = phục hồi có kiểm toán.)

**4.3 Vault tokenize chết → nguy cơ lộ PII.** Nếu vault down mà vẫn cho ghi, PII
thô sẽ hạ cánh. *Thiết kế fail-closed:* không token được thì **đẩy DLQ**, tuyệt
đối không landing PII thô. *Detection:* độ sâu DLQ + thiếu mạch `pii_access_audit`.
*Rollback:* reprocess DLQ sau khi vault hồi phục — đúng tinh thần NĐ 13 (thà trễ
còn hơn lộ).

---

## 5. Ước tính chi phí (back-of-envelope, $/tháng)

| Hạng mục | Phép tính | $/tháng |
|---|---|---|
| **Storage Bronze** (raw CDC, giữ 90 ngày nóng) | 2 GB/ngày × 90 = 180 GB × $0.023/GB | ~$4 |
| **Silver + Gold + Delta history** (~1.5 TB active) | 1.500 GB × $0.023 | ~$35 |
| **Cold/archive** (>90 ngày → S3 Glacier IR, ~1 TB/năm) | 1.000 GB × $0.004 | ~$4 |
| **Spark Streaming cluster** (always-on, ~4× m5.xlarge) | 4 × $0.192/h × 730h | ~$560 |
| **Kafka/MSK** (3 broker nhỏ + Connect cho Debezium) | cụm nhỏ managed | ~$450 |
| **Query/BI compute** (Trino/serverless, ad-hoc) | ~200 h-DBU/tháng | ~$300 |
| **Vault + audit store** | dịch vụ nhỏ + DynamoDB-class | ~$80 |
| **TỔNG** | | **≈ $1.430/tháng** |

Điểm tối ưu lớn nhất là **compute streaming** (~75% chi phí), không phải storage.
FinOps: nếu nới SLA xuống ~2–3 phút có thể chuyển sang trigger định kỳ và **giảm
~40% compute**; tiering nóng/lạnh giữ storage gần như không đáng kể.

---

## 6. MVP một tuần (slice nhỏ nhất shippable)

Chỉ làm **một bảng `trips`** xuyên suốt, bỏ qua đa region / SCD2 dimension / full
catalog governance:

1. **D1–2:** Debezium trên bảng `trips` → Kafka topic; Spark SS đọc → Bronze
   Delta (append), **tokenize `phone` + `cmnd`** bằng một UDF vault stub.
2. **D3–4:** Silver bằng `MERGE` dedup theo `trip_id` + **late-data
   `WHEN MATCHED AND s.ts > t.ts`**; bật **CDF**.
3. **D5:** một Gold dashboard *chuyến/giờ theo thành phố*, Z-order `city_id`.
4. **D6–7:** đo **freshness < 60s** end-to-end; chèn 1 lô sự kiện cũ để chứng
   minh không sinh trùng; kiểm tra **`pii_access_audit`** ghi nhận mọi lần
   detokenize. Demo replay từ Kafka offset để chứng minh idempotency.

**Tiêu chí MVP đạt:** một chuyến mới ở Oracle hiện trên dashboard < 60s; lô đến
muộn không tạo bản ghi trùng; mọi truy cập PII đều có dòng audit.

*PoC khả thi (`poc/`):* một notebook nhỏ demo hàm tokenize FPE deterministic +
`MERGE` late-data trên Delta là phần "khó" nhất của thiết kế.
