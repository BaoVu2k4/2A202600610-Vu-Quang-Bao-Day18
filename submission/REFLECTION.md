# REFLECTION — Day 18 Lakehouse Lab

> ✏️ **Bản nháp — hãy viết lại bằng trải nghiệm/ý kiến của chính bạn trước khi nộp.**
> Yêu cầu: ≤ 200 từ. Câu hỏi: *Trong "Top 5 Lakehouse Anti-Patterns", anti-pattern
> nào dữ liệu của team bạn dễ vướng nhất, vì sao?*

---

Anti-pattern team mình dễ vướng nhất là **small-file problem** (ingest tạo ra hàng
trăm file nhỏ mà không bao giờ compaction). Pipeline observability của bọn mình
ghi log theo từng micro-batch streaming, nên mỗi phút sinh ra rất nhiều file con
— đúng hình dạng đã tái hiện ở NB2 (200 lần append → 200 file).

Hệ quả thấy rõ trong lab: một point-query lọc theo `user_id` mất **10,86s** khi
bảng còn manh mún, nhưng chỉ còn **0,33s** sau `OPTIMIZE ... ZORDER BY (user_id)`
— nhanh **32,6×**. Nguyên nhân là file-skipping: Delta đọc min/max mỗi file từ
transaction log và bỏ qua file không chứa giá trị cần.

Lý do team mình rủi ro: chưa có lịch `OPTIMIZE` định kỳ và chưa chọn cột clustering
hợp lý, nên chi phí query lẫn chi phí list-object trên S3 đều phình theo thời gian.
Hướng khắc phục: đặt compaction job theo cadence (vd hằng giờ cho hot path),
Z-order theo cột lọc phổ biến, và theo dõi `numFiles` như một health metric.

*(Nguồn số liệu: NB2 trong lab này — speedup 32,6×, 200 file → gộp sau OPTIMIZE.)*
