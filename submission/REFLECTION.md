# REFLECTION — Day 18 Lakehouse Lab

**Sinh viên:** Vũ Quang Bảo · **MSSV:** 2A202600610

> Câu hỏi: Trong "Top 5 Lakehouse Anti-Patterns", anti-pattern nào dữ liệu của
> team mình dễ vướng nhất, và vì sao?

---

Anti-pattern team mình dễ vướng nhất là **small-file problem** — ingest sinh ra
vô số file nhỏ mà không bao giờ compaction. Pipeline observability của bọn mình
ghi log LLM theo từng micro-batch streaming, mỗi phút đẻ ra hàng loạt file con,
đúng hình dạng tái hiện trong NB2 (200 lần append → 200 file).

Hệ quả đo được ngay trong lab: một point-query lọc theo `user_id` mất **10,86s**
khi bảng manh mún, nhưng chỉ còn **0,33s** sau `OPTIMIZE ... ZORDER BY (user_id)`
— nhanh **32,6×**, và `numFiles` từ 200 gộp còn 1. Cơ chế là file-skipping:
Delta đọc min/max mỗi file từ transaction log và bỏ qua file không chứa giá trị
cần tìm.

Team mình rủi ro vì chưa có lịch `OPTIMIZE` định kỳ và chưa chọn cột clustering
hợp lý, khiến cả chi phí query lẫn chi phí list-object trên S3 phình theo thời
gian. Hướng khắc phục: đặt compaction job theo cadence (vd hằng giờ cho hot
path), Z-order theo cột lọc phổ biến nhất, và coi `numFiles` như một health
metric cần theo dõi.
