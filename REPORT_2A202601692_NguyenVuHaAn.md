# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Vũ Hà An  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 30.4s
  run 2/3 … 29.6s
  run 3/3 … 31.5s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt** (+2 bài mở rộng)

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Khi chạy lại pipeline (hoặc Clear Task trên Airflow), số hàng trong `gold_training_set` tăng lên liên tục sau mỗi lần chạy thay vì giữ nguyên, gây trùng lặp ticket (13.790 hàng ở lượt 1, tăng thêm ở các lượt sau). |
| **Nguyên nhân** | Model `gold_training_set` cấu hình `materialized = 'incremental'` nhưng thiếu `unique_key`. Khi không có `unique_key`, dbt mặc định dùng chiến lược `append` (chèn thêm hàng mới) khi gặp bản ghi update `op = 'u'` hoặc khi chạy lại một ngày. Ngoài ra, DAG ban đầu bật `catchup=True` và không giới hạn `max_active_runs`, dẫn đến rủi ro các DAG run chạy song song ghi đồng thời làm race condition. |
| **Cách khắc phục** | 1. Trong `dbt/models/gold/gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'delete+insert'`.<br>2. Trong `dags/ai_training_pipeline.py`: Đặt `catchup = False` và `max_active_runs = 1`. |
| **Bằng chứng** | trước: 13.790 hàng (thừa 1.310 hàng, lặp ticket) · sau: 12.480 hàng (đúng 100%) · checksum 3 lượt: `8dd7c98653` (ổn định tuyệt đối). |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` thiếu ~5% số hàng ở các ngày trong quá khứ (8.645 / 9.100 hàng). Các ngày mới thì đủ nhưng các ngày cũ bị hụt. |
| **P99 độ trễ đo được** | **2.73 ngày** *(bắt buộc)* (P50 = 0.13 ngày, P95 = 1.81 ngày, Max = 2.94 ngày, tỷ lệ trễ > 1 ngày = 5.05%). |
| **Lookback đã chọn** | **3 ngày** — vì P99 = 2.73 ngày (< 3 ngày), bao phủ được hơn 99% dữ liệu về muộn mà chi phí tính toán cố định và tối ưu. |
| **Nguyên nhân** | Dữ liệu về muộn (Late-arriving data) bị loại bỏ bởi điều kiện `where event_date > (select max(event_date) from {{ this }})` trong khối `is_incremental()`. Khi chạy ngày 08-15, `max(event_date)` đã là 08-14 nên sự kiện ngày 08-12 về muộn bị điều kiện so sánh loại bỏ vĩnh viễn khỏi bảng Gold. |
| **Cách khắc phục** | 1. Trong `dbt/models/gold/gold_feature_daily.sql`: Sửa điều kiện thành lùi 3 ngày `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`.<br>2. Thêm khóa composite `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'delete+insert'` để ghi đè các bản ghi tính lại thay vì cộng dồn. |
| **Bằng chứng** | trước: 8.645 hàng · sau: 9.100 hàng (đủ 14 ngày × 650 customer) · checksum 3 lượt: `3db448685c` (ổn định). |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Chọn theo **P99** (2.73 ngày ➔ Lookback 3 ngày) giúp bao phủ >99% dữ liệu về muộn với chi phí tính toán cố định và rất thấp ở mỗi lần chạy hàng ngày. Nếu chọn theo **`max`**, một vài bản ghi cá biệt bị trễ cực lớn (ví dụ thiết bị offline gửi bù sau vài tháng) sẽ buộc toàn bộ pipeline phải quét lại toàn bộ lịch sử ở **mọi lần chạy**, gây lãng phí IO/CPU khổng lồ (small-file problem, full scan). Các trường hợp cá biệt vượt P99 nên được xử lý bằng luồng reconciliation/backfill định kỳ riêng.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Backend đổi kiểu cột `priority` sang chuỗi từ ngày 08-10, pipeline không dừng nhưng bảng `silver_tickets` có hơn 6.600 hàng sai (bị NULL hoặc ngoài khoảng 1..4), làm hỏng chất lượng tập dữ liệu huấn luyện model AI. |
| **Nguyên nhân** | File `schema.yml` tắt Data Contract (`enforced: false`) và chưa bật kiểm tra miền giá trị. Macro cũ dùng `try_cast` làm nhãn chữ hợp lệ ('urgent', 'high',...) biến thành `NULL`, đồng thời chấp nhận các giá trị lỗi ('0', '5', '-1') vì chúng ép kiểu số thành công. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | 1. **Số hợp lệ** (`'1'`, `'2'`, `'3'`, `'4'` - 6.846 bản ghi): Giữ nguyên, cast về integer.<br>2. **Nhãn chuỗi** (`'urgent'`, `'high'`, `'medium'`, `'low'` - 7.142 bản ghi): Schema Evolution ➔ Map về 1..4 (`urgent→1, high→2, medium→3, low→4`).<br>3. **Dữ liệu lỗi** (`'0'`, `'5'`, `'-1'`, `''`, `'unknown'`, `'P1'`, `'P2'`, `NULL` - 312 bản ghi): Trả về `NULL` ➔ Định tuyến sang `quarantine_tickets`. |
| **Cách khắc phục** | 1. `dbt/macros/normalize_priority.sql`: Viết khối `CASE` xử lý 3 nhóm.<br>2. `dbt/models/silver/silver_tickets.sql`: Lọc bỏ bản ghi lỗi trước (`where normalize_priority is not null`), sau đó mới đánh số thứ tự `row_number()` để loại bản ghi hỏng mà không làm mất ticket.<br>3. `dbt/models/silver/quarantine_tickets.sql`: Nhận các bản ghi `where normalize_priority is null`.<br>4. `dbt/models/silver/schema.yml`: Bật `contract: enforced: true` và test `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `silver_tickets.priority` sạch 100% · `dbt test` 11/11 pass. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> **Nên lưu toàn bộ vào Bronze và chặn/cách ly ở Silver:** Tầng Bronze là hồ sơ bất biến (Raw/Immutable Log). Nếu chặn ở Bronze, dữ liệu lỗi bị mất dấu hoàn toàn, không thể điều tra nguyên nhân hay replay khi backend fix bug.  
> **Không để pipeline dừng khi gặp bản ghi lỗi (Quarantine / Dead-Letter Queue pattern):** Trong hệ thống có hàng trăm nghìn sự kiện, vài trăm bản ghi lỗi (<0.2%) không được phép làm tê liệt toàn bộ hệ thống phục vụ người dùng. Tách riêng các bản ghi lỗi vào bảng `quarantine_tickets` giúp luồng chính tiếp tục vận hành liên tục, đồng thời tạo hàng đợi để đội trực ca cảnh báo và xử lý sau.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | Cả hai bài (Bài A + Bài B) |
| **Nguyên nhân** | **Bài A:** Small-file problem (5.000 file nhỏ không partition, non-sargable predicate) khiến scan 5.000.000 rows.<br>**Bài B:** Consumer commit offset trước khi ghi (At-Most-Once), khi crash bị mất 500 bản ghi của batch hiện tại. |
| **Cách khắc phục** | **Bài A:** Viết `tools/compact.py` gom thành 14 partition ngày (`PARTITION_BY event_date`), sort theo `customer_name, event_time`, `row_group_size 1000`; sửa `dashboard.sql` dùng filter sargable `event_date = '2026-08-09'`.<br>**Bài B:** Đảo thứ tự ghi dữ liệu trước, crash, commit offset sau (At-Least-Once); thêm `PRIMARY KEY (event_id)` và `ON CONFLICT (event_id) DO UPDATE` để đảm bảo idempotency khi replay batch. |
| **Bằng chứng** | **Bài A:** Rows scanned giảm từ **5.000.000 → 9.324** (giảm **536.3×**), số file giảm từ 5.000 → 14, result hash không đổi.<br>**Bài B:** `make crash-test` đạt 100% (không mất bản ghi, không trùng bản ghi, C == A). |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra tính **Idempotent** của các model incremental: đã có `unique_key` và `incremental_strategy` phù hợp với grain của bảng (Entity vs Event) chưa; DAG có giới hạn `max_active_runs` và tắt `catchup` khi backfill không. |
| 2 | Kiểm tra phân bố thời gian trễ của dữ liệu nguồn (`event_time` vs `ingestion_time` theo các mốc P95/P99) và kiểm tra xem điều kiện `is_incremental` đã có **Lookback Window** đủ an toàn kèm khóa ghi đè hay chưa. |
| 3 | Kiểm tra việc kích hoạt **Data Contract** (`enforced: true`) và các ràng buộc schema/giá trị (`not_null`, `accepted_values`), đồng thời kiểm tra xem hệ thống có cơ chế **Quarantine / DLQ** để cách ly bản ghi lỗi mà không làm dừng pipeline hay không. |
