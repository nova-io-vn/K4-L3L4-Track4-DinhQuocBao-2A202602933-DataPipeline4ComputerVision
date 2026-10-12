# REPORT — Lab Day 9

| | |
|---|---|
| Họ và tên | Đinh Quốc Bảo |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602933` |
| GitHub | `nova-io-vn` |
| Trip dùng cho bài tập sập và chạy lại | `trip-04868` (phần số MSSV `202602933 mod 26 = 13`, map index 13) |

## Kết quả cuối

- `dataset_version` của bạn: `3b85ec0fa3d1` (khớp `report/evidence/run_summary.json`).
- Số khung hình train / val / test: **723 / 167 / 129**, tổng **1.019** khung từ 26 trip.
- Kiểm tra tự động: **27/27 checks passed**; cả hai DAG run cuối đều `success`.
- Ảnh bằng chứng chính: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`. Bài tập lỗi có thêm `airflow_failed_log.png` và `airflow_failed_mapped_tasks.png`.

## Bước 1 · Bronze

### Câu 1

Nguồn A dùng `ts_utc` dạng ISO UTC, `speed_mps`, `heading_deg`, `gas/brake/steer`; nguồn B dùng `timestamp_ms` dạng epoch-ms, `spd_kmh`, `hdg`, `throttle_pct/brake_pct/steering`. B còn có đồng hồ nhanh **1.500 ms**, trong khi A không lệch; số dòng thô cũng khác nhau: A **3.021**, B **3.234**. Bronze giữ hai nguồn riêng và giữ giá trị dạng chữ để không mất dấu schema/đơn vị gốc, không làm hỏng cả batch khi gặp giá trị thiếu hoặc vô lý, đồng thời cho phép viết lại quy tắc chuẩn hóa mà không cần ingest lại video/CSV.

## Bước 2 · Silver telemetry

### Câu 2

Staging đổi tốc độ B từ km/h sang m/s, bàn đạp từ phần trăm về tỷ lệ, epoch-ms thành timestamp UTC và trừ offset **1.500 ms**; sau đó cast hai nguồn về cùng schema. Bằng chứng: thời điểm đầu B là `2025-10-08 14:30:00.009+00:00`, chỉ lệch 9 ms so với video, tốc độ cực đại hợp lệ là **71 km/h (A)** và **100 km/h (B)**. Mỗi drive có quarantine: `duplicate=20`, `speed_missing=30`, `speed_out_of_range=3`; dữ liệu sạch lần lượt **2.968** và **3.181**, nên sạch + 53 rejects đúng bằng 3.021/3.234 dòng nhận vào. Rejects giữ lại dòng lỗi và lý do để audit/sửa luật/replay, thay vì xóa mất bằng chứng nguồn.

### Câu 3

`max_gap_ms` là `warn` vì mất tín hiệu tạo khoảng trống nhưng các mẫu còn lại vẫn đúng; pipeline có thể gắn `sync_lost` và tiếp tục. `in_range` là `error` vì tốc độ ngoài 0–70 m/s có thể làm sai feature, thống kê và nhãn, nên phải chặn downstream. Trong `break_gate.json`, khi nới luật làm sạch lên 1.000 m/s, test `in_range_silver_telemetry_speed_mps__70__0` bắt đúng **6 dòng**, kết quả `PASS=5 WARN=1 ERROR=1`; khôi phục luật cho `PASS=6 WARN=1 ERROR=0`. Điều này chứng minh cổng dbt là lớp phòng thủ độc lập với transformation.

## Bước 3 · Silver frames

### Câu 4

Theo `idempotency.json`, bảng có **1.246 dòng** trước và vẫn **1.246 dòng** sau hai lần chạy lại `trip-00003`; fingerprint giữ nguyên `6c7480dfde7e`. Trip có 60 khung, nhưng cách ghi `append` hai lần tạo **120 dòng (2x)**. Delta `overwrite` theo predicate `trip_id` tạo commit nguyên tử cho đúng partition: reader hoặc thấy bản cũ hoặc bản mới hoàn chỉnh. Cách “xóa rồi ghi” có khoảng hở và có thể để partition thiếu nếu tiến trình sập giữa hai thao tác.

### Câu 5

Khung đen là lỗi/mất nội dung (A **6**, B **4**) nên bị loại; khung tối vẫn còn cảnh hữu ích (A **4**, B **61**) nên chỉ gắn cờ và giữ lại. Kết quả độ nhạy dHash:

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---:|---:|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Mình chọn ngưỡng **2** như pipeline: nó bắt frozen/near-duplicate nhưng chưa tăng mạnh như ngưỡng 6, vốn có nguy cơ loại cảnh thật. Khi xe chạy chậm, các khung liên tiếp tự nhiên thay đổi ít nên dHash nhỏ; cùng một ngưỡng sẽ loại nhiều hơn ở đoạn chậm, vì vậy khi đổi FPS/camera/tốc độ cần hiệu chỉnh lại.

## Bước 4 · Gold

### Câu 6

`leakage.json` có **1.019** khung dùng được thuộc 26 trip. Chia ngẫu nhiên theo frame làm **26/26 trip** xuất hiện ở nhiều split và cả **26/26** có frame ở train lẫn test; chia theo trip của Gold có **0** trip bị cắt. Các frame liền nhau gần như giống nhau, nên split theo frame đưa “hàng xóm” của ảnh test vào train. Mô hình có thể ghi nhớ cảnh thay vì tổng quát hóa, khiến metric test đẹp giả tạo.

### Câu 7

`dataset_version=3b85ec0fa3d1` là hash của nội dung/manifest nên cùng đầu vào và luật sẽ cho cùng phiên bản, không phụ thuộc giờ chạy; cả lần chạy trực tiếp và hai DAG run đều cho cùng version. Dùng ngày giờ hoặc số đếm sẽ tạo version khác dù dữ liệu không đổi và không hỗ trợ kiểm tra tái lập. MSSV được salt vào thứ tự MD5 của trip; MSSV khác làm trip đổi split, tập ảnh theo train/val/test và manifest thay đổi, do đó hash/version khác dù raw data giống nhau.

### Câu 8

Manifest hiện tại ghi lineage: `bronze.raw_telemetry_a=0`, `bronze.raw_telemetry_b=0`, `bronze.trips_raw=0`, `bronze.video_files=1`; `silver.frames=25`, `silver.telemetry=0`, `silver.frames_flagged=0`, `silver.frame_telemetry=0`. SHA-256 video là `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa` (A) và `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc` (B); manifest hash là `ff575d1c4f0d6fb44fc1fbc323b6423e894ad3b636ca0db4bfb30d714565d459`. Ba tháng sau, các Delta version, video hash, luật drop/split, MSSV và mã pipeline/dbt cho phép kiểm tra và tái tạo đúng dataset.

## Bước 5 · Điều phối và phục vụ

### Câu 9

Với `LAB_FAIL_ONCE=trip-04868`, đúng **1** task đỏ: `silver_frames[13]`; **7** task downstream chuyển `upstream_failed`. Ảnh `airflow_failed_log.png` chứa `simulated crash while extracting trip-04868`, còn `airflow_failed_mapped_tasks.png` cho thấy 25 success/1 failed. Mình chỉ chọn **Clear task** tại map index 13; hộp thoại báo 8 affected tasks (partition đỏ + 7 downstream). Chỉ partition 13 được extract lại, sau đó DAG xanh; dữ liệu không nhân đôi nhờ overwrite theo trip và version vẫn `3b85ec0fa3d1`, kiểm tra cuối vẫn 27/27.

### Câu 10

Ở 500 xe/ngày, ba điểm nghẽn đầu tiên là: (1) một MinIO node/volume cho video và ảnh, thay bằng object storage phân tán có lifecycle/partition; (2) FFmpeg chạy tuần tự với Airflow `LocalExecutor`, parallelism 2, thay bằng worker pool Kubernetes/Celery/Ray và autoscale theo trip; (3) DuckDB file đơn + dbt và Postgres phục vụ đồng thời BI, thay bằng engine phân tán/lakehouse catalog và warehouse có compute tách storage. Đồng thời cần event ingestion, backpressure, monitoring/SLA và partition theo ngày/xe để backfill không quét toàn bộ dữ liệu.

## Câu hỏi riêng trên Metabase

SQL đã thêm vào dashboard:

```sql
select drive_id, trip_id, frames, frames_kept,
       round(100.0 * (frames - frames_kept) / nullif(frames, 0), 1) as dropped_pct,
       lighting, conditions
from marts.mart_trip_summary
order by dropped_pct desc, drive_id, trip_id
limit 10;
```

Câu hỏi trả lời “trip nào có tỷ lệ khung bị loại cao nhất?”. Kết quả là `drive_a / trip-04622`: **34/60** khung bị loại, tương đương **56,7%**; tiếp theo là `trip-03904` với **50,0%**. Đây là tín hiệu để ưu tiên kiểm tra camera/frozen frames của drive A và cảnh cụ thể trước khi huấn luyện.

## Nhìn lại

Điều bất ngờ nhất là chỉ cần split sai cấp độ frame đã làm cả 26 trip rò sang test, trong khi pipeline vẫn có thể trông “đúng” nếu chỉ nhìn số lượng split. Mình hiểu rõ hơn vai trò riêng của quarantine, quality gate và commit Delta trong khả năng audit/retry. Điểm còn muốn khảo sát thêm là chọn dHash thích nghi theo tốc độ/FPS thay vì một ngưỡng cố định. Một lỗi pipeline chưa bắt rõ là telemetry hợp lệ về range nhưng bị “kẹt” ở cùng một giá trị lâu bất thường; nên bổ sung kiểm tra sensor-stuck theo cửa sổ thời gian.
