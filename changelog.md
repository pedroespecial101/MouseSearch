# Changelog

## 2026-08-06 — Download to Kindle Button & SampleFetch Integration (Antigravity)

- **Download to Kindle UI Button**: Added dedicated "Download to Kindle" button (`btn-warning` on search result cards and `btn-info` in book details modal) with `<i class="bi bi-tablet-fill"></i>` styling and dynamic visual progress states ("Queueing...", "Queued for Kindle").
- **Backend Queue Handoff Endpoint**: Implemented `@app.route('/api/v1/client/kindle_add', methods=['POST'])` in `app.py` to forward book details to SampleFetch orchestrator's `/api/v1/requests/direct` endpoint (`http://ts-samplefetch:8000/api/v1/requests/direct`).
- **Offline & Sleep Support**: Leveraged SampleFetch's automated queue engine for retrying Kindle SFTP transfers every 60 seconds whenever the device connects to Wi-Fi.
- **Doppler Org Cleanup**: Verified single `dev` config under Doppler project `mousesearch` (`stg` and `prd` deleted).
