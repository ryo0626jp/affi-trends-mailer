# affi_trends_mail_v1（Excel作成 → メール送信）

## 1) セットアップ
- 依存インストール:  `pip install -r requirements.txt`
- `config.json.example` を `config.json` にリネームし、楽天IDとメール設定を記入

## 2) 実行
- `python app.py`
- 成果物: `trending_affiliates.xlsx`（追記） と `run.log`
- メール設定が正しければ、Excelを添付して送信します

## 3) 9:00から3時間ごとに自動実行（Windows）
- `run_job.bat` のパスを編集
- `create_schedule.bat` を管理者として実行
