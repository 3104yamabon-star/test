
# 施設空き状況 5分監視（黄色マーカーで差分をDiscord送付）

このリポジトリは、.jsp 直リンク禁止のサイトで「複数ボタン操作を経て当月の空き状況ページへ到達」する要件に対応し、5分おきに監視して差分があれば黄色マーカーで強調した画像を Discord へ送信します。Discord 投稿タイトルは必ず **「岩槻0」** になります。

## セットアップ

1. **Discord Webhook URL** を作成  
   - Discord サーバーのチャンネル設定 → Webhooks → Create New Webhook → URL をコピー

2. **GitHub Secrets** を設定  
   - `BASE_URL`: 施設予約システムのトップページURL（直リンク禁止のため、トップからクリック遷移します）
   - `DISCORD_WEBHOOK_URL`: 上で取得した Discord Webhook の URL

3. （必要に応じて）`config.json` を編集  
   - `click_sequence` は、ページ遷移のボタン/リンクの日本語ラベルです（例の手順に合わせて初期値を設定済み）。
   - 差分検出の感度（`diff_threshold_pixel`,`tile_size`,`tile_pixel_threshold`）や黄色の透明度（`yellow_alpha`）も調整可。

4. 初回コミット後、GitHub Actions の `monitor` ワークフローを確認  
   - 自動で 5 分おきに実行されます。
   - 手動実行（`workflow_dispatch`）で即時確認も可能です。

## 仕様

- ブラウザ操作は Playwright（Chromium, headless）で行います。
- 当月の空き状況ページへ遷移後、フルページスクリーンショットを撮影。
- 前回スクショ（`snapshots/latest.png`）と今回を比較し、差分領域を**黄色半透明**で矩形マーキングした画像（`snapshots/diff_YYYYMMDD_HHMMSS.png`）を生成。
- 差分があった場合のみ、Discord Webhook に画像を添付して送信。本文の先頭は必ず **「岩槻0」**。
- スナップショットはリポジトリにコミットして履歴を保持（比較用）。

## 使い方のコツ

- サイトの UI 文言が微妙に異なる場合は `config.json` の `click_sequence` を該当ラベルに合わせて変更してください。
- ページがフレーム構成・動的描画の場合でも、Playwright のテキストロケータで極力対応します。必要ならロール（`link`/`button`）や wait 条件を調整してください。
- 差分検出は**画像ベース**です。セル単位の厳密検知が必要な場合、DOM からテーブル内容を抽出して比較する拡張も可能です。

## 注意点

- GitHub Actions のスケジュールは厳密な 5 分固定ではありません（多少の遅延・前後あり）。
- 直リンク禁止（.jsp）に対応するため、毎回トップから**クリック遷移**しています。セレクタが変わると失敗するので、ラベル変更時は `config.json` を更新してください。
- 監視対象サイトの負荷を考慮し、必要以上の実行頻度や並列実行は避けてください。
