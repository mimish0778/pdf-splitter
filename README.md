# PDF Splitter — Flask + qpdf

複数のPDFを結合し、指定サイズ（MB）以下に分割するWebアプリです。

## 必要なもの

| ツール | インストール方法 |
|--------|----------------|
| Python 3.10+ | https://python.org |
| qpdf | `brew install qpdf` / `sudo apt install qpdf` / `choco install qpdf` |

## セットアップと起動

```bash
# 1. 依存パッケージをインストール
pip install -r requirements.txt

# 2. 起動
python app.py

# → http://localhost:5000 をブラウザで開く
```

## 使い方

1. PDFファイルをドラッグ＆ドロップ（複数可・順番はドラッグで並び替え可能）
2. 分割サイズ（MB）を設定
3. 「分割を実行する」をクリック
4. 完了後、ファイルを個別ダウンロードまたはZIPで一括ダウンロード

## 仕組み

- **結合**: `qpdf --empty --pages a.pdf b.pdf -- merged.pdf`
- **分割**: `qpdf merged.pdf --pages . 1-N -- chunk.pdf`
- **サイズ決定**: 二分探索で各チャンクの最大ページ数を実測して確定
- qpdfはPDFの内部バイナリ構造を書き換えないため **破損ゼロ**

## 本番運用する場合

```bash
pip install gunicorn
gunicorn -w 2 -b 0.0.0.0:5000 app:app
```
