concat_CSV.pyとDRM_aggregate.pyをCSVデータと同じフォルダに格納し，
powershellから以下のコードを実行すると，CSVファイルが結合されたファイル（all_data.csv）と，
集計されたエクセルファイル（all_data.xlsx）が出力されます。


powershellコード（実行にはpythonが必要です）
python .\DRM_aggregate.py "." --pattern "*.csv" --start 2 --end 256 --subj-col "subj_id" --truth-col "item_type" --resp-col "response" --rt-col "rt"


