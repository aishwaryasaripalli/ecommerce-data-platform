#!/usr/bin/env bash
# Verifies the 9 Olist CSV files are in data/raw/olist/
# Run from the project root:  bash scripts/check_data.sh

DIR="$(cd "$(dirname "$0")/.." && pwd)/data/raw/olist"

FILES=(
  olist_orders_dataset.csv
  olist_customers_dataset.csv
  olist_order_items_dataset.csv
  olist_order_payments_dataset.csv
  olist_order_reviews_dataset.csv
  olist_products_dataset.csv
  olist_sellers_dataset.csv
  olist_geolocation_dataset.csv
  product_category_name_translation.csv
)

missing=0
echo "Checking $DIR"
for f in "${FILES[@]}"; do
  if [[ -f "$DIR/$f" ]]; then
    echo "  OK       $f"
  else
    echo "  MISSING  $f"
    missing=1
  fi
done

if [[ $missing -eq 1 ]]; then
  echo
  echo "Some files are missing. Download from:"
  echo "  https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
  echo "Unzip it and move all 9 CSVs into data/raw/olist/"
  exit 1
fi

echo
echo "All 9 files found. You're ready for the producer."
