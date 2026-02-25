import json

class AdditionalInfoCombiner:
    def __init__(self):
        self.v4_data = "./product_details/get_product_detail_v4_2026-02-20.json"
        self.v5_data = "./product_details/get_product_detail_v5_2026-02-20.json"
        self.v6_data = "./product_details/get_product_detail_v6_2026-02-20.json"

    def combine(self):
        with open(self.v4_data, "r") as f:
            v4 = json.load(f)

        with open(self.v5_data, "r") as f:
            v5 = json.load(f)

        with open(self.v6_data, "r") as f:
            v6 = json.load(f)

        # combine into one dict
        combined = {}
        for brand_name, details in v4.items():
            combined[brand_name] = details
        for brand_name, details in v5.items():
            combined[brand_name] = details
        for brand_name, details in v6.items():
            combined[brand_name] = details

        with open("./product_details/combined_product_details.json", "w") as f:
            json.dump(combined, f, indent=2)

    def create_additional_info_dict(self):
        with open("./product_details/combined_product_details.json", "r") as f:
            combined = json.load(f)

        fees_by_brand: dict[str, dict[str, list]] = {}

        for brand_name, products in combined.items():
            if not isinstance(products, dict):
                continue

            brand_fees: dict[str, list] = {}

            for product_id, record in products.items():
                if not isinstance(record, dict):
                    continue

                if record.get("statusCode") not in (None, 200):
                    continue

                body = record.get("body")
                if not isinstance(body, dict):
                    continue
                data = body.get("data")
                if not isinstance(data, dict):
                    continue

                fees = data.get("fees")
                if fees is None:
                    continue

                # Fees should be a list per CDR spec, but keep it flexible.
                if not isinstance(fees, list):
                    fees = [fees]

                brand_fees[product_id] = fees

            if brand_fees:
                fees_by_brand[brand_name] = brand_fees

        with open("./product_details/fees_by_brand_product.json", "w") as f:
            json.dump(fees_by_brand, f, indent=2)
        print(f"Wrote fees for {len(fees_by_brand)} brands to ./product_details/fees_by_brand_product.json")
        

def main():
    combiner = AdditionalInfoCombiner()
    combiner.combine()
    combiner.create_additional_info_dict()

if __name__ == "__main__":
    main()