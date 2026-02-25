import json
from pathlib import Path

obj = json.loads(Path('product_details/additional_info.json').read_text(encoding='utf-8'))
print('Total banks:', len(obj))

# Find banks with products
banks_with_products = []
for bank_name, bank_data in obj.items():
    if isinstance(bank_data, dict):
        products = bank_data.get('products', [])
        if products:
            banks_with_products.append((bank_name, len(products)))

print('Banks with products:', len(banks_with_products))
if banks_with_products:
    print('\nFirst 5 banks with products:')
    for bank, count in banks_with_products[:5]:
        print(f'  {bank}: {count} products')
    
    # Show structure of first product
    for bank_name, bank_data in obj.items():
        if isinstance(bank_data, dict):
            products = bank_data.get('products', [])
            if products and len(products) > 0:
                print(f'\n\nSample product structure from {bank_name}:')
                print(json.dumps(products[0], indent=2))
                break
