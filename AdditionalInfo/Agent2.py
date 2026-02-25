import json
import time
import re
from openai import OpenAI

# ------------- CONFIG -------------
API_KEY = "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"  # put your real key here
BANK_NAMES = ["CommBank", "ANZ", "NATIONAL AUSTRALIA BANK", "Westpac"]
JSON_FILE = "./fees_data.json"

FEE_TYPES = [
    "CASH_ADVANCE", "DEPOSIT", "DISHONOUR", "ENQUIRY", "EVENT", "EXIT", "LATE_PAYMENT", "OTHER", "PAYMENT", "PERIODIC",
    "PURCHASE", "REPLACEMENT", "TRANSACTION", "UPFRONT", "UPFRONT_PER_PLAN", "VARIATION", "WITHDRAWAL"
]

# Fee Type Definitions and Context for AI Agent
FEE_TYPE_DEFINITIONS = {
    "CASH_ADVANCE": {
        "definition": "Fees charged for cash advance transactions, typically on credit cards",
        "examples": ["Cash advance fee", "ATM cash advance", "Over-the-counter cash advance"],
        "when_to_use": "When fee is specifically for obtaining cash using a credit card or credit facility"
    },
    "DEPOSIT": {
        "definition": "Fees related to making deposits into accounts",
        "examples": ["Deposit processing fee", "Cash deposit fee", "Cheque deposit fee"],
        "when_to_use": "When fee is charged for depositing money into an account"
    },
    "DISHONOUR": {
        "definition": "Fees for failed transactions due to insufficient funds or other payment failures",
        "examples": ["Dishonour fee", "NSF fee", "Bounced cheque fee", "Failed payment fee", "Insufficient funds fee"],
        "when_to_use": "When payment fails due to insufficient funds, stopped payments, or account issues"
    },
    "ENQUIRY": {
        "definition": "Fees for account enquiries, balance checks, or information requests",
        "examples": ["Balance enquiry fee", "Statement request fee", "Account information fee"],
        "when_to_use": "When fee is for checking account information, balances, or requesting statements"
    },
    "EVENT": {
        "definition": "Fees triggered by specific account events or circumstances",
        "examples": ["Account closure fee", "Dormancy fee", "Special event processing"],
        "when_to_use": "When fee is triggered by a specific event but doesn't fit other categories clearly"
    },
    "EXIT": {
        "definition": "Fees charged when leaving or closing products/services",
        "examples": ["Early exit fee", "Account closure fee", "Loan discharge fee", "Early termination fee"],
        "when_to_use": "When fee is specifically for closing, exiting, or early termination of a product"
    },
    "LATE_PAYMENT": {
        "definition": "Penalties for overdue or late payments",
        "examples": ["Late payment fee", "Overdue fee", "Default fee", "Payment due reminder fee"],
        "when_to_use": "When fee is specifically a penalty for late or missed payments"
    },
    "OTHER": {
        "definition": "Fees that don't fit into any other specific category",
        "examples": ["Special processing fees", "Unusual transaction fees", "Custom service fees"],
        "when_to_use": "Only when fee cannot be classified into any other specific category"
    },
    "PAYMENT": {
        "definition": "Fees for making payments or payment processing",
        "examples": ["Payment processing fee", "Electronic payment fee", "Bill payment fee"],
        "when_to_use": "When fee is charged for processing outgoing payments"
    },
    "PERIODIC": {
        "definition": "Regularly recurring fees charged at set intervals",
        "examples": ["Monthly account fee", "Annual fee", "Quarterly maintenance fee"],
        "when_to_use": "When fee recurs regularly (monthly, annually, etc.) regardless of usage"
    },
    "PURCHASE": {
        "definition": "Fees related to purchase transactions",
        "examples": ["Purchase fee", "Transaction fee for purchases", "Point of sale fee"],
        "when_to_use": "When fee is specifically charged for purchase transactions"
    },
    "REPLACEMENT": {
        "definition": "Fees for replacing cards, documents, or account materials",
        "examples": ["Card replacement fee", "PIN replacement", "Statement replacement"],
        "when_to_use": "When fee is for replacing lost, stolen, or damaged items"
    },
    "TRANSACTION": {
        "definition": "General fees for various transaction types",
        "examples": ["Transaction fee", "Per-transaction charge", "Activity fee"],
        "when_to_use": "For general transaction-based fees that don't fit more specific categories"
    },
    "UPFRONT": {
        "definition": "One-time fees charged at the beginning of a service or product",
        "examples": ["Establishment fee", "Application fee", "Setup fee", "Joining fee"],
        "when_to_use": "When fee is a one-time charge at account/service establishment"
    },
    "UPFRONT_PER_PLAN": {
        "definition": "One-time fees that vary based on the specific plan or product tier chosen",
        "examples": ["Plan setup fee", "Tier-specific establishment fee", "Product-specific joining fee"],
        "when_to_use": "When upfront fee varies depending on the plan/tier selected"
    },
    "VARIATION": {
        "definition": "Fees for making changes or variations to existing accounts/services",
        "examples": ["Account variation fee", "Service change fee", "Plan modification fee"],
        "when_to_use": "When fee is charged for modifying existing account terms or services"
    },
    "WITHDRAWAL": {
        "definition": "Fees for withdrawing money from accounts",
        "examples": ["ATM withdrawal fee", "Over-the-counter withdrawal", "Early withdrawal penalty"],
        "when_to_use": "When fee is specifically for withdrawing funds from an account"
    }
}

def format_fee_type_definitions(definitions):
    """Format fee type definitions for the AI prompt"""
    formatted = []
    for fee_type, info in definitions.items():
        examples_str = ", ".join(info["examples"][:3])  # Limit to 3 examples for brevity
        formatted.append(f"• {fee_type}: {info['definition']}. Examples: {examples_str}")
    return "\n".join(formatted)

FEE_METHOD_U_TYPE = [
    "fixedAmount", "rateBased", "variable"
]

AMOUNT_STRING = (
    'String with optional "-" prefix for negatives. '
    'No currency symbols or commas. '
    '1–16 digits before decimal. '
    'At least 2 digits after decimal (more allowed if needed). '
    'Example: "123.45"'
)

# Data Type Definitions for AI Context
DATA_TYPE_DEFINITIONS = """
DATA TYPE FORMATTING RULES:
• String: Standard UTF-8 string, unrestricted content, any valid Unicode character
• Enum: All caps, spaces replaced with underscores (_), ASCII only. Examples: "OPTION1", "ANOTHER_OPTION"
• Boolean: Standard JSON boolean (true/false)
• AmountString: Monetary amount in currency units. Format: "123.45" (no symbols, min 2 decimal places, 1-16 digits before decimal)
• RateString: Percentage as decimal. Examples: "1" (100%), "0.2" (20%), "-0.056" (-5.6%)
• CurrencyString: 3-character ISO-4217 codes. Examples: "AUD", "USD", "GBP"
• NaturalNumber: Positive integer including zero (0, 1, 10000)
• PositiveInteger: Positive integer excluding zero (1, 10000)
• DateTimeString: RFC3339 format with UTC offset. Example: "2007-05-01T15:43:00.12345Z"
"""

BANKING_FEE_RATE = {
    "rateType": ["BALANCE", "INTEREST_ACCRUED", "TRANSACTION"],
    "rate": "RateString (numeric rate, e.g. '0.015' for 1.5%)",
    "accrualFrequency": "ISO 8601 Duration (e.g. 'P1M') or 'unknown'",
    "amountRange": "Valid amount range if applicable, else 'unknown'"
}


PROMPT = f"""
You are an intelligent document parser. Given the following Bank and a SINGLE product's fees' additional info,
extract the information into this JSON structure:

{{
  "bank": "",
  "products": [
    {{
      "fees": [
        {{
          "name": "",
          "feeTypeProbabilities": {{
            "CASH_ADVANCE": 0.0,
            "DEPOSIT": 0.0,
            "DISHONOUR": 0.0,
            "ENQUIRY": 0.0,
            "EVENT": 0.0,
            "EXIT": 0.0,
            "LATE_PAYMENT": 0.0,
            "OTHER": 0.0,
            "PAYMENT": 0.0,
            "PERIODIC": 0.0,
            "PURCHASE": 0.0,
            "REPLACEMENT": 0.0,
            "TRANSACTION": 0.0,
            "UPFRONT": 0.0,
            "UPFRONT_PER_PLAN": 0.0,
            "VARIATION": 0.0,
            "WITHDRAWAL": 0.0
          }},
          "feeMethodUType": "",
          // Conditional fields based on feeMethodUType:
          // If "fixedAmount": include "fixedAmount" field only
          // If "rateBased": include "rateBased" object only
          // If "variable": include "variable" object only  
          // If "unknown" or null: include NO conditional fields at all
          "feeCap": "",
          "feeCapPeriod": "",
          "currency": "AUD",
          "additionalInfo": ""
        }}
      ]
    }}
  ]
}}

- If a field cannot be determined, mark it "unknown".
- feeTypeProbabilities must contain ALL 17 fee types with independent confidence scores (0.0 to 1.0)
- Each probability represents how confident you are that the fee belongs to that specific category
- Multiple fee types can have high scores if the fee could reasonably fit multiple categories
- Higher scores indicate stronger confidence (0.9+ = very confident, 0.5+ = moderately confident, 0.1+ = possible but unlikely)
- feeMethodUType must be one of: {FEE_METHOD_U_TYPE}

FEE TYPE INDEPENDENT SCORING:
Analyze the fee description and assign independent confidence scores (0.0 to 1.0) to each fee type based on:
1. Direct keyword matches in the fee name/description
2. Contextual clues from additionalInfo
3. Common banking industry patterns
4. Fee characteristics and usage patterns

Each fee type is scored independently - probabilities do NOT need to sum to 1.0. Use these definitions to guide your scoring:

{format_fee_type_definitions(FEE_TYPE_DEFINITIONS)}

{DATA_TYPE_DEFINITIONS}

CRITICAL: Conditional field presence based on feeMethodUType:
- If feeMethodUType = "fixedAmount": ONLY include "fixedAmount" field, do NOT include "rateBased" or "variable" fields
- If feeMethodUType = "rateBased": ONLY include "rateBased" object, set "fixedAmount" to "unknown", do NOT include "variable" field
- If feeMethodUType = "variable": ONLY include "variable" object, set "fixedAmount" to "unknown", do NOT include "rateBased" field  
- If feeMethodUType = "unknown": do NOT include ANY conditional fields ("fixedAmount", "rateBased", or "variable")

rateBased structure when included:
{{
  "rateType": "",
  "rate": "",
  "accrualFrequency": "",
  "amountRange": ""
}}

variable structure when included:
{{
  "minimum": "",
  "maximum": ""
}}

Other rules:
- fixedAmount, variable values, amountRange, and feeCap must be valid {AMOUNT_STRING}
- rateBased must follow the structure above with rateType from: {BANKING_FEE_RATE["rateType"]}
- feeCap is optional, but if included must be a valid {AMOUNT_STRING}
- currency defaults to "AUD" if not specified
- For amount values, only return numeric value and unit (e.g., "1%", "20 AUD", "15.00"). No explanatory text.
- CRITICAL: All rate values must be in decimal format, NOT percentage. Examples: "0.025" not "2.5%", "0.003" not "0.3%"
- Include additionalInfo with relevant context about the fee
- CRITICAL: feeTypeProbabilities are independent scores - they do NOT need to sum to 1.0. Score each type based on its individual fit.
- CRITICAL: if all scores are very low (e.g. below 0.1), then set feeType to "Can't Determine"
- A fee can have high scores (0.8+) for multiple types if it genuinely fits multiple categories
- Only return valid JSON. No explanations or commentary

"""

# ----------------------------------

def normalize_rate_string(rate_str: str) -> str:
    """
    Convert percentage strings to decimal rate strings.
    Examples: "3%" -> "0.03", "0.5%" -> "0.005", "0.025" -> "0.025"
    """
    if not isinstance(rate_str, str):
        return str(rate_str)
    
    # Remove whitespace
    rate_str = rate_str.strip()
    
    # If it's already "unknown", return as is
    if rate_str.lower() == "unknown":
        return "unknown"
    
    # Check if it contains a percentage sign
    if "%" in rate_str:
        # Extract the numeric part
        numeric_part = re.sub(r'[^\d.-]', '', rate_str)
        try:
            # Convert percentage to decimal
            percentage_value = float(numeric_part)
            decimal_value = percentage_value / 100.0
            return str(decimal_value)
        except ValueError:
            return rate_str  # Return original if conversion fails
    
    # If it's already a decimal (no %), validate and return
    try:
        float(rate_str)
        return rate_str
    except ValueError:
        return rate_str  # Return original if not a valid number


def normalize_rates_in_product(product_data: dict) -> dict:
    """
    Normalize all rate fields in a product to ensure consistent decimal format.
    Apply conditional field logic based on feeMethodUType.
    """
    if not isinstance(product_data, dict):
        return product_data
    
    # Make a deep copy to avoid modifying the original
    import copy
    normalized_product = copy.deepcopy(product_data)
    
    # Normalize rates in fees
    if "fees" in normalized_product and isinstance(normalized_product["fees"], list):
        for fee in normalized_product["fees"]:
            if isinstance(fee, dict):
                # Apply conditional field logic - completely remove irrelevant fields
                fee_method = fee.get("feeMethodUType", "unknown")
                
                if fee_method == "fixedAmount":
                    # Keep only fixedAmount, remove others completely
                    if "rateBased" in fee:
                        del fee["rateBased"]
                    if "variable" in fee:
                        del fee["variable"]
                elif fee_method == "rateBased":
                    # Keep only rateBased, remove others completely
                    fee["fixedAmount"] = "unknown"
                    if "variable" in fee:
                        del fee["variable"]
                elif fee_method == "variable":
                    # Keep only variable, remove others completely
                    fee["fixedAmount"] = "unknown"
                    if "rateBased" in fee:
                        del fee["rateBased"]
                elif fee_method == "unknown" or fee_method is None:
                    # Remove all method-specific fields completely - no conditionals at all
                    if "fixedAmount" in fee:
                        del fee["fixedAmount"]
                    if "rateBased" in fee:
                        del fee["rateBased"]
                    if "variable" in fee:
                        del fee["variable"]
                
                # Normalize rateBased.rate if it still exists
                if "rateBased" in fee and isinstance(fee["rateBased"], dict):
                    if "rate" in fee["rateBased"]:
                        fee["rateBased"]["rate"] = normalize_rate_string(fee["rateBased"]["rate"])
                
                # Normalize any other rate fields that might exist
                for key in ["transactionRate", "balanceRate", "accruedRate"]:
                    if key in fee:
                        fee[key] = normalize_rate_string(fee[key])
    
    return normalized_product


# ----------------------------------


def load_json_safely(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except UnicodeDecodeError:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)


class ChatGptAgent:
    def __init__(self, api_key: str):
        self.client = OpenAI(api_key=api_key)

    def parse_product(self, bank_name: str, fees_list: list[str]) -> dict:
        """
        Call model for a single product using ONLY the bank name and a list
        of additionalInfo strings (fees_list). Product IDs/names are NOT
        sent to the model. Returns a single normalized product dict
        containing at minimum { "fees": [...] }.
        """
        # Keep the payload minimal to avoid leaking product names/IDs
        payload = {
            "bank": bank_name,
            "fees_additional_info": fees_list,
        }
        resp = self.client.chat.completions.create(
            model="gpt-4o",
            temperature=0,
            top_p=1,
            max_tokens=4000,
            response_format={"type": "json_object"},
            messages=[
                {"role": "user",
                 "content": f"{PROMPT}\n\nInput JSON:\n{json.dumps(payload, ensure_ascii=False)}"}
            ]
        )
        content = resp.choices[0].message.content.strip()
        if content.startswith("```"):
            lb, rb = content.find("{"), content.rfind("}")
            if lb != -1 and rb != -1:
                content = content[lb:rb+1]
        out = json.loads(content)
        products = out.get("products", [])

        # We expect exactly one product block in response
        product_block = products[0] if products else {"fees": []}
        return normalize_rates_in_product(product_block)


def build_input_dict(data: dict) -> tuple[dict, dict]:
    """
    Build:
      - input_dict: { bank: [ [fee_additionalInfo, ...] (per product, in order) ] }
      - products_order: { bank: [ original_product_dict, ... ] } preserving order
    """
    input_dict: dict[str, list[list[str]]] = {}
    products_order: dict[str, list[dict]] = {}

    for bank, products in data.items():
        input_dict[bank] = []
        products_order[bank] = []
        for product in products:
            products_order[bank].append(product)
            fees_list: list[str] = []
            for fee in product.get("fees", []):
                extra = fee.get("additionalInfo") or "unknown"
                fees_list.append(extra)
            input_dict[bank].append(fees_list)
    return input_dict, products_order

def main():
    data = load_json_safely(JSON_FILE)
    input_dict, products_order = build_input_dict(data)

    # print(input_dict)  # Optional: debug structure

    for BANK_NAME in BANK_NAMES:
        if BANK_NAME not in input_dict:
            raise KeyError(f"{BANK_NAME} not found in {JSON_FILE}")

        agent = ChatGptAgent(API_KEY)
        product_fee_lists = input_dict[BANK_NAME]          # list[list[str]]
        product_meta_list = products_order[BANK_NAME]       # list[dict]

        # run the full aggregation 1 time
        for run in range(1, 2):
            aggregated = {"bank": BANK_NAME, "products": []}

            total = len(product_fee_lists)
            for idx, fees_list in enumerate(product_fee_lists, start=1):
                try:
                    # Call model with only bank + fees list
                    product_block = agent.parse_product(BANK_NAME, fees_list)

                    # Attach metadata without sending it to the model
                    # meta = product_meta_list[idx - 1] if idx - 1 < len(product_meta_list) else {}
                    # product_block["productId"] = meta.get("productId", "unknown")
                    # product_block["productName"] = meta.get("productName", "Unknown Product")

                    aggregated["products"].append(product_block)
                    print(f"[run {run}] [{idx}/{total}] Processed product from {BANK_NAME}")
                except Exception as e:
                    print(f"[run {run}] [warn] failed for index {idx}: {e}")
                   #meta = product_meta_list[idx - 1] if idx - 1 < len(product_meta_list) else {}
                    aggregated["products"].append(
                        {
                         "fees": [],
                         "__error__": str(e)}
                    )
                time.sleep(1)

            # save this run into its own file
            filename = f"{BANK_NAME}{run}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(aggregated, f, indent=2, ensure_ascii=False)

            print(f"[run {run}] saved {filename} with {len(aggregated['products'])} products\n")



if __name__ == "__main__":
    main()
