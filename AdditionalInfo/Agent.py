import string

import requests
import sys
import os
from dotenv import load_dotenv
import re
import time
import json
import copy
import argparse
from pathlib import Path

load_dotenv()
from typing import Dict, List, Tuple, Any, Optional

from openai import OpenAI


class _TerminalProgressBar:
    def __init__(self, total: int, prefix: str = "", width: int = 30, stream=None):
        self.total = max(int(total or 0), 0)
        self.prefix = prefix
        self.width = max(int(width or 0), 10)
        self.stream = stream or sys.stdout
        self._last_rendered = None

    def update(self, current: int, suffix: str = "") -> None:
        if self.total <= 0:
            return

        current = max(0, min(int(current), self.total))
        ratio = current / self.total
        filled = int(self.width * ratio)
        bar = "=" * filled + "-" * (self.width - filled)
        percent = int(ratio * 100)

        line = f"{self.prefix}[{bar}] {percent:3d}% ({current}/{self.total})"
        if suffix:
            line += f" {suffix}"

        # Avoid excessive redraws if nothing changed.
        if line == self._last_rendered:
            return
        self._last_rendered = line

        self.stream.write("\r" + line)
        self.stream.flush()

    def finish(self) -> None:
        if self.total > 0:
            self.stream.write("\n")
            self.stream.flush()


def _format_elapsed(seconds: float) -> str:
    seconds = int(max(0, seconds))
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

# Sentinel used when a value cannot be found explicitly in the input text.
NOT_FOUND = "NF"

# mode 0: test a single call to extract()
# mode 1: test consistency by running the same call 10 times and comparing results
# mode 2: run the full agent on a specific bank (limited to 10 products for testing)
# mode 3: run the full agent on a specific bank (no limit, can be time consuming)
MODE = 3

PROMPT = f"""You are an intelligent document parser. Given the following Bank and a SINGLE product's fees' name and or additional info,
extract the information

Critical: the title should be weighed more when deciding the fields.

only display the item from the schema if and only if it is not null or NF. If the frequency is not explicitly stated, do not infer it.
"""

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
    "DISHONOR": {
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
        "examples": ["Account closure fee", "Notice fee", "processing fees", "Cheque Fee"],
        "when_to_use": "When fee is triggered by a specific event but doesn't fit other categories clearly, more use as default unless strong evidence suggests \"OTHER\""
    },
    "EXIT": {
        "definition": "Fees charged when leaving or closing products/services",
        "examples": ["Early exit fee", "Account closure fee", "Loan discharge fee", "Early termination fee"],
        "when_to_use": "When fee is specifically for closing, exiting, or early termination of a product, if it says pay it out or exit or close or early termination or discharge or closure or something like that then it should be weighed more heavily for this classification"
    },
    "LATE_PAYMENT": {
        "definition": "Penalties for overdue or late payments",
        "examples": ["Late payment fee", "Overdue fee", "Default fee", "Payment due reminder fee"],
        "when_to_use": "When fee is specifically a penalty for late or missed payments"
    },
    "OTHER": {
        "definition": "Fees that don't fit into any other specific category",
        "examples": ["Special processing fees", "Unusual transaction fees", "Custom service fees"],
        "when_to_use": "Only when fee cannot be classified into any other specific category, should only be used if totally unsure"
    },
    "PAYMENT": {
        "definition": "Fees for making payments or payment processing",
        "examples": ["Payment processing fee", "Electronic payment fee", "Bill payment fee"],
        "when_to_use": "When fee is charged for processing outgoing payments. If there is no additional_information, and teh tital does not specifically state: payment, do not choose this fee type."
    },
    "PERIODIC": {
        "definition": "Regularly recurring fees charged at set intervals",
        "examples": ["Monthly account fee", "Annual fee", "Quarterly maintenance fee", "Loan Account Fee"],
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

AMOUNT_STRING = (
    'String with optional "-" prefix for negatives. '
    'No currency symbols or commas. '
    '1–16 digits before decimal. '
    'At least 2 digits after decimal (more allowed if needed). '
    'Example: "123.45"'
)

# Reusable JSON Schema fragments for format enforcement (since JSON Schema has no native
# AmountString/RateString types, we enforce them via patterns).
AMOUNT_STRING_SCHEMA = {
    "type": "string",
    "pattern": r"^-?\d{1,16}\.\d{2,}$"
}

RATE_STRING_SCHEMA = {
    "type": "string",
    # Decimal only. Examples: "1", "0.2", "-0.056". No percent signs.
    "pattern": r"^-?\d+(?:\.\d+)?$"
}

ISO8601_DURATION_OR_UNKNOWN_SCHEMA = {
    "oneOf": [
        {"const": NOT_FOUND},
        {
            "type": "string",
            # Basic ISO 8601 duration support (PnYnMnDTnHnMnS). Examples: P1M, P7D, PT30M
            "pattern": r"^P(?!$)(?:\d+Y)?(?:\d+M)?(?:\d+W)?(?:\d+D)?(?:T(?:\d+H)?(?:\d+M)?(?:\d+(?:\.\d+)?S)?)?$"
        }
    ]
}

BANKING_FEE_RANGE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "min": AMOUNT_STRING_SCHEMA,
        "max": AMOUNT_STRING_SCHEMA
    },
    "required": ["min", "max"]
}

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

# JSON Schema structure for rateBased fee calculation
BANKING_FEE_RATE = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "rateType": {
            "type": "string",
            "enum": ["BALANCE", "INTEREST_ACCRUED", "TRANSACTION"]
        },
        "rate": {
            "oneOf": [
                {"const": NOT_FOUND},
                RATE_STRING_SCHEMA
            ]
        },
        "accrualFrequency": ISO8601_DURATION_OR_UNKNOWN_SCHEMA
    },
    "required": ["rateType", "rate"]
}

SCHEMA = {
        "name": "fee_additional_info_extract",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "bank": {"type": "string"},
                "product": {"type": "string"},
                "explanation": {"type": "string"},
                "extracted_fees": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            # Fields MUST be in this order: name, feeType, feeMethodUType, (fee method object), feeCap, feeCapPeriod, currency
                            # 'name' field contains a descriptive fee name derived from the additional info
                            "name": {"type": "string"},
                            "feeType": {
                                "type": "string",
                                "enum": ["CASH_ADVANCE", "DEPOSIT", "DISHONOR", "ENQUIRY", "EVENT", "EXIT", 
                                        "LATE_PAYMENT", "OTHER", "PAYMENT", "PERIODIC", "PURCHASE", "REPLACEMENT", 
                                        "TRANSACTION", "UPFRONT", "UPFRONT_PER_PLAN", "VARIATION", "WITHDRAWAL"]
                            },
                            "feeMethodUType": {
                                "type": "string",
                                "enum": ["fixedAmount", "rateBased", "variable", NOT_FOUND]
                            },
                            "fixedAmount": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "amount": {
                                        "oneOf": [
                                            {"const": NOT_FOUND},
                                            AMOUNT_STRING_SCHEMA
                                        ]
                                    }
                                },
                                "required": ["amount"]
                            },
                            "rateBased": BANKING_FEE_RATE,
                            "variable": BANKING_FEE_RANGE_SCHEMA,
                            "feeCap": {"type": "string"},
                            "feeCapPeriod": {"type": "string"},
                            "currency": {"type": "string", "pattern": r"^[A-Z]{3}$", "default": "AUD"},

                            # Short justification for why each included field was extracted.
                            # Must reference exact phrases from Additional Info (no guessing).
                            "explanation": {"type": "string"},
                        },
                        # Require these core fields for stability/consistency.
                        "required": ["name", "feeType", "feeMethodUType", "explanation"],

                        # Enforce that exactly one fee method object is present, matching feeMethodUType
                        "oneOf": [
                            {
                                "properties": {
                                    "feeMethodUType": {"const": "fixedAmount"}
                                },
                                "required": ["feeMethodUType", "fixedAmount"],
                                "not": {"anyOf": [{"required": ["rateBased"]}, {"required": ["variable"]}]}
                            },
                            {
                                "properties": {
                                    "feeMethodUType": {"const": "rateBased"}
                                },
                                "required": ["feeMethodUType", "rateBased"],
                                "not": {"anyOf": [{"required": ["fixedAmount"]}, {"required": ["variable"]}]}
                            },
                            {
                                "properties": {
                                    "feeMethodUType": {"const": "variable"}
                                },
                                "required": ["feeMethodUType", "variable"],
                                "not": {"anyOf": [{"required": ["fixedAmount"]}, {"required": ["rateBased"]}]}
                            },
                            {
                                "properties": {
                                    "feeMethodUType": {"const": NOT_FOUND}
                                },
                                "required": ["feeMethodUType"],
                                "not": {
                                    "anyOf": [
                                        {"required": ["fixedAmount"]},
                                        {"required": ["rateBased"]},
                                        {"required": ["variable"]}
                                    ]
                                }
                            }
                        ]
                        
                    }
                }
            },
            "required": ["bank", "product", "explanation", "extracted_fees"]
        }
}

class Agent:
    def __init__(self, model: str = "gpt-5.2", temperature: float = 0.7, max_tokens: int = 2048):
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError(
                "Missing OPENAI_API_KEY environment variable. "
                "Set it in your environment or in a .env file."
            )
        self.client = OpenAI(api_key=api_key)
        self._last_usage = None
    
    @staticmethod
    def _strip_null_values(obj):
        """Recursively remove keys with null/None values from dictionaries."""
        if isinstance(obj, dict):
            return {k: Agent._strip_null_values(v) for k, v in obj.items() if v is not None}
        elif isinstance(obj, list):
            return [Agent._strip_null_values(item) for item in obj]
        else:
            return obj
    
    @staticmethod
    def _reorder_fee_fields(fee_dict):
        """Reorder fee fields to match the required order: name, feeType, feeMethodUType, fee method object, feeCap, feeCapPeriod, currency, explanation"""
        ordered_fee = {}
        field_order = [
            "name",
            "feeType",
            "feeMethodUType",
            "fixedAmount",
            "rateBased",
            "variable",
            "feeCap",
            "feeCapPeriod",
            "currency",
            "explanation",
        ]
        
        # Add fields in the specified order
        for field in field_order:
            if field in fee_dict:
                ordered_fee[field] = fee_dict[field]
        
        # Add any remaining fields that weren't in our order list
        for key, value in fee_dict.items():
            if key not in ordered_fee:
                ordered_fee[key] = value
        
        return ordered_fee

    @staticmethod
    def normalize_rate_string(rate_str: str) -> str:
        """
        Normalize rate strings to decimal format.
        Examples:
            "3%" -> "0.03"
            "0.025" -> "0.025"
            "invalid" -> "NF"
        """
        if not isinstance(rate_str, str):
            return NOT_FOUND
        
        rate_str = rate_str.strip()
        
        # Handle percentage format (e.g., "3%", "2.5%")
        if rate_str.endswith('%'):
            try:
                # Remove % and convert to decimal
                percentage_value = float(rate_str[:-1])
                decimal_value = percentage_value / 100
                return f"{decimal_value:.10f}".rstrip('0').rstrip('.')
            except ValueError:
                return NOT_FOUND
        
        # Handle decimal format (e.g., "0.025")
        try:
            float(rate_str)  # Validate it's a number
            return rate_str
        except ValueError:
            return NOT_FOUND

    @staticmethod
    def _stabilize_fee_type(fee_name: str, source_text: str) -> Optional[str]:
        """Deterministically override feeType for strong keyword matches to reduce run-to-run drift."""
        haystack = f"{fee_name} {source_text}".lower()

        # EXIT / closure / payout signals
        if any(k in haystack for k in ["prepayment", "pay out", "payout", "paid out", "refinanc", "discharge", "termination", "close", "closure", "exit"]):
            return "EXIT"

        # Upfront / establishment signals
        if any(k in haystack for k in ["establishment", "application", "set up", "setup", "joining"]):
            return "UPFRONT"

        # Late / missed payment signals
        if any(k in haystack for k in ["missed payment", "late payment", "overdue", "default notice", "arrears"]):
            return "LATE_PAYMENT"

        # Dishonour / overdrawn signals
        if any(k in haystack for k in ["dishonour", "dishonor", "overdrawn", "nsf", "bounced"]):
            return "DISHONOR"

        # Basic transaction types
        if "cash advance" in haystack:
            return "CASH_ADVANCE"
        if any(k in haystack for k in ["withdrawal", "atm"]):
            return "WITHDRAWAL"
        if "deposit" in haystack:
            return "DEPOSIT"
        if any(k in haystack for k in ["enquiry", "inquiry", "balance"]):
            return "ENQUIRY"
        if "replacement" in haystack:
            return "REPLACEMENT"

        # Periodic signals
        if any(k in haystack for k in ["monthly", "per month", "annual", "per year", "yearly", "quarterly"]):
            return "PERIODIC"

        return None

    @staticmethod
    def _ensure_fee_method_shape(fee: dict) -> None:
        """Ensure feeMethodUType and the corresponding method object exist for consistent output."""
        if not isinstance(fee, dict):
            return

        fee_method = fee.get("feeMethodUType")

        # Infer feeMethodUType from present method object; otherwise mark as unknown.
        if not fee_method:
            if "fixedAmount" in fee:
                fee_method = "fixedAmount"
            elif "rateBased" in fee:
                fee_method = "rateBased"
            elif "variable" in fee:
                fee_method = "variable"
            else:
                fee_method = NOT_FOUND
            fee["feeMethodUType"] = fee_method

        # Enforce exactly one method object and ensure required subfields exist.
        if fee_method == NOT_FOUND:
            fee.pop("fixedAmount", None)
            fee.pop("rateBased", None)
            fee.pop("variable", None)
            return

        if fee_method == "fixedAmount":
            fee.pop("rateBased", None)
            fee.pop("variable", None)
            fixed = fee.get("fixedAmount")
            if not isinstance(fixed, dict):
                fixed = {}
                fee["fixedAmount"] = fixed
            amount = fixed.get("amount")
            if amount is None or (isinstance(amount, str) and not amount.strip()):
                fixed["amount"] = NOT_FOUND
            elif isinstance(amount, (int, float)):
                fixed["amount"] = f"{amount:.2f}"

        elif fee_method == "rateBased":
            fee.pop("fixedAmount", None)
            fee.pop("variable", None)
            # If the model chose rateBased but didn't include it, fall back to unknown (don't assume fixedAmount).
            if "rateBased" not in fee or not isinstance(fee.get("rateBased"), dict):
                fee["feeMethodUType"] = NOT_FOUND
                fee.pop("rateBased", None)
                return

        elif fee_method == "variable":
            fee.pop("fixedAmount", None)
            fee.pop("rateBased", None)
            if "variable" not in fee or not isinstance(fee.get("variable"), dict):
                fee["feeMethodUType"] = NOT_FOUND
                fee.pop("variable", None)
                return

        else:
            # Invalid feeMethodUType -> mark unknown and remove method objects
            fee["feeMethodUType"] = NOT_FOUND
            fee.pop("fixedAmount", None)
            fee.pop("rateBased", None)
            fee.pop("variable", None)

    def test_a_response(self) :
        # test an open ai call with a sample prompt
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": "You are an intelligent document parser. Given the following Bank and a SINGLE product's fees' additional info, extract the information thats 100% correct from that field",
                },
                {
                    "role": "user",
                    "content": "Bank: Bank of Australia\nProduct: Everyday Account\nAdditional Info: Monthly account fee of $5 waived if you deposit at least $2000 each month.",
                },
            ],
            temperature=self.temperature,
            max_completion_tokens=self.max_tokens,
        )
        return response
    
    
    
    def extract(self, bank: str, product: str, additional_info: str) -> dict:
        # Format fee type definitions for the prompt
        fee_type_guide = "\n".join([
            f"• {key}: {value['definition']}\n  Examples: {', '.join(value['examples'])}\n  Use when: {value['when_to_use']}"
            for key, value in FEE_TYPE_DEFINITIONS.items()
        ])
        
        system_prompt = (
            "Extract ONLY information explicitly stated in Additional Info. "
            "Do NOT infer or guess. "
            "Return ONLY valid JSON (no markdown, no explanation). "
            f"IMPORTANT: Omit any field with null/{NOT_FOUND} value - do not include it in the output. "
            "EXCEPTION: You MUST always include feeType and feeMethodUType. "
            f"If the fee method cannot be determined from the text, set feeMethodUType to '{NOT_FOUND}' and OMIT fixedAmount/rateBased/variable entirely. "
            f"If a method IS determined but the amount/rate/range is not explicitly stated, use the literal string '{NOT_FOUND}' inside the required method object.\n\n"
            f"{DATA_TYPE_DEFINITIONS}\n\n"
            "FEE TYPE CLASSIFICATION GUIDE:\n"
            f"{fee_type_guide}\n\n"
            "You MUST follow this exact JSON schema:\n"
            f"{json.dumps(SCHEMA['schema'], indent=2)}\n\n"
            "Key rules:\n"
            "- The 'name' field in each fee MUST be the original name of the fee\n"
            "- Fields MUST appear in this exact order: name, feeType, feeMethodUType, (fixedAmount/rateBased/variable), feeCap, feeCapPeriod, currency, explanation\n"
            "- feeMethodUType determines which fee calculation method to use (only ONE of: fixedAmount, rateBased, or variable)\n"
            f"- If feeMethodUType='{NOT_FOUND}', include NO fee method object\n"
            "- If feeMethodUType='fixedAmount', include ONLY the fixedAmount object\n"
            "- If feeMethodUType='rateBased', include ONLY the rateBased object\n"
            "- If feeMethodUType='variable', include ONLY the variable object\n"
            f"- Do NOT include fields with null/{NOT_FOUND} values (except within the required fee method object)\n"
            "- The 'explanation' field is REQUIRED (top-level and for each fee). It must briefly justify each included field using exact phrases from the Additional Info (quote them). Do not infer.\n"
            "- Use the FEE TYPE CLASSIFICATION GUIDE above to select the most appropriate feeType"
        )

        payload = {
            "bank": bank,
            "product": product,
            "additional_info": additional_info,
        }

        # Try to improve repeatability if the client supports seeding.
        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                temperature=0,          # determinism
                top_p=1,
                presence_penalty=0,
                frequency_penalty=0,
                response_format={"type": "json_object"},  # JSON mode
                max_completion_tokens=self.max_tokens,
                seed=0,
            )
        except TypeError:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                temperature=0,          # determinism
                top_p=1,
                presence_penalty=0,
                frequency_penalty=0,
                response_format={"type": "json_object"},  # JSON mode
                max_completion_tokens=self.max_tokens,
            )

            # Capture token usage for progress reporting (if available).
            self._last_usage = getattr(resp, "usage", None)

        content = resp.choices[0].message.content
        obj = json.loads(content)
        
        # Strip null values from the result
        obj = self._strip_null_values(obj)

        # Ensure required explanation fields exist (avoid silently producing invalid output).
        if "explanation" not in obj or not isinstance(obj.get("explanation"), str) or not obj.get("explanation").strip():
            obj["explanation"] = "Explanation not provided by model."

        # Drop detailed explanations if the model included them.
        obj.pop("explanationDetail", None)
        
        # Normalize rates and reorder fields for each extracted fee
        if "extracted_fees" in obj:
            for fee in obj["extracted_fees"]:
                if "explanation" not in fee or not isinstance(fee.get("explanation"), str) or not fee.get("explanation").strip():
                    fee["explanation"] = "Explanation not provided by model."

                # Drop detailed explanations if the model included them.
                fee.pop("explanationDetail", None)

                # Stabilize feeType for strong keyword matches.
                fee_name = fee.get("name", "")
                override_fee_type = self._stabilize_fee_type(fee_name=fee_name, source_text=additional_info)
                if override_fee_type is not None:
                    fee["feeType"] = override_fee_type

                # Ensure feeType exists and is valid (fallback to OTHER).
                if fee.get("feeType") not in SCHEMA["schema"]["properties"]["extracted_fees"]["items"]["properties"]["feeType"]["enum"]:
                    fee["feeType"] = "OTHER"

                # Ensure consistent feeMethodUType + method object.
                self._ensure_fee_method_shape(fee)

                # Normalize rate if present in rateBased method
                if "rateBased" in fee and "rate" in fee["rateBased"]:
                    fee["rateBased"]["rate"] = self.normalize_rate_string(fee["rateBased"]["rate"])
                
                # Reorder fields to match schema order
                fee_reordered = self._reorder_fee_fields(fee)
                # Update the fee in place with reordered fields
                fee.clear()
                fee.update(fee_reordered)

        return obj
    
    def run_agent(
        self,
        bank_name: str,
        json_path: str = "product_details/combined_product_details.json",
        max_products: int = None,
        show_progress: bool = False,
    ) -> dict:
        """
        Run the agent over all products for a specific bank.
        
        Args:
            bank_name: Name of the bank to process
            json_path: Path to the combined product details JSON file
            max_products: Maximum number of products to process (None = all products)
            
        Returns:
            Dictionary with extracted fee information for all products
        """
        # Load the JSON file
        data_path = Path(json_path)
        if not data_path.exists():
            raise FileNotFoundError(f"File not found: {json_path}")
        
        with open(data_path, 'r', encoding='utf-8') as f:
            all_data = json.load(f)
        
        # Find the bank
        if bank_name not in all_data:
            available_banks = list(all_data.keys())[:10]
            raise ValueError(f"Bank '{bank_name}' not found. Available banks (first 10): {available_banks}")
        
        bank_data = all_data[bank_name]

        # Progress bar counts product entries visited (not just products with fees).
        progress_total = len(bank_data)
        if max_products is not None:
            progress_total = min(progress_total, int(max_products))
        progress = _TerminalProgressBar(
            total=progress_total,
            prefix=f"{bank_name}: ",
            width=30,
        ) if show_progress else None

        started_at = time.time()
        prompt_tokens_total = 0
        completion_tokens_total = 0

        results = {
            "bank": bank_name,
            "products": [],
            "summary": {
                "total_products": 0,
                "products_with_fees": 0,
                "total_fees_processed": 0,
                "total_fees_with_additional_info": 0,
                "total_fees_using_name_only": 0,
                "duplicate_fees_skipped_within_product": 0
            }
        }
        
        # Process each product
        products_processed = 0
        products_seen = 0
        for product_id, product_info in bank_data.items():
            # Check if we've hit the max_products limit
            if max_products is not None and products_processed >= max_products:
                break

            products_seen += 1
            if progress is not None:
                suffix = (
                    f"tok in/out: {prompt_tokens_total}/{completion_tokens_total} | "
                    f"elapsed: {_format_elapsed(time.time() - started_at)}"
                )
                progress.update(products_seen, suffix=suffix)
                
            if not isinstance(product_info, dict):
                continue
                
            body = product_info.get('body', {})
            data = body.get('data', {})
            
            product_name = data.get('name', 'Unknown Product')
            brand_name = data.get('brandName', bank_name)
            fees = data.get('fees', [])
            
            results["summary"]["total_products"] += 1
            
            if not fees:
                continue
                
            results["summary"]["products_with_fees"] += 1
            
            product_result = {
                "product_id": product_id,
                "product_name": product_name,
                "extracted_fees": []
            }
            
            # Track fee names within this product to prevent duplicates
            seen_fee_names = set()
            
            # Process each fee
            for fee in fees:
                if not isinstance(fee, dict):
                    continue
                    
                results["summary"]["total_fees_processed"] += 1
                
                # Get additional info, fallback to fee name if empty
                additional_info = fee.get('additionalInfo', '').strip()
                using_name_only = False
                
                if not additional_info:
                    # If no additional info, use the fee name as context
                    additional_info = fee.get('name', '').strip()
                    if not additional_info:
                        # Skip if both additional info and name are empty
                        continue
                    using_name_only = True
                    results["summary"]["total_fees_using_name_only"] += 1
                else:
                    results["summary"]["total_fees_with_additional_info"] += 1
                
                # Get the original fee name
                fee_name = fee.get('name', 'Unknown')
                
                # Skip if this fee name already exists in the current product
                if fee_name in seen_fee_names:
                    results["summary"]["duplicate_fees_skipped_within_product"] += 1
                    continue
                
                # Run the extraction
                try:
                    extracted = self.extract(
                        bank=brand_name,
                        product=product_name,
                        additional_info=additional_info
                    )

                    # Update token counters (best-effort; depends on SDK/model support).
                    usage = getattr(self, "_last_usage", None)
                    if usage is not None:
                        prompt_tokens_total += int(getattr(usage, "prompt_tokens", 0) or 0)
                        completion_tokens_total += int(getattr(usage, "completion_tokens", 0) or 0)
                        if progress is not None:
                            suffix = (
                                f"tok in/out: {prompt_tokens_total}/{completion_tokens_total} | "
                                f"elapsed: {_format_elapsed(time.time() - started_at)}"
                            )
                            progress.update(products_seen, suffix=suffix)

                    # Extract fees from the nested structure and flatten
                    if "extracted_fees" in extracted and isinstance(extracted["extracted_fees"], list):
                        if len(extracted["extracted_fees"]) > 0:
                            for extracted_fee in extracted["extracted_fees"]:
                                # Override the 'name' field with the original fee name from API
                                extracted_fee["name"] = fee_name
                                # Ensure required explanation exists
                                if "explanation" not in extracted_fee or not isinstance(extracted_fee.get("explanation"), str) or not extracted_fee.get("explanation").strip():
                                    extracted_fee["explanation"] = "Explanation not provided by model."

                                # Drop detailed explanations if the model included them.
                                extracted_fee.pop("explanationDetail", None)

                                # Stabilize/validate feeType and ensure fee method fields exist
                                override_fee_type = self._stabilize_fee_type(fee_name=fee_name, source_text=additional_info)
                                if override_fee_type is not None:
                                    extracted_fee["feeType"] = override_fee_type
                                if extracted_fee.get("feeType") not in SCHEMA["schema"]["properties"]["extracted_fees"]["items"]["properties"]["feeType"]["enum"]:
                                    extracted_fee["feeType"] = "OTHER"
                                self._ensure_fee_method_shape(extracted_fee)

                                # Reorder fields to match required order
                                ordered_fee = self._reorder_fee_fields(extracted_fee)
                                product_result["extracted_fees"].append(ordered_fee)
                            # Mark this fee name as seen for this product AFTER successful extraction
                            seen_fee_names.add(fee_name)
                        else:
                            # If AI returned empty array, still add fee with just the name
                            product_result["extracted_fees"].append({
                                "name": fee_name,
                                "feeType": "OTHER",
                                "feeMethodUType": NOT_FOUND,
                                "explanation": "Model returned no extracted fees; recorded fee name only."
                            })
                            seen_fee_names.add(fee_name)
                    else:
                        # If extracted_fees key is missing, still add fee with just the name
                        product_result["extracted_fees"].append({
                            "name": fee_name,
                            "feeType": "OTHER",
                            "feeMethodUType": NOT_FOUND,
                            "explanation": "Model response missing extracted_fees; recorded fee name only."
                        })
                        seen_fee_names.add(fee_name)
                except Exception as e:
                    # On error, still record the fee with error info
                    product_result["extracted_fees"].append({
                        "name": fee_name,
                        "feeType": "OTHER",
                        "feeMethodUType": NOT_FOUND,
                        "explanation": "Extraction failed; recorded error details.",
                        "error": str(e),
                        "original_additional_info": additional_info
                    })
                    seen_fee_names.add(fee_name)
            
            # Only add product if it has extracted fees
            if product_result["extracted_fees"]:
                results["products"].append(product_result)
            
            # Increment products processed counter
            products_processed += 1

        if progress is not None:
            progress.finish()
        
        return results
    

def main():

    if MODE == 0:
        agent = Agent(temperature=0)
        response = agent.extract(
            bank="Bank of Australia",
            product="Everyday Account",
            additional_info="Monthly account fee of $5",
        )
        print(json.dumps(response, indent=2))

    elif MODE == 1:
        agent = Agent(temperature=0)
        
        # run agent 10 times to test consistency
        prev_result = None
        all_same = True
        
        for i in range(1, 11):
            result = agent.extract(
                bank="Bank of Australia",
                product="Everyday Account",
                additional_info="Monthly account fee of $5"
            )
            
            print(f"\n=== Run {i} ===")
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
            
            if prev_result is not None:
                if result == prev_result:
                    print(f"✓ Run {i} matches Run {i-1}")
                else:
                    print(f"✗ Run {i} DIFFERS from Run {i-1}")
                    all_same = False
            
            prev_result = result
        
        print(f"\n{'='*50}")
        if all_same:
            print("✓ All 10 runs produced identical results")
        else:
            print("✗ Results varied across runs")
    
    elif MODE == 2:
        # Test run_agent for a specific bank (limited to 3 products for testing)
        agent = Agent(temperature=0)
        bank_name = "Westpac" 
        
        print(f"Processing bank: {bank_name} (max 3 products)")
        results = agent.run_agent(bank_name, max_products=3)
        
        # Print summary
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        print(json.dumps(results["summary"], indent=2))
        
        # Print results
        print(f"\n{'='*60}")
        print("EXTRACTED FEES")
        print(f"{'='*60}")
        for product in results["products"][:3]:  # Show first 3 products
            print(f"\nProduct: {product['product_name']}")
            print(f"Product ID: {product['product_id']}")
            print(f"Number of fees: {len(product['extracted_fees'])}")
            for fee in product['extracted_fees']:
                print(f"\n  Fee: {fee.get('name', 'Unknown')}")
                if 'error' in fee:
                    print(f"  Error: {fee.get('error', 'Unknown error')}")
                else:
                    print(f"  Extracted data:")
                    print(f"    {json.dumps(fee, indent=6)}")
        
        # Save to file
        output_path = Path(f"output_{bank_name.replace(' ', '_').replace('.', '_')}_test10.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\n{'='*60}")
        print(f"Full results saved to: {output_path}")
        print(f"Note: Only processed first 3 products for testing")
    
    elif MODE == 3:
        # Full run_agent for a specific bank (NO LIMIT - processes all products)
        agent = Agent(temperature=0)
        bank_name = "Westpac" 
        
        print(f"Processing bank: {bank_name} (ALL PRODUCTS - this may take a while)")
        results = agent.run_agent(bank_name, show_progress=True)  # No max_products limit
        
        # Print summary
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        print(json.dumps(results["summary"], indent=2))
        
        # Print results (first 3 products only for display)
        print(f"\n{'='*60}")
        print("EXTRACTED FEES (showing first 3 products)")
        print(f"{'='*60}")
        for product in results["products"][:3]:
            print(f"\nProduct: {product['product_name']}")
            print(f"Product ID: {product['product_id']}")
            print(f"Number of fees: {len(product['extracted_fees'])}")
            for fee in product['extracted_fees'][:2]:  # Show first 2 fees per product
                print(f"\n  Fee: {fee.get('name', 'Unknown')}")
                if 'error' in fee:
                    print(f"  Error: {fee.get('error', 'Unknown error')}")
                else:
                    print(f"  Extracted data:")
                    print(f"    {json.dumps(fee, indent=6)}")
        
        # Save to file
        output_path = Path(f"output_{bank_name.replace(' ', '_').replace('.', '_')}_full.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\n{'='*60}")
        print(f"Full results saved to: {output_path}")
        print(f"Processed {results['summary']['total_products']} products total")


if __name__ == "__main__":
    main()

