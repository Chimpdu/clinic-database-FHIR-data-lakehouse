import os
from dotenv import load_dotenv

load_dotenv()

FHIR_BASE = os.getenv("FHIR_BASE", "https://hapi-fhir.app.cloud.cbh.kth.se/fhir").rstrip("/")
FHIR_BASIC_USER = os.getenv("FHIR_BASIC_USER")
FHIR_BASIC_PASS = os.getenv("FHIR_BASIC_PASS")
FHIR_BEARER     = os.getenv("FHIR_BEARER")

TIMEOUT = (5, 30)  # (connect, read)
HEADERS_JSON = {"Accept": "application/fhir+json", "Content-Type": "application/fhir+json"}
