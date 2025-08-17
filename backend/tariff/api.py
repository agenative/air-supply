from fastapi import FastAPI, Depends
from .models import TariffRequest, TariffResponse
from typing import List, Dict, Any
from .country_code_repo import CountryCodeRepo
from .hs_code_repo import HSCodeRepo
from ..utils.singleton import singleton_cache
from .config import TariffConfig
import requests
import xml.etree.ElementTree as ET
import urllib.parse


# Define dependency functions with singleton cache to reuse instances
@singleton_cache
def get_tariff_config() -> TariffConfig:
    """Dependency to get CountryCodeRepo instance (singleton)"""
    return TariffConfig()

# Define dependency functions with singleton cache to reuse instances
@singleton_cache
def get_country_code_repo() -> CountryCodeRepo:
    """Dependency to get CountryCodeRepo instance (singleton)"""
    return CountryCodeRepo()

@singleton_cache
def get_hs_code_repo() -> HSCodeRepo:
    """Dependency to get HSCodeRepo instance (singleton)"""
    return HSCodeRepo()

async def find_hs_code(
    product_desc: str, 
    hs_code_repo: HSCodeRepo
) -> tuple[str, List[Dict[str, Any]]]:
    """
    Fetch HS code list from WITS API, filter by keywords, and use Gemini to select the best match.
    
    Args:
        product_desc: The product description (e.g., "wireless earbuds").
        hs_code_repo: HSCodeRepo instance for finding HS codes.
    
    Returns:
        The best-matching 6-digit HS code as a string.
    """
    result = await hs_code_repo.find_hs_codes(product_desc, top_k=1)
    return result[0]['metadata']["productcode"] if result else "000000", result[0]


async def find_country_code(
    country_name: str, 
    is_reporter: bool,
    country_code_repo: CountryCodeRepo
) -> tuple[str, List[Dict[str, Any]]]:
    """
    Find the country code from countries.csv using Gemini embedding model for semantic matching.
    Args:
        country_name: The name of the country to search for.
        is_reporter: True if the country is a reporter, False if a partner.
        country_code_repo: CountryCodeRepo instance for finding country codes.
    Returns:
        The country code as a string.
        Returns "000" if not found.
    """
    result = await country_code_repo.find_country_codes(name=country_name, top_k=1, metadata={"isreporter": "1" if is_reporter else "0"})
    return result[0]["metadata"]["countrycode"] if result else "000", result[0]


async def request_tariff_from_wits(hs_code: str, partner: str, reporter: str, target_year: int, wto_api_key: str) -> tuple[float | None, str]:
    """
    Fetches the tariff rate using WITS API with fallbacks, cross-referencing with WTO API for zero rates or bilateral tariffs.
    
    Parameters:
    - reporter: str, ISO 3-digit code for reporter country (e.g., '156' for China)
    - partner: str, ISO 3-digit code for partner country or '000' for World
    - target_year: str, the target year (e.g., '2024')
    - hs_code: str, 6-digit HS code (e.g., '851830')
    - wto_api_key: str, WTO API key for cross-referencing (optional)
    
    Returns:
    - tuple: (tariff_rate: float or None, reason: str)
    """
    base_url = "https://wits.worldbank.org/API/V1"
    availability_url = f"{base_url}/wits/datasource/trn/dataavailability/country/{reporter}/year/all"
    
    # Step 1: Fetch WITS availability data
    try:
        resp = requests.get(availability_url, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        return None, f"WITS availability check failed: {str(e)}"
    
    # Step 2: Parse XML with WITS namespace and handle UTF-8 BOM
    try:
        xml_content = resp.content.decode('utf-8-sig')  # utf-8-sig automatically skips the BOM
        root = ET.fromstring(xml_content)
        namespaces = {'wits': 'http://wits.worldbank.org'}
    except ET.ParseError as e:
        return None, f"WITS XML parsing failed: {str(e)}"
    
    # Step 3: Extract available years and partners
    available = {}  # year: list of partners
    reporters = root.findall('.//wits:reporter', namespaces)
    if not reporters:
        xml_snippet = ET.tostring(root, encoding='unicode')[:500]
        return None, f"No reporter elements found in WITS XML. Snippet: {xml_snippet}"
    
    for rep in reporters:
        year_elem = rep.find('wits:year', namespaces)
        if year_elem is None or not year_elem.text:
            continue
        year = int(year_elem.text)
        
        partner_elem = rep.find('wits:partnerlist', namespaces)
        partners = []
        if partner_elem is not None and partner_elem.text:
            partners = [p.strip() for p in partner_elem.text.split(';') if p.strip()]
        if '000' not in partners:
            partners.append('000')
        available[year] = partners
    
    if not available:
        return None, "No availability data found in WITS XML response"
    
    # Step 4: Select year and partner with fallback
    used_partner = partner
    used_year = None
    reason_parts = []
    
    sorted_years = sorted(available.keys(), reverse=True)
    for yr in sorted_years:
        if yr > int(target_year):
            continue
        if partner in available[yr] or partner == '000':
            used_year = yr
            break
    
    if used_year is None:
        used_partner = '000'
        reason_parts.append("partner 000")
        for yr in sorted_years:
            if yr > int(target_year):
                continue
            used_year = yr
            break
    
    if used_year is None:
        return None, "No available year found in WITS"
    
    if used_year != int(target_year):
        reason_parts.append(f"year {used_year}")
    
    # Step 5: Query WITS tariff with fallbacks on HS granularity
    hs_levels = [hs_code, hs_code[:4] if len(hs_code) >= 4 else hs_code, hs_code[:2] if len(hs_code) >= 2 else hs_code]
    used_hs = None
    rate = None
    last_query_url = None
    attempted_bilateral = False
    
    partners_to_try = [partner, '000'] if partner != '000' and not attempted_bilateral else ['000']
    
    for p in partners_to_try:
        for hs in hs_levels:
            query_url = f"{base_url}/SDMX/V21/datasource/TRN/reporter/{reporter}/partner/{p}/product/{hs}/year/{used_year}/datatype/reported?format=JSON"
            last_query_url = query_url
            try:
                resp = requests.get(query_url, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if 'dataSets' in data and data['dataSets']:
                        series = data['dataSets'][0].get('series', {})
                        if series:
                            first_series_key = next(iter(series), None)
                            if first_series_key:
                                observations = series[first_series_key].get('observations', {})
                                if observations and '0' in observations:
                                    first_obs = observations['0']
                                    if isinstance(first_obs, list) and len(first_obs) > 0 and first_obs[0] is not None:
                                        try:
                                            rate = float(first_obs[0])
                                            used_hs = hs
                                            used_partner = p
                                            break
                                        except (ValueError, TypeError):
                                            return None, f"Invalid WITS tariff rate format for HS {hs}, partner {p}: {first_obs[0]} at {query_url}"
                    if rate == 0.0:
                        reason_parts.append(f"zero WITS rate for HS {hs}, partner {p}")
                if p == partner:
                    attempted_bilateral = True
            except (requests.RequestException, ValueError, KeyError):
                continue
        if rate is not None and rate != 0.0:
            break
    
    if rate is None:
        return None, f"No tariff data found in WITS after all fallbacks at {last_query_url}"
    
    if used_hs != hs_code:
        reason_parts.append(f"HS code {used_hs}")
    if used_partner != partner:
        reason_parts.append(f"partner {used_partner}")
    
    reason = "WITS: No fallback needed" if not reason_parts else f"WITS: Fallback to " + " and ".join(reason_parts) + f" at {last_query_url}"
    
    # Step 6: Cross-reference with WTO API if rate is 0.0 or bilateral partner was requested
    wto_rate = None
    wto_reason = ""
    if (rate == 0.0 or partner != '000') and wto_api_key:
        # First, we'll fetch the available indicators to find the correct one for tariffs
        indicators_url = "https://api.wto.org/timeseries/v1/indicators?i=all&t=all&pc=all&tp=all&frq=all&lang=1"
        
        headers = {
            "Cache-Control": "no-cache",
            "Ocp-Apim-Subscription-Key": wto_api_key
        }
        
        print(f"Fetching available indicators from WTO API...")
        
        indicator_code = None
        tariff_indicator = None
        
        try:
            # Try to get the list of available indicators
            indicators_resp = requests.get(indicators_url, headers=headers, timeout=10)
            
            if indicators_resp.status_code == 200:
                indicators_data = indicators_resp.json()
                print(f"Got indicators response. Searching for tariff indicators...")
                
                # Look for indicators related to tariffs by searching their names
                # The API returns a list of indicators
                for ind in indicators_data:
                    if 'code' in ind and 'name' in ind:
                        if ('tariff' in ind['name'].lower() or 
                            'duty' in ind['name'].lower() or 
                            'tax' in ind['name'].lower()):
                            print(f"Found potential tariff indicator: {ind['code']} - {ind['name']}")
                            # Capture all potential tariff indicators
                            # Use the first one we find as fallback
                            if indicator_code is None:
                                indicator_code = ind['code']
                                tariff_indicator = ind
                            
                            # Prefer indicators with "MFN" and "average" in the name
                            # These likely represent Most Favored Nation average tariff rates
                            if ('mfn' in ind['name'].lower() and 'average' in ind['name'].lower()):
                                indicator_code = ind['code']
                                tariff_indicator = ind
                                break
                
                if indicator_code:
                    # Get indicator name from the tariff indicator
                    indicator_name = tariff_indicator.get('name', 'Unknown')
                    print(f"Using indicator code: {indicator_code} ({indicator_name})")
                else:
                    print("No suitable tariff indicator found in the response.")
                    # We can't proceed without an indicator code
                    return None, "No suitable tariff indicator found in WTO API response"
            else:
                print(f"Failed to get indicators: {indicators_resp.status_code}, {indicators_resp.content}")
                # Can't proceed with WTO API check without valid indicators
                return None, f"Failed to get indicators from WTO API: HTTP {indicators_resp.status_code}"
        except Exception as e:
            print(f"Error fetching indicators: {str(e)}")
            # Can't proceed with WTO API check without valid indicators
            return None, f"Error fetching indicators from WTO API: {str(e)}"
        
        # Build the URL with proper encoding using urllib.parse
        base_url = "https://api.wto.org/timeseries/v1/data"
        
        # Need to check if the selected indicator supports partner and product dimensions
        # Check indicator metadata for supported dimensions
        print(f"Checking dimensions for indicator: {indicator_code}")
        
        # Start with basic parameters that should work for any indicator
        params = {
            "i": indicator_code,
            "r": reporter if reporter else "all",
            "fmt": "json",
            "mode": "full",
            "dec": "default",
            "off": "0",
            "max": "500",
            "head": "H",
            "lang": "1",
            "meta": "false"
        }
        
        # Check if this indicator might have partner and product dimensions
        # Based on the name/description, some indicators like MFN tariff rates might support these
        has_partner_dim = False
        has_product_dim = False
        
        # If indicator name contains keywords suggesting it supports product/partner dimensions
        ind_name = tariff_indicator.get('name', '').lower()
        if 'bilateral' in ind_name or 'partner' in ind_name:
            has_partner_dim = True
            params["p"] = partner if partner else "default"
            params["ps"] = "default"
        
        if 'product' in ind_name or 'sector' in ind_name or 'hs' in ind_name or 'harmonized' in ind_name:
            has_product_dim = True
            params["pc"] = hs_code if hs_code else "default"
            params["spc"] = "false"
        
        print(f"Using dimensions - partner: {has_partner_dim}, product: {has_product_dim}")
        
        # Manually encode parameters
        query_string = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items())
        wto_url = f"{base_url}?{query_string}"
        
        # Print the URL for debugging
        print(f"Making WTO API request to: {wto_url}")
        
        try:
            # Use the correct header format as shown in the example
            headers = {
                "Cache-Control": "no-cache",
                "Ocp-Apim-Subscription-Key": wto_api_key
            }
            
            # Try the URL
            resp = requests.get(wto_url, headers=headers, timeout=10)
            print(f"WTO API Response status: {resp.status_code}")
            
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    # Log the raw response for debugging (limited length to avoid huge logs)
                    print(f"WTO API Response: {str(data)[:1000]}")
                    
                    if 'Dataset' in data and data['Dataset']:
                        # Find the tariff rate in the new response format
                        if data['Dataset'] and len(data['Dataset']) > 0:
                            # Navigate through the dataset structure to find the value
                            for item in data['Dataset']:
                                if 'Value' in item:
                                    wto_rate = float(item['Value'])
                                    wto_reason = f"WTO: Rate {wto_rate} for HS {hs_code}, partner {partner} at {wto_url}"
                                    if wto_rate != rate:
                                        reason += f". Discrepancy detected! {wto_reason}"
                                    else:
                                        reason += f". WTO confirms rate: {wto_reason}"
                                    break
                except (ValueError, KeyError) as e:
                    wto_reason = f"Failed to parse WTO response: {str(e)}"
                    reason += f". {wto_reason}"
            else:
                # Detailed error for non-200 responses
                try:
                    error_content = resp.content.decode('utf-8')
                    print(f"WTO API Error: {error_content}")
                    
                    # Check for dimension errors specifically
                    if "does not have a partner dimension" in error_content or "does not have a product/sector dimension" in error_content:
                        print("Indicator does not support requested dimensions. Retrying with basic parameters...")
                        
                        # Retry with only the essential parameters
                        simplified_params = {
                            "i": indicator_code,
                            "r": reporter if reporter else "all",
                            "fmt": "json",
                            "mode": "full",
                            "lang": "1"
                        }
                        
                        # Encode simplified parameters
                        simplified_query = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in simplified_params.items())
                        simplified_url = f"{base_url}?{simplified_query}"
                        
                        print(f"Retrying with simplified URL: {simplified_url}")
                        
                        # Try the simplified URL
                        retry_resp = requests.get(simplified_url, headers=headers, timeout=10)
                        
                        if retry_resp.status_code == 200:
                            try:
                                retry_data = retry_resp.json()
                                print(f"Simplified WTO API Response: {str(retry_data)[:1000]}")
                                
                                # Process the simplified response
                                if 'Dataset' in retry_data and retry_data['Dataset']:
                                    for item in retry_data['Dataset']:
                                        if 'Value' in item:
                                            wto_rate = float(item['Value'])
                                            wto_reason = f"WTO: General rate {wto_rate} at {simplified_url}"
                                            reason += f". Used general tariff rate: {wto_reason}"
                                            break
                            except Exception as retry_e:
                                print(f"Error processing simplified response: {str(retry_e)}")
                        else:
                            reason += f". WTO API error: {error_content[:200]}"
                    else:
                        reason += f". WTO API error: {error_content[:200]}"
                except Exception as e:
                    reason += f". WTO API error: Status {resp.status_code}"
        except (requests.RequestException, ValueError, KeyError) as e:
            wto_reason = f"WTO API check failed: {str(e)}"
            reason += f". {wto_reason}"
    
    # Warn if rate is 0.0
    if rate == 0.0:
        reason += ". Warning: Zero rate may indicate duty-free status (e.g., WTO ITA for electronics) or missing data; verify with WTO Tariff Download Facility (ttd.wto.org) or Transcustoms.com."
    
    # Return WTO rate if non-zero and different from WITS, else WITS rate
    final_rate = wto_rate if wto_rate is not None and wto_rate != 0.0 and wto_rate != rate else rate
    if final_rate != rate:
        reason += f". Using WTO rate {final_rate} due to non-zero value."
    
    return final_rate, reason

app = FastAPI()

# FastAPI dependencies are injected at the endpoint level and passed explicitly to helper functions
# The repositories are singleton-cached to avoid creating multiple instances for every request


@app.post("/tariff", response_model=TariffResponse)
async def get_tariff(
    req: TariffRequest,
    hs_code_repo: HSCodeRepo = Depends(get_hs_code_repo),
    country_code_repo: CountryCodeRepo = Depends(get_country_code_repo),
    tariff_config: TariffConfig = Depends(get_tariff_config)
):
    # Stage 1: Get HS Code using Google ADK agent
    hs_code, hs_code_ref = await find_hs_code(req.product, hs_code_repo)
    reporter_code, reporter_ref = await find_country_code(req.reporter, is_reporter=True, country_code_repo=country_code_repo)
    partner_code, partner_ref = await find_country_code(req.partner, is_reporter=False, country_code_repo=country_code_repo)

    # Stage 2: Get tariff from WITS with fallback mechanism
    rate, reason = await request_tariff_from_wits(hs_code, partner_code, reporter_code, req.year, tariff_config.wto_api_key)

    # Stage 3: Format and return response with structured reason dict
    return TariffResponse(
        hs_code=hs_code,
        reason={
            "hs_code_ref": hs_code_ref,
            "reporter_code_ref": reporter_ref,
            "partner_code_ref": partner_ref,
            "tariff_fallback": reason
        },
        tariff=rate,
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.tariff.api:app", host="0.0.0.0", port=8000, reload=True)
