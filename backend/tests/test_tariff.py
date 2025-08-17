from backend.tariff.api import get_tariff, find_country_code, find_hs_code, get_country_code_repo, get_hs_code_repo, get_tariff_config
from backend.tariff.models import TariffRequest, TariffResponse
import pytest

@pytest.mark.asyncio
async def test_get_tariff():
    # Create a TariffRequest object
    request = TariffRequest(
        product="wireless earbuds",
        partner="Iraq",
        reporter="Brazil",
        year=2021
    )
    
    # Get the repository instances
    hs_code_repo = get_hs_code_repo()
    country_code_repo = get_country_code_repo()
    tariff_config = get_tariff_config()
    
    # Call the get_tariff function with the request and repository dependencies
    result = await get_tariff(request, hs_code_repo, country_code_repo, tariff_config)
    
    # Verify the response structure
    assert isinstance(result, TariffResponse)
    assert hasattr(result, "hs_code")
    assert hasattr(result, "reason")
    assert hasattr(result, "tariff")
    
    # Verify the expected values based on the captured good result
    assert result.hs_code == "851830"
    assert isinstance(result.reason, dict)
    assert result.tariff == 13.4
    
    # Detailed assertions for hs_code_ref
    assert "hs_code_ref" in result.reason
    assert result.reason["hs_code_ref"]["content"].startswith("851830 --  - Headphones and earphones")
    assert result.reason["hs_code_ref"]["metadata"]["productcode"] == "851830"
    assert result.reason["hs_code_ref"]["metadata"]["isgroup"] == "No"
    assert result.reason["hs_code_ref"]["metadata"]["nomenclaturecode"] == "HS"
    assert result.reason["hs_code_ref"]["metadata"]["grouptype"] == "N/A"
    
    # Detailed assertions for reporter_code_ref (Brazil)
    assert "reporter_code_ref" in result.reason
    assert result.reason["reporter_code_ref"]["content"] == "Brazil"
    assert result.reason["reporter_code_ref"]["metadata"]["countrycode"] == "076"
    assert result.reason["reporter_code_ref"]["metadata"]["isreporter"] == "1"
    assert result.reason["reporter_code_ref"]["metadata"]["ispartner"] == "1"
    assert result.reason["reporter_code_ref"]["metadata"]["iso3Code"] == "BRA"
    
    # Detailed assertions for partner_code_ref (Iraq)
    assert "partner_code_ref" in result.reason
    assert result.reason["partner_code_ref"]["content"] == "Iraq"
    assert result.reason["partner_code_ref"]["metadata"]["countrycode"] == "368"
    assert result.reason["partner_code_ref"]["metadata"]["isreporter"] == "0"
    assert result.reason["partner_code_ref"]["metadata"]["ispartner"] == "1"
    assert result.reason["partner_code_ref"]["metadata"]["iso3Code"] == "IRQ"
    
    # Detailed assertion for tariff_fallback
    assert "tariff_fallback" in result.reason
    assert isinstance(result.reason["tariff_fallback"], str)
    assert "Fallback to partner 000" in result.reason["tariff_fallback"]
    # The message doesn't contain "year 2021" explicitly in the message text
    assert "reporter/076/partner/000/product/851830/year/2021" in result.reason["tariff_fallback"]


@pytest.mark.asyncio
async def test_get_tariff_with_wto_cross_referencing():
    # Create a TariffRequest object
    request = TariffRequest(
        product="wireless earbuds",
        partner="China",
        reporter="USA",
        year=2024
    )
    
    # Get the repository instances
    hs_code_repo = get_hs_code_repo()
    country_code_repo = get_country_code_repo()
    tariff_config = get_tariff_config()
    
    # Call the get_tariff function with the request and repository dependencies
    result = await get_tariff(request, hs_code_repo, country_code_repo, tariff_config)
    
    # Verify the response structure
    assert isinstance(result, TariffResponse)
    assert hasattr(result, "hs_code")
    assert hasattr(result, "reason")
    assert hasattr(result, "tariff")
    
    # Verify the expected values based on the captured good result
    assert result.hs_code == "851830"
    assert isinstance(result.reason, dict)
    assert result.tariff == 3.3
    
    # Detailed assertions for hs_code_ref
    assert "hs_code_ref" in result.reason
    assert result.reason["hs_code_ref"]["content"].startswith("851830 --  - Headphones and earphones")
    assert result.reason["hs_code_ref"]["metadata"]["productcode"] == "851830"
    assert result.reason["hs_code_ref"]["metadata"]["isgroup"] == "No"
    assert result.reason["hs_code_ref"]["metadata"]["nomenclaturecode"] == "HS"
    assert result.reason["hs_code_ref"]["metadata"]["grouptype"] == "N/A"
    
    # Detailed assertions for reporter_code_ref (Brazil)
    assert "reporter_code_ref" in result.reason
    assert result.reason["reporter_code_ref"]["content"] == "United States"
    assert result.reason["reporter_code_ref"]["metadata"]["countrycode"] == "840"
    assert result.reason["reporter_code_ref"]["metadata"]["isreporter"] == "1"
    assert result.reason["reporter_code_ref"]["metadata"]["ispartner"] == "1"
    assert result.reason["reporter_code_ref"]["metadata"]["iso3Code"] == "USA"
    
    # Detailed assertions for partner_code_ref (Iraq)
    assert "partner_code_ref" in result.reason
    assert result.reason["partner_code_ref"]["content"] == "Iraq"
    assert result.reason["partner_code_ref"]["metadata"]["countrycode"] == "368"
    assert result.reason["partner_code_ref"]["metadata"]["isreporter"] == "0"
    assert result.reason["partner_code_ref"]["metadata"]["ispartner"] == "1"
    assert result.reason["partner_code_ref"]["metadata"]["iso3Code"] == "IRQ"
    
    # Detailed assertion for tariff_fallback
    assert "tariff_fallback" in result.reason
    assert isinstance(result.reason["tariff_fallback"], str)
    assert "Fallback to partner 000" in result.reason["tariff_fallback"]
    # The message doesn't contain "year 2021" explicitly in the message text
    assert "reporter/840/partner/000/product/851830/year/2021" in result.reason["tariff_fallback"]
    assert "Using WTO rate 3.3 due to non-zero value" in result.reason["tariff_fallback"]


@pytest.mark.asyncio
async def test_find_country_code():
    # Test the find_country_code function
    country_code_repo = get_country_code_repo()
    code, ref = await find_country_code("United States", is_reporter=True, country_code_repo=country_code_repo)
    assert isinstance(code, str)
    assert code == "840"
    assert isinstance(ref, dict)
    assert "content" in ref
    assert "metadata" in ref

@pytest.mark.asyncio
async def test_find_hs_code():
    # Test the find_hs_code function
    hs_code_repo = get_hs_code_repo()
    code, ref = await find_hs_code("wireless earbuds", hs_code_repo)
    assert isinstance(code, str)
    assert code == '851830'
    assert isinstance(ref, dict)
    assert "content" in ref
    assert "metadata" in ref