import pytest
from unittest.mock import MagicMock, patch
from app import SearchEngine, Scraper, AIBrain

# 1. Test Search Engine (DuckDuckGo)
def test_search_engine_success(mocker):
    # Mock DDGS context manager
    mock_ddgs = mocker.patch('app.DDGS')
    mock_instance = mock_ddgs.return_value.__enter__.return_value
    mock_instance.text.return_value = [
        {'href': 'http://example1.com'},
        {'href': 'http://example2.com'}
    ]
    
    results = SearchEngine.search("flooring shanghai", max_results=2)
    assert len(results) == 2
    assert results[0] == 'http://example1.com'

# 2. Test Scraper (Content Extraction)
def test_scraper_get_context(mocker):
    mock_get = mocker.patch('app.curl_requests.get')
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"<html><body><h1>Welcome to Flooring Co</h1><p>Contact us at info@flooring.com</p></body></html>"
    mock_response.text = "Contact us at info@flooring.com"
    mock_get.return_value = mock_response
    
    # Mock trafilatura to avoid real extraction overhead
    mocker.patch('app.trafilatura.extract', return_value="Welcome to Flooring Co")
    
    context = Scraper.get_context("http://test.com", depth=1)
    assert "Welcome to Flooring Co" in context
    assert "info@flooring.com" in context

# 3. Test AI Brain (DeepSeek/OpenAI Mock)
def test_ai_brain_openai(mocker):
    # Mock OpenAI client
    mock_openai = mocker.patch('app.OpenAI')
    mock_client = mock_openai.return_value
    
    # Mock the nested chat.completions.create call
    mock_completion = MagicMock()
    mock_completion.choices[0].message.content = '{"company_name": "Test Co", "deal_score": 8, "summary": "Top flooring brand"}'
    mock_client.chat.completions.create.return_value = mock_completion
    
    brain = AIBrain(provider="DeepSeek", api_key="fake_key", model_name="deepseek-chat", base_url="http://api.deepseek.com")
    result = brain.analyze("Sample text content", "Persona", "Focus")
    
    assert result['company_name'] == "Test Co"
    assert result['deal_score'] == 8

# 4. Test Error Handling in Scraper
def test_scraper_error_handling(mocker):
    mock_get = mocker.patch('app.curl_requests.get', side_effect=Exception("Connection Failed"))
    
    context = Scraper.get_context("http://broken-site.com")
    assert "抓取错误: Connection Failed" in context

# 5. Test AI Brain with Invalid JSON
def test_ai_brain_invalid_json(mocker):
    mock_openai = mocker.patch('app.OpenAI')
    mock_client = mock_openai.return_value
    mock_completion = MagicMock()
    mock_completion.choices[0].message.content = 'This is not JSON'
    mock_client.chat.completions.create.return_value = mock_completion
    
    brain = AIBrain(provider="OpenAI", api_key="fake_key", model_name="gpt-4o")
    result = brain.analyze("text", "persona", "focus")
    
    assert "error" in result
