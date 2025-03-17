import httpx
import openai
import config
import httpx_socks


transport = httpx_socks.AsyncProxyTransport.from_url(f"socks5://{config.PROXY_SERVER}:{config.PROXY_PORT}")
http_client = httpx.AsyncClient(transport=transport)

client_openai = openai.AsyncOpenAI(api_key=config.OPENAI_TOKEN, http_client=http_client)