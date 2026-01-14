import os
from app.core.config import get_settings

# Force load of .env
print(f"Loading settings...")
settings = get_settings()
print(f"CORS_ORIGINS type: {type(settings.cors_origins)}")
print(f"CORS_ORIGINS value: {settings.cors_origins}")

if isinstance(settings.cors_origins, list):
    print(f"List length: {len(settings.cors_origins)}")
    if len(settings.cors_origins) > 0:
         print(f"First item: {settings.cors_origins[0]}")
