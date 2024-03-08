# onsul 配置
CONSUL_OPTIONS = {
    'CONSUL_SERVER': {
        'HOST': '128.5.9.79',
        'PORT': 8500,
        'MAX_TRY_COUNT': 5
    }
}

CONSUL_HOST: str = CONSUL_OPTIONS.get('CONSUL_SERVER').get('HOST')
CONSUL_PORT: int = CONSUL_OPTIONS.get('CONSUL_SERVER').get('PORT')

CONSUL_MAX_TRY_COUNT: int = CONSUL_OPTIONS.get('CONSUL_SERVER').get('MAX_TRY_COUNT')

