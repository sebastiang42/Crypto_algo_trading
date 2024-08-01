interval_map = {
    '1m': {
        'binance': '1m',
        'coinbase': 'ONE_MINUTE',
        'bybit': '1',
        'okx': '1m',
        'digifinex': '1',
        'bitget': '1min'
    },
    '5m': {
        'binance': '5m',
        'coinbase': 'FIVE_MINUTE',
        'bybit': '5',
        'okx': '5m',
        'digifinex': '5',
        'bitget': '5min'
    },
    '15m': {
        'binance': '15m',
        'coinbase': 'FIFTEEN_MINUTE',
        'bybit': '15',
        'okx': '15m',
        'digifinex': '15',
        'bitget': '15min'
    },
    '30m': {
        'binance': '30m',
        'coinbase': 'THIRTY_MINUTE',
        'bybit': '30',
        'okx': '30m',
        'digifinex': '30',
        'bitget': '30min'
    },
    '1h': {
        'binance': '1h',
        'coinbase': 'ONE_HOUR',
        'bybit': '60',
        'okx': '1H',
        'digifinex': '60',
        'bitget': '1h'
    },
    '6h': {
        'binance': '6h',
        'coinbase': 'SIX_HOUR',
        'bybit': '360',
        'okx': '6Hutc',
        'bitget': '6Hutc'
    },
    '1d': {
        'binance': '1d',
        'coinbase': 'ONE_DAY',
        'bybit': 'D',
        'okx': '1Dutc',
        'digifinex': '1D',
        'bitget': '1Dutc'
    }
}

symbol_map = {
    'BTCUSD': {
        'binance': 'BTCUSDT',
        'coinbase': 'BTC-USD',
        'bybit': 'BTCUSD',
        'okx': 'BTC-USDT',
        'digifinex': 'btc_usdt',
        'bitget': 'BTCUSDT'
    },
    'ETHUSD': {
        'binance': 'ETHUSDT',
        'coinbase': 'ETH-USD',
        'bybit': 'ETHUSD',
        'okx': 'ETH-USDT',
        'digifinex': 'eth_usdt',
        'bitget': 'ETHUSDT'
    },
    'XRPUSD': {
        'binance': 'XRPUSDT',
        'coinbase': 'XRP-USD',
        'bybit': 'XRPUSD',
        'okx': 'XRP-USDT',
        'digifinex': 'xrp_usdt',
        'bitget': 'XRPUSDT'
    },
    'SOLUSD': {
        'binance': 'SOLUSDT',
        'coinbase': 'SOL-USD',
        'bybit': 'SOLUSDT',
        'okx': 'SOL-USDT',
        'digifinex': 'sol_usdt',
        'bitget': 'SOLUSDT'
    },
    'DOGEUSD': {
        'binance': 'DOGEUSDT',
        'coinbase': 'DOGE-USD',
        'bybit': 'DOGEUSDT',
        'okx': 'DOGE-USDT',
        'digifinex': 'doge_usdt',
        'bitget': 'DOGEUSDT'
    }
}