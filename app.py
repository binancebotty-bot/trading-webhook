from flask import Flask, request, jsonify
import ccxt
import os
import logging
import threading
import time
import requests

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Keep-alive function to prevent Render from sleeping
def keep_alive():
    while True:
        try:
            # Get the current app URL (works in production)
            base_url = os.getenv('RENDER_EXTERNAL_URL') or 'https://your-project-name.onrender.com'
            health_url = f"{base_url}/health"
            
            response = requests.get(health_url, timeout=10)
            app.logger.info(f"Keep-alive ping successful: {response.status_code}")
        except Exception as e:
            app.logger.warning(f"Keep-alive ping failed: {str(e)}")
        
        # Ping every 5 minutes (more frequent than 15-minute sleep threshold)
        time.sleep(300)  # 300 seconds = 5 minutes

# Start keep-alive in background thread
if os.getenv('RENDER'):  # Only run in production, not locally
    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()
    app.logger.info("Keep-alive service started")

def get_binance():
    return ccxt.binance({
        'apiKey': os.getenv('PUoezDXyGvZROIGGN9T74cgYTFZoIdlyDpI2lRgmCA72B6TUXO70CMCvgwOxtNf4'),
        'secret': os.getenv('1yebFyjsgEiI5Sbhd9LqhfhR5sIwN5vbRB50EDTYsPTrExJUkewuV8uqRZpsRZRx'),
        'sandbox': False,
        'options': {'defaultType': 'spot'}
    })

@app.route('/webhook', methods=['POST'])
def handle_tradingview_alert():
    try:
        data = request.json
        app.logger.info(f"Received alert: {data}")
        
        symbol = data.get('ticker', '').replace(':', '').replace('/', '')
        action = data.get('order_action', '').lower()
        quantity = float(data.get('order_contracts', 0))
        
        alert_message = data.get('strategy_order_alert_message', '')
        centreline_price = extract_centreline(alert_message)
        
        binance = get_binance()
        
        if action == 'buy' and centreline_price:
            results = {}
            
            # 1. Market buy
            buy_order = binance.create_order(
                symbol=symbol,
                type='market',
                side='buy', 
                amount=quantity
            )
            results['buy_order'] = buy_order
            
            # 2. Centreline sell
            tp_order = binance.create_order(
                symbol=symbol,
                type='limit', 
                side='sell',
                amount=quantity,
                price=centreline_price
            )
            results['tp_order'] = tp_order
            
            return jsonify({'status': 'success', 'results': results})
        else:
            return jsonify({'status': 'error', 'message': 'Invalid action or missing centreline'})
            
    except Exception as e:
        app.logger.error(f"Error: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})

def extract_centreline(alert_message):
    if not alert_message:
        return None
    for part in alert_message.split('|'):
        if 'centreline=' in part:
            return float(part.split('=')[1])
    return None

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'service': 'tradingview-webhook'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)