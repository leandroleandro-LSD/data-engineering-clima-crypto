
import requests
import json
import os
from datetime import datetime
from google.cloud import storage
from flask import Flask

app = Flask(__name__)
BUCKET_NAME = os.environ.get("BUCKET_NAME", "clima-crypto-datalake")

CIDADES = {
    "sao_paulo":    {"lat": -23.5505, "lon": -46.6333},
    "rio_janeiro":  {"lat": -22.9068, "lon": -43.1729},
    "brasilia":     {"lat": -15.7801, "lon": -47.9292},
    "manaus":       {"lat": -3.1190,  "lon": -60.0217},
    "porto_alegre": {"lat": -30.0346, "lon": -51.2177},
}

CRYPTOS = "bitcoin,ethereum,solana,cardano,ripple"

def coletar_clima():
    resultados = {}
    for cidade, coords in CIDADES.items():
        url = (f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true&timezone=America/Sao_Paulo")
        resp = requests.get(url)
        resultados[cidade] = resp.json()
    return resultados

def coletar_crypto():
    url = (f"https://api.coingecko.com/api/v3/simple/price?ids={CRYPTOS}&vs_currencies=brl,usd&include_24hr_change=true&include_market_cap=true&include_24hr_vol=true")
    resp = requests.get(url)
    return resp.json()

def salvar_no_gcs(dados, tipo):
    cliente = storage.Client()
    bucket = cliente.bucket(BUCKET_NAME)
    agora = datetime.utcnow()
    caminho = f"raw/{tipo}/ano={agora.strftime('%Y')}/mes={agora.strftime('%m')}/dia={agora.strftime('%d')}/hora={agora.strftime('%H')}/{tipo}_{agora.strftime('%Y%m%d_%H%M%S')}.json"
    blob = bucket.blob(caminho)
    blob.upload_from_string(json.dumps(dados, ensure_ascii=False), content_type="application/json")
    return caminho

@app.route("/", methods=["GET", "POST"])
def main():
    clima = coletar_clima()
    crypto = coletar_crypto()
    caminho_clima = salvar_no_gcs(clima, "clima")
    caminho_crypto = salvar_no_gcs(crypto, "crypto")
    return f"OK!\nClima  -> {caminho_clima}\nCrypto -> {caminho_crypto}", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
