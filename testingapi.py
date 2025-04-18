import base64

with open("ipl_prediction_bot/ipl-predicitions-1e2c80dbf7a9.json", "rb") as f:
    encoded = base64.b64encode(f.read()).decode("utf-8")
    print(encoded)