from flask import Flask,request
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
logger.addHandler(console_handler)

app = Flask(__name__)

stores = [
    {
        "name": "My Store",
        "items":[
            {
                "name": "Chair",
                "price": 15.99
            }
        ]
    }
]

@app.get("/stores")  #http://127.0.0.1:5000/stores
def get_stores():
    logger.info("Fetching all stores")
    return {"stores":stores}

@app.post("/stores") #http://127.0.0.1:5000/stores
def create_store():
    logger.info("Creating a new store")
    request_data = request.get_json()
    new_store = {"name":request_data['name'], "items":[]}
    stores.append(new_store)
    return new_store, 201

@app.post("/stores/<string:name>/item")
def create_item(name):
    logger.info(f"Adding item to store: {name}")
    request_data =request.get_json()
    for store in stores:
        if store['name'] == name:
            new_item = {
                "name": request_data['name'],
                "price": request_data['price']
            }
            store['items'].append(new_item)
            return new_item, 201
    return {"Message": "Store not found"}, 404

@app.get("/stores/get-name/")
def get_store():
    logger.info("Fetching store by name")
    request_data = request.get_json()
    store_name = request_data.get('name')
    if not store_name:
        return {"Message": "Store name is required"}, 404
    for store in stores:
        if store['name'] == store_name:
            return store, 200
    return {"Message": "Store not found"}, 404

@app.get("/stores/get-item/")
def get_item():
    logger.info("Fetching item by name")
    request_data = request.get_json()
    store_name = request_data.get("store_name")
    item_name = request_data.get("item_name")
    if store_name is None:
        return {"Message: Store name is required"}, 404
    if item_name is None:
        return {"Message: Item name is required"}, 404
    for store in stores:
        if store['name'] ==  store_name:
            for item in store['items']:
                if item['name'] == item_name:
                    return item, 200
    return {"Message": "Item not found"}, 404

if __name__ == '__main__':
    app.run(debug=True)