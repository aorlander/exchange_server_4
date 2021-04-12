from flask import Flask, request, g
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask import jsonify
import json
import eth_account
import algosdk
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import load_only
from datetime import datetime
import sys

from models import Base, Order, Log
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

app = Flask(__name__)

@app.before_request
def create_session():
    g.session = scoped_session(DBSession)

@app.teardown_appcontext
def shutdown_session(response_or_exc):
    sys.stdout.flush()
    g.session.commit()
    g.session.remove()


""" Suggested helper methods """

def check_sig(payload,sig):
    s_pk = payload['sender_pk'] 
    platform = payload['platform']
    response = False
    if platform=='Ethereum':
        eth_encoded_msg = eth_account.messages.encode_defunct(text=payload)
        if eth_account.Account.recover_message(eth_encoded_msg,signature=sig) == s_pk:
            response = True
    if platform=='Algorand':
        if algosdk.util.verify_bytes(payload.encode('utf-8'),sig,s_pk):
            response = True
    return response

def check_match(existing_order, order):
    if(order.filled==None):
        if(existing_order.buy_currency == order.sell_currency):
            if(existing_order.sell_currency == order.buy_currency):
                if(existing_order.sell_amount / existing_order.buy_amount >= order.buy_amount/order.sell_amount):
                     return True
    return False

def match_order(existing_order, order):  
    if (existing_order.sell_amount < order.buy_amount):
        #print("\n current: SELL " + str(order.sell_amount) + " " + order.sell_currency + " / BUY " + str(order.buy_amount) + " " + order.buy_currency)
        remaining_buy_amt = order.buy_amount - existing_order.sell_amount
        remaining_sell_amt = order.sell_amount - existing_order.buy_amount
        derived_implied_fx=remaining_buy_amt/remaining_sell_amt
        #print("\n order FX = " + str(parent_implied_fx)  + " // existing FX = " + str(existing_implied_fx) + " // derived FX = " + str(derived_implied_fx))
        derived_order = Order (
            creator_id=order.id, 
            sender_pk=order.sender_pk,
            receiver_pk=order.receiver_pk, 
            buy_currency=order.buy_currency, 
            sell_currency=order.sell_currency, 
            buy_amount=remaining_buy_amt, 
            sell_amount= remaining_sell_amt)
        derived_order.timestamp = datetime.now()
        derived_order.relationship = (derived_order.id, order.id)
        g.session.add(derived_order)
        g.session.commit()
        existing_order.filled = order.timestamp 
        order.filled = order.timestamp
        existing_order.counterparty_id = order.id
        order.counterparty_id = existing_order.id
        existing_implied_fx=existing_order.buy_amount/existing_order.sell_amount
        parent_implied_fx= order.buy_amount/order.sell_amount
        #print("created: SELL " + str(child_order.sell_amount) + " " + child_order.sell_currency + " / BUY " + str(child_order.buy_amount) + " " + child_order.buy_currency)
    return 0

def fill_order(order,txes=[]):
    for tx in txes:
        if tx.filled == None:
            if(check_match(tx, order)==True):
                match_order(tx, order)
    pass
  
def log_message(d):
    # Takes input dictionary d and writes it to the Log table
    # Hint: use json.dumps or str() to get it in a nice string form
    time = datetime.now()
    log = Log(logtime=time, message=d)
    g.session.add(log)
    g.session.commit()
    pass

""" End of helper methods """

@app.route('/trade', methods=['POST'])
def trade():
    print("In trade endpoint")
    if request.method == "POST":
        content = request.get_json(silent=True)
        print( f"content = {json.dumps(content)}" )
        columns = [ "sender_pk", "receiver_pk", "buy_currency", "sell_currency", "buy_amount", "sell_amount", "platform" ]
        fields = [ "sig", "payload" ]

        for field in fields:
            if not field in content.keys():
                print( f"{field} not received by Trade" )
                print( json.dumps(content) )
                log_message(content)
                return jsonify( False )
        
        for column in columns:
            if not column in content['payload'].keys():
                print( f"{column} not received by Trade" )
                print( json.dumps(content) )
                log_message(content)
                return jsonify( False )
            
        #Your code here
        #Note that you can access the database session using g.session

        # TODO: Check the signature
        response = check_sig(content['payload'], content['sig'])
        print(response)
        # TODO: Add the order to the database and Fill the order
        if response == True:
            order = Order(sender_pk=content['payload']['sender_pk'] , 
                          receiver_pk=content['payload']['receiver_pk'], 
                          buy_currency=content['payload']['buy_currency'], 
                          sell_currency=content['payload']['sell_currency'], 
                          buy_amount=content['payload']['buy_amount'], 
                          sell_amount=content['payload']['sell_amount'])
            g.session.add(order)
            g.session.commit()
            fill_order(order, g.session.query(Order).all())
            return jsonify(True)

        if response == False:
            leg_message(json.dumps(content['payload']))
            return jsonify(False)

        # TODO: Be sure to return jsonify(True) or jsonify(False) depending on if the method was successful
        return jsonify(True)

@app.route('/order_book')
def order_book():
    orders = g.session.query(Order).all()
    list_orders = []
    for order in orders:
        o = {"sender_pk": order.sender_pk, "receiver_pk": order.receiver_pk, 
            "buy_currency": order.buy_currency, "sell_currency": order.sell_currency, 
            "buy_amount": order.buy_amount, "sell_amount": order.sell_amount}
        list_orders.append(o)

    return jsonify(data=list_orders)

if __name__ == '__main__':
    app.run(port='5002')