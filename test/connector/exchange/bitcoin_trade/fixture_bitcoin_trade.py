class FixtureBitcoinTrade:
    GET_BALANCES = {
        "code": None,
        "message": None,
        "data": [{
            "available_amount": 924.74,
            "currency_code": "BRL",
            "locked_amount": 0
        }, {
            "available_amount": 0,
            "currency_code": "BTC",
            "locked_amount": 0
        }, {
            "available_amount": 0,
            "currency_code": "ETH",
            "locked_amount": 0
        }, {
            "available_amount": 0,
            "currency_code": "LTC",
            "locked_amount": 0
        }, {
            "available_amount": 0,
            "currency_code": "BCH",
            "locked_amount": 0
        }, {
            "available_amount": 21.9964932,
            "currency_code": "XRP",
            "locked_amount": 0
        }, {
            "available_amount": 0,
            "currency_code": "EOS",
            "locked_amount": 0
        }, {
            "available_amount": 0,
            "currency_code": "DAI",
            "locked_amount": 0
        }]
    }

    ORDER_PLACE = {
        "code": None,
        "message": None,
        "data": {
            "id": "U2FsdGVkX1/Q7OCvrbYDb61D2wUcbNeUBP7EVhQOQ+E=",
            "code": "atnJb6HJg8",
            "type": "buy",
            "unit_price": 12489.75,
            "origin_currency_code": "BRL",
            "destination_currency_code": "ETH",
            "total_price": 37.47,
            "amount": 0.00300007,
            "pair": "BRLETH"
        }
    }

    ORDER_GET_LIMIT_BUY_FILLED = {
        "code": None,
        "message": None,
        "data": {
            "code": "atnJb6HJg8",
            "create_date": "2021-07-04T20:20:12.830Z",
            "executed_amount": 0.00300007,
            "id": "U2FsdGVkX1++guCsKyAKqQ5fvxqJAPh2H+U7Tqt/pl0=",
            "pair_code": "BRLETH",
            "remaining_amount": 0,
            "remaining_price": 0,
            "requested_amount": 0.00300007,
            "status": "executed_completely",
            "subtype": "limited",
            "total_price": 0,
            "type": "buy",
            "unit_price": 12489.75,
            "update_date": "2021-07-04T20:20:12.860Z",
            "transactions": [{
                "amount": 0.00300007,
                "create_date": "2021-07-04T20:20:12.860Z",
                "total_price": 35.84,
                "unit_price": 11945
            }]
        }
    }

    ORDER_GET_LIMIT_SELL_FILLED = {
        "code": None,
        "message": None,
        "data": {
            "code": "-zGRYdU9a",
            "create_date": "2021-07-04T20:49:17.817Z",
            "executed_amount": 0.003,
            "id": "U2FsdGVkX188FRogs2RIUIO200GhZ1D0f4gC6cvetyk=",
            "pair_code": "BRLETH",
            "remaining_amount": 0,
            "remaining_price": 0,
            "requested_amount": 0.003,
            "status": "executed_completely",
            "subtype": "limited",
            "total_price": 0,
            "type": "sell",
            "unit_price": 11347.75,
            "update_date": "2021-07-04T20:49:17.887Z",
            "transactions": [{
                "amount": 0.003,
                "create_date": "2021-07-04T20:49:17.887Z",
                "total_price": 35.7,
                "unit_price": 11900
            }]
        }
    }

    ORDER_GET_MARKET_BUY = {
        "code": None,
        "message": None,
        "data": {
            "code": "zosnxKn7ce",
            "create_date": "2021-07-04T23:04:47.253Z",
            "executed_amount": 0.00300659,
            "id": "U2FsdGVkX181c8PBj4ryDzuuo/iFMs4j/6JIPhj9Le4=",
            "pair_code": "BRLETH",
            "remaining_amount": 0,
            "remaining_price": 0,
            "requested_amount": 0.00300659,
            "status": "executed_completely",
            "subtype": "market",
            "total_price": 0,
            "type": "buy",
            "unit_price": 11790.7662834,
            "update_date": "2021-07-04T23:04:47.320Z",
            "transactions": [{
                "amount": 0.00300659,
                "create_date": "2021-07-04T23:04:47.320Z",
                "total_price": 35.45,
                "unit_price": 11790.76
            }]
        }
    }

    ORDER_GET_MARKET_SELL = {
        "code": None,
        "message": None,
        "data": {
            "code": "w71JJoqZLm",
            "create_date": "2021-07-04T23:00:40.190Z",
            "executed_amount": 0.003,
            "id": "U2FsdGVkX18rZ2XKuTYVUnSSOwm6JcrmV/1bQFPHtEY=",
            "pair_code": "BRLETH",
            "remaining_amount": 0,
            "remaining_price": 0,
            "requested_amount": 0.003,
            "status": "executed_completely",
            "subtype": "market",
            "total_price": 0,
            "type": "sell",
            "unit_price": 11663.33333333,
            "update_date": "2021-07-04T23:00:40.223Z",
            "transactions": [{
                "amount": 0.003,
                "create_date": "2021-07-04T23:00:40.223Z",
                "total_price": 34.99,
                "unit_price": 11664.82
            }]
        }
    }

    ORDER_GET_LIMIT_BUY_UNFILLED = {
        "code": None,
        "message": None,
        "data": {
            "code": "CmIRDmse8",
            "create_date": "2021-07-05T08:04:52.593Z",
            "executed_amount": 0,
            "id": "U2FsdGVkX1+7UqIGbJs8zS4Fesh1/BQ2BJe5aD8zL9Q=",
            "pair_code": "BRLETH",
            "remaining_amount": 0.003,
            "remaining_price": 30,
            "requested_amount": 0.003,
            "status": "waiting",
            "subtype": "limited",
            "total_price": 0,
            "type": "buy",
            "unit_price": 10000,
            "update_date": "2021-07-05T08:04:52.593Z",
            "transactions": []
        }
    }

    ORDER_GET_LIMIT_SELL_UNFILLED = {
        "code": None,
        "message": None,
        "data": {
            "code": "ON9O8Qqwx5",
            "create_date": "2021-07-05T08:13:39.223Z",
            "executed_amount": 0,
            "id": "U2FsdGVkX19ck+lznGtd0ZvaDYnykyHeAw5A6ik4R0w=",
            "pair_code": "BRLETH",
            "remaining_amount": 0.003,
            "remaining_price": 150,
            "requested_amount": 0.003,
            "status": "waiting",
            "subtype": "limited",
            "total_price": 0,
            "type": "sell",
            "unit_price": 50000,
            "update_date": "2021-07-05T08:13:39.223Z",
            "transactions": []
        }
    }

    ORDER_GET_CANCELED = {
        "code": None,
        "message": None,
        "data": {
            "id": "U2FsdGVkX1+4qw47xXOvf8wAYb6SBHYbXSz5QIidTns=",
            "code": "cbXQFPZX7",
            "type": "sell",
            "unit_price": 3.5,
            "origin_currency_code": "XRP",
            "destination_currency_code": "BRL",
            "total_price": 7,
            "amount": 2,
            "pair": "BRLXRP"
        }
    }
